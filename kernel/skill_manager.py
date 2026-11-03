# G:\Aoao_OS\kernel\skill_manager.py
"""
Aoao OS 分布式技能树框架 - 技能管理器核心

提供技能的全生命周期管理:
  - 动态环境指纹采集 (get_env_hash)
  - 技能编译与落盘 (save_skill)
  - 负反馈熔断机制 (report_failure)
  - 语义定向检索与注入 (load_active_skills)

所有技能以 JSON 格式存储在 skills/{domain}/ 子文件夹中，按领域隔离。
每个技能文件包含版本号、环境指纹、失败计数和核心行为指南 (prompt_recipe)。
"""
import os
import json
import hashlib
import glob
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from kernel.logger import get_logger

logger = get_logger(__name__)

# 项目根目录 (kernel/ 的上层)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SKILLS_DIR = os.path.join(BASE_DIR, "skills")

# 连续失败次数阈值，达到后永久禁用
FAILURE_THRESHOLD = 3

# 工具名到领域文件夹的映射表
# 当主脑调用某工具时，通过此表定位对应的技能子文件夹
TOOL_DOMAIN_MAP = {
    "wechat": "wechat",
    "image_generator": "image_generator",
}

# 进程级环境指纹缓存: 同一进程内源文件不变则指纹不变,
# 避免每次 think 都重算 7 个文件的 MD5 (原实现每轮打 5 行 DEBUG 且重复 IO)
_env_hash_cache: Optional[Tuple[float, str]] = None


def _collect_env_fingerprint_files() -> List[str]:
    """返回参与环境指纹计算的所有源文件路径 (仅存在的)。"""
    targets: List[str] = []

    # 微信组件
    for wechat_file in ["main.py", "wx_bridge.py"]:
        targets.append(os.path.join(BASE_DIR, "apps", "wechat", wechat_file))

    # 图片生成器
    targets.append(os.path.join(BASE_DIR, "apps", "image_generator", "main.py"))

    # 核心内核
    for kernel_file in ["brain.py", "app_bus.py", "prompts.py"]:
        targets.append(os.path.join(BASE_DIR, "kernel", kernel_file))

    return [p for p in targets if os.path.exists(p)]


def _ensure_domain_dir(domain: str) -> str:
    """确保领域子文件夹存在，返回其绝对路径。"""
    domain_dir = os.path.join(SKILLS_DIR, domain)
    os.makedirs(domain_dir, exist_ok=True)
    return domain_dir


def get_env_hash() -> str:
    """
    动态环境指纹采集 (带进程级缓存)。

    抓取核心源文件 (微信组件 / 图片生成器 / 内核) 的 mtime + 内容 MD5，
    生成当前系统运行时的唯一哈希特征。当任何底层依赖代码变化时哈希改变，
    触发已有技能软淘汰 (环境指纹不匹配即视为过期技能)。

    缓存策略: 按文件 mtime 缓存。源文件未修改时直接返回缓存值，
    避免每轮 think 重复读取 7 个文件并重复打 DEBUG 日志。

    Returns:
        32 位 MD5 十六进制字符串
    """
    global _env_hash_cache

    files = _collect_env_fingerprint_files()

    # mtime 签名: 任一源文件被改写则签名变化, 触发重算
    mtime_signature = "|".join(f"{p}:{os.path.getmtime(p):.6f}" for p in files)

    if _env_hash_cache and _env_hash_cache[0] is not None:
        cached_signature, cached_hash = _env_hash_cache
        if cached_signature == mtime_signature:
            return cached_hash

    fingerprints = []
    for path in files:
        with open(path, "rb") as f:
            fingerprints.append(hashlib.md5(f.read()).hexdigest())

    combined = "|".join(fingerprints)
    env_hash = hashlib.md5(combined.encode("utf-8")).hexdigest()
    _env_hash_cache = (mtime_signature, env_hash)
    # 仅在指纹实际变化时记录一次, 消除每轮 think 的 DEBUG 噪声
    logger.info(f"[SkillManager] 环境指纹采集完成: {env_hash[:12]}...")
    return env_hash


def save_skill(
    skill_name: str,
    domain: str,
    prompt_recipe: str,
    version: str = None,
    metadata: dict = None,
) -> str:
    """
    技能动态编译与落盘。

    接收主脑传来的成功经验文本，将其精炼封装为包含以下字段的标准 JSON 格式，
    并物理写入对应领域的子文件夹中:

        skill_name:      技能名称
        domain:          所属领域
        version:         版本号
        environment_hash: 创建时的环境指纹
        fail_count:      失败计数 (初始为 0)
        prompt_recipe:   核心高密度行为指南
        created_at:      创建时间戳
        metadata:        额外元数据

    Args:
        skill_name: 技能名称 (用作文件名，非法字符会被过滤)
        domain: 所属领域 (对应 skills/ 下的子文件夹名)
        prompt_recipe: 核心高密度行为指南文本 (技能的核心价值)
        version: 版本号，默认使用时间戳格式 YYYYMMDD_HHMMSS
        metadata: 额外元数据字典 (如触发场景、来源对话摘要等)

    Returns:
        保存的技能文件绝对路径
    """
    domain_dir = _ensure_domain_dir(domain)

    if version is None:
        version = datetime.now().strftime("%Y%m%d_%H%M%S")

    env_hash = get_env_hash()

    skill_data = {
        "skill_name": skill_name,
        "domain": domain,
        "version": version,
        "environment_hash": env_hash,
        "fail_count": 0,
        "prompt_recipe": prompt_recipe,
        "created_at": datetime.now().isoformat(),
        "metadata": metadata or {},
    }

    # 清理文件名中的非法字符，保留字母数字和 . _ -
    safe_name = "".join(c for c in skill_name if c.isalnum() or c in "._-")
    if not safe_name:
        safe_name = hashlib.md5(skill_name.encode("utf-8")).hexdigest()[:12]

    file_path = os.path.join(domain_dir, f"{safe_name}.json")

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(skill_data, f, ensure_ascii=False, indent=2)

    logger.info(f"[SkillManager] 技能已编译落盘: {skill_name} -> {file_path}")
    return file_path


def _apply_failure_penalty(json_path: str, disabled_path: str, skill_name: str) -> bool:
    """
    对单个技能文件执行失败惩罚逻辑。

    Args:
        json_path: 技能 JSON 文件路径
        disabled_path: 禁用后的目标路径 (.disabled)
        skill_name: 技能名称 (日志用)

    Returns:
        True 如果技能刚刚被禁用，False 如果仅增加了失败计数
    """
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            skill_data = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"[SkillManager] 读取技能文件失败: {json_path}: {e}")
        return False

    skill_data["fail_count"] = skill_data.get("fail_count", 0) + 1
    current_failures = skill_data["fail_count"]

    logger.warning(
        f"[SkillManager] 技能 {skill_name} 失败计数: {current_failures}/{FAILURE_THRESHOLD}"
    )

    if current_failures >= FAILURE_THRESHOLD:
        # 物理打入黑名单: 将 .json 重命名为 .disabled
        try:
            os.rename(json_path, disabled_path)
            skill_data["disabled_at"] = datetime.now().isoformat()
            # 更新已重命名文件中的最终状态
            with open(disabled_path, "w", encoding="utf-8") as f:
                json.dump(skill_data, f, ensure_ascii=False, indent=2)
            logger.error(
                f"[SkillManager] 技能 {skill_name} 连续失败 {current_failures} 次，"
                f"已永久禁用 -> {disabled_path}"
            )
            return True
        except OSError as e:
            logger.error(f"[SkillManager] 重命名技能文件失败: {e}")
            return False
    else:
        # 仅更新失败计数
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(skill_data, f, ensure_ascii=False, indent=2)
        return False


def report_failure(domain: str, skill_name: str = None) -> bool:
    """
    负反馈惩罚机制 (异常拦截接口)。

    如果主脑加载了某技能后底层依然抛出报错，调用此方法让该技能的 fail_count 加 1。
    当连续报错达到 3 次，物理将其重命名为 .disabled，打入黑名单，永远剥夺其加载权限。

    Args:
        domain: 领域文件夹名 (如 "wechat", "image_generator")
        skill_name: 技能文件名 (不含 .json 扩展名)。
                    如果为 None，则对该领域下所有活跃技能执行惩罚。

    Returns:
        True 如果至少有一个技能被禁用，False 如果仅增加了失败计数或无技能可惩罚
    """
    domain_dir = os.path.join(SKILLS_DIR, domain)
    if not os.path.isdir(domain_dir):
        logger.warning(f"[SkillManager] 领域目录不存在，无法执行失败上报: {domain_dir}")
        return False

    if skill_name:
        json_path = os.path.join(domain_dir, f"{skill_name}.json")
        disabled_path = os.path.join(domain_dir, f"{skill_name}.disabled")

        if not os.path.exists(json_path):
            # 检查是否已经被禁用
            if os.path.exists(disabled_path):
                logger.debug(f"[SkillManager] 技能 {skill_name} 此前已被禁用，跳过。")
                return True
            logger.warning(f"[SkillManager] 技能文件不存在，无法上报失败: {json_path}")
            return False

        return _apply_failure_penalty(json_path, disabled_path, skill_name)
    else:
        # 未指定技能名，对该领域下所有活跃技能各记一次失败
        any_disabled = False
        for file_path in glob.glob(os.path.join(domain_dir, "*.json")):
            base_name = os.path.splitext(os.path.basename(file_path))[0]
            disabled_path = os.path.join(domain_dir, f"{base_name}.disabled")
            if _apply_failure_penalty(file_path, disabled_path, base_name):
                any_disabled = True
        return any_disabled


def load_active_skills(domain: str) -> Tuple[str, List[Dict]]:
    """
    语义定向检索与注入。

    根据工具名/领域名直接下钻到对应的子文件夹 (如 skills/wechat/)，
    遍历所有未禁用的 JSON 技能文件。比对 environment_hash:
      - 指纹一致: 提取 prompt_recipe，标记为活跃技能
      - 指纹不一致: 执行软淘汰 (Deprecate)，跳过不加载

    最后将所有合规的 prompt_recipe 文本提取出来，
    作为高密度的行为准则直接合并返回。

    Args:
        domain: 领域名或工具名 (如 "wechat", "image_generator")

    Returns:
        (merged_recipes, active_metadata):
          - merged_recipes: 所有合规技能的 prompt_recipe 合并文本，
                            以 "---" 分隔符连接
          - active_metadata: 活跃技能的元数据列表，每项包含
                             skill_name, domain, file, version, fail_count
    """
    # 将工具名映射到领域文件夹
    domain_dir_name = TOOL_DOMAIN_MAP.get(domain, domain)
    domain_dir = os.path.join(SKILLS_DIR, domain_dir_name)

    if not os.path.isdir(domain_dir):
        logger.debug(f"[SkillManager] 领域目录不存在，无技能可加载: {domain_dir}")
        return "", []

    current_env_hash = get_env_hash()
    active_recipes = []
    active_metadata = []
    deprecated_count = 0

    for file_path in sorted(glob.glob(os.path.join(domain_dir, "*.json"))):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                skill_data = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"[SkillManager] 跳过损坏的技能文件: {file_path}: {e}")
            continue

        skill_env_hash = skill_data.get("environment_hash", "")
        skill_name = skill_data.get("skill_name", os.path.basename(file_path))

        # 环境指纹校验: 不一致则软淘汰 (跳过不加载)
        if skill_env_hash and skill_env_hash != current_env_hash:
            logger.info(
                f"[SkillManager] 技能 {skill_name} 环境指纹不匹配 "
                f"(skill={skill_env_hash[:12]}... vs env={current_env_hash[:12]}...)，"
                f"执行软淘汰。"
            )
            deprecated_count += 1
            continue

        # 提取核心行为指南
        recipe = skill_data.get("prompt_recipe", "")
        if recipe:
            active_recipes.append(recipe)
            active_metadata.append({
                "skill_name": skill_name,
                "domain": skill_data.get("domain", domain),
                "file": os.path.basename(file_path),
                "version": skill_data.get("version", ""),
                "fail_count": skill_data.get("fail_count", 0),
            })

    if active_recipes:
        logger.info(
            f"[SkillManager] 领域 [{domain}] 加载 {len(active_recipes)} 个活跃技能"
            + (f", {deprecated_count} 个已软淘汰" if deprecated_count else "")
        )
    elif deprecated_count:
        # 有软淘汰才值得记录; 纯空领域静默, 避免每轮 think 刷屏
        logger.debug(
            f"[SkillManager] 领域 [{domain}] 无活跃技能 ({deprecated_count} 个已软淘汰)"
        )

    merged = "\n\n---\n\n".join(active_recipes)
    return merged, active_metadata


def get_all_active_skills() -> Tuple[str, Dict[str, List[Dict]]]:
    """
    遍历所有领域文件夹，加载全部活跃技能。

    用于系统启动时或需要全量技能注入的场景。

    Returns:
        (merged_text, metadata_by_domain):
          - merged_text: 所有领域活跃技能的合并文本
          - metadata_by_domain: 按领域分组的技能元数据
    """
    if not os.path.isdir(SKILLS_DIR):
        return "", {}

    all_recipes = []
    all_metadata = {}

    for domain_name in sorted(os.listdir(SKILLS_DIR)):
        domain_path = os.path.join(SKILLS_DIR, domain_name)
        if not os.path.isdir(domain_path):
            continue

        recipes, metadata = load_active_skills(domain_name)
        if recipes:
            all_recipes.append(f"[领域: {domain_name}]\n{recipes}")
            all_metadata[domain_name] = metadata

    merged = "\n\n=========\n\n".join(all_recipes)
    return merged, all_metadata


def list_skills(domain: str = None) -> List[Dict]:
    """
    列出指定领域或全部技能的状态 (用于调试和运维)。

    Args:
        domain: 领域文件夹名，为 None 时列出全部领域

    Returns:
        技能信息列表，每项在原始 JSON 字段基础上追加:
          _file:   文件名
          _status: "active" | "disabled"
    """
    results = []

    if domain:
        domain_path = os.path.join(SKILLS_DIR, domain)
        if not os.path.isdir(domain_path):
            return []
        search_dirs = [domain_path]
    else:
        if not os.path.isdir(SKILLS_DIR):
            return []
        search_dirs = [
            os.path.join(SKILLS_DIR, d) for d in os.listdir(SKILLS_DIR)
            if os.path.isdir(os.path.join(SKILLS_DIR, d))
        ]

    for domain_dir in search_dirs:
        for ext_pattern in ["*.json", "*.disabled"]:
            for file_path in sorted(glob.glob(os.path.join(domain_dir, ext_pattern))):
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        skill_data = json.load(f)
                except Exception:
                    continue
                skill_data["_file"] = os.path.basename(file_path)
                skill_data["_status"] = "disabled" if file_path.endswith(".disabled") else "active"
                results.append(skill_data)

    return results


def get_skill_tree() -> str:
    """
    生成 skills/ 目录的树状结构图 (纯文本格式)。

    Returns:
        树状结构字符串
    """
    if not os.path.isdir(SKILLS_DIR):
        return "(skills/ 目录不存在)"

    lines = ["skills/"]

    domains = sorted([
        d for d in os.listdir(SKILLS_DIR)
        if os.path.isdir(os.path.join(SKILLS_DIR, d))
    ])

    for i, domain in enumerate(domains):
        is_last_domain = (i == len(domains) - 1)
        prefix = "└── " if is_last_domain else "├── "
        lines.append(f"{prefix}{domain}/")

        domain_path = os.path.join(SKILLS_DIR, domain)
        files = sorted([
            f for f in os.listdir(domain_path)
            if f.endswith(".json") or f.endswith(".disabled")
        ])

        child_prefix = "    " if is_last_domain else "│   "

        for j, filename in enumerate(files):
            is_last_file = (j == len(files) - 1)
            file_prefix = "└── " if is_last_file else "├── "

            file_path = os.path.join(domain_path, filename)
            status = ""
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                skill_name = data.get("skill_name", filename)
                fail_count = data.get("fail_count", 0)
                if filename.endswith(".disabled"):
                    status = f" [DISABLED, failures={fail_count}]"
                elif fail_count > 0:
                    status = f" [warnings={fail_count}]"
            except Exception:
                skill_name = filename
                status = " [CORRUPT]"

            lines.append(f"{child_prefix}{file_prefix}{skill_name}{status}")

    return "\n".join(lines)

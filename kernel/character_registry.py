# kernel/character_registry.py
"""多智能体注册表 (0.3.17.0 P4-b 多角色记忆命名空间)。

架构级记忆隔离的注册中枢: 每个智能体 = 人设 (name/identity/persona) +
目标 (goals) + 记忆命名空间 (memory_root 指向一套独立存储目录树)。

三记忆模式 (老板拍板):
  shared   -- 与源智能体共用 memory_root (同库共演化, 零拷贝);
  snapshot -- 新 memory_root, 创建时一致性热拷贝源库 (kernel.memory_snapshot),
              此后独立演化;
  fresh    -- 新 memory_root 空白起步。

fail-safe 纪律 (对齐 principal.py 范式):
  - 阿露 (id=ailu) 为代码级内置默认, registry.json 缺失/损坏/无阿露/
    活动 id 未知全部回退阿露; get_active 永不抛 (boot 关键路径)。
  - 阿露 memory_root 恒由代码从 data/memory 推导, 不信 json (防注册表
    被改坏导致阿露记忆指错目录); builtin 的 mode/root 不可编辑。
  - snapshot 拷贝失败: 半成品目录整体回滚, registry 不落新记录。

守 capability-generic-not-coupled: 本模块只管注册与目录分配, 记忆存储
内部结构 (entities.json/向量库等) 归 system_memory_god 管, 灵魂文档播种
(D6) 由记忆 app boot 自检完成。
"""
from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
import threading
import time
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, Callable, Optional, Sequence, Tuple

from kernel.logger import get_logger

logger = get_logger(__name__)

DEFAULT_AGENT_ID = "ailu"
DEFAULT_AGENT_NAME = "阿露"
DEFAULT_AGENT_IDENTITY = "胖脸鱼的AI女儿"

MEMORY_MODE_SHARED = "shared"
MEMORY_MODE_SNAPSHOT = "snapshot"
MEMORY_MODE_FRESH = "fresh"
MEMORY_MODES = (MEMORY_MODE_SHARED, MEMORY_MODE_SNAPSHOT, MEMORY_MODE_FRESH)

PERSONA_MAX_CHARS = 300   # prompt 注入防膨胀: 人设描述截断上限
MAX_GOALS = 5             # prompt 注入防膨胀: 目标条数上限
IDENTITY_MAX_CHARS = 100  # 一句话身份截断上限
GOAL_MAX_CHARS = 100      # 单条目标截断上限


class DuplicateNameError(ValueError):
    """智能体重名 (路由层按类型映射 409, 不靠错误消息子串匹配)。"""


# 注册表写锁: create/update/activate 都是 load->改->save 的 RMW, web 端点
# 经 to_thread 并发执行, 无锁会丢更新/同 id 双记录; 串行化写路径 (本地单
# 用户, 锁开销可忽略)。
_REGISTRY_LOCK = threading.Lock()

# 创建后可编辑的字段; memory_mode/memory_root/base/builtin/id/created_at
# 创建时定型, 改它们 = 换存储根/换身份来源, 本版本不支持 (热切换留后续)。
_UPDATABLE_FIELDS = frozenset({"name", "identity", "persona", "goals"})


def _project_data_dir() -> str:
    """项目 data/ 绝对路径 (本文件在 kernel/, 上跳一层), 对齐 brain.py 范式。"""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_dir, "data")


def _base(base_dir: Optional[str]) -> str:
    return base_dir or _project_data_dir()


def registry_path(base_dir: Optional[str] = None) -> str:
    return os.path.join(_base(base_dir), "agents", "registry.json")


def default_memory_root(base_dir: Optional[str] = None) -> str:
    """阿露记忆根 = <base>/memory (现状 data/memory, 含 soul/ 与 graph.db)。"""
    return os.path.abspath(os.path.join(_base(base_dir), "memory"))


@dataclass(frozen=True)
class AgentRecord:
    """智能体注册记录 (frozen 不可变, 改动走 replace 出新对象)。"""

    id: str
    name: str
    identity: str = ""
    persona: str = ""
    goals: Tuple[str, ...] = ()
    memory_mode: str = MEMORY_MODE_FRESH
    memory_root: str = ""
    base_agent_id: str = ""
    created_at: str = ""          # ISO-8601 (管理层文件, 人类可读)
    builtin: bool = False

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "identity": self.identity,
            "persona": self.persona,
            "goals": list(self.goals),
            "memory_mode": self.memory_mode,
            "memory_root": self.memory_root,
            "base_agent_id": self.base_agent_id,
            "created_at": self.created_at,
            "builtin": self.builtin,
        }

    @staticmethod
    def from_dict(raw: dict) -> "AgentRecord":
        agent_id = str(raw.get("id") or "")
        name = str(raw.get("name") or "")
        if not agent_id or not name:
            raise ValueError("智能体记录缺 id/name")
        goals = tuple(
            g[:GOAL_MAX_CHARS]
            for g in (str(x).strip() for x in (raw.get("goals") or ())) if g
        )
        return AgentRecord(
            id=agent_id,
            name=name,
            identity=str(raw.get("identity") or ""),
            persona=str(raw.get("persona") or "")[:PERSONA_MAX_CHARS],
            goals=goals[:MAX_GOALS],
            memory_mode=str(raw.get("memory_mode") or MEMORY_MODE_FRESH),
            memory_root=str(raw.get("memory_root") or ""),
            base_agent_id=str(raw.get("base_agent_id") or ""),
            created_at=str(raw.get("created_at") or ""),
            builtin=bool(raw.get("builtin", False)),
        )


def default_agent(base_dir: Optional[str] = None) -> AgentRecord:
    """阿露: 代码级内置默认, memory_root 恒从代码推导 (不信 json)。"""
    return AgentRecord(
        id=DEFAULT_AGENT_ID,
        name=DEFAULT_AGENT_NAME,
        identity=DEFAULT_AGENT_IDENTITY,
        memory_mode=MEMORY_MODE_SHARED,
        memory_root=default_memory_root(base_dir),
        builtin=True,
    )


def load_registry(base_dir: Optional[str] = None) -> Tuple[list, str]:
    """读注册表, 返回 (agents, active_agent_id)。

    fail-safe: 文件缺失/损坏/结构错 -> ([阿露], "ailu"); 单条坏记录跳过;
    阿露缺失自动补位; active_id 指向不存在的智能体 -> 回退阿露。
    """
    base = _base(base_dir)
    fallback = ([default_agent(base)], DEFAULT_AGENT_ID)
    path = registry_path(base)
    if not os.path.exists(path):
        return fallback
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            raise ValueError("registry 顶层必须是对象")
        raw_agents = raw.get("agents")
        if not isinstance(raw_agents, list):
            raise ValueError("agents 必须是列表")
    except Exception as e:
        logger.warning(f"[Registry] registry.json 不可读, 回退默认智能体阿露: {e}")
        return fallback

    agents: list = []
    for item in raw_agents:
        if not isinstance(item, dict):
            continue
        try:
            rec = AgentRecord.from_dict(item)
        except Exception as e:
            logger.warning(
                f"[Registry] 跳过非法智能体记录 {item.get('id', '?')}: {e}")
            continue
        # [review 整改] 非内置记录的 memory_root 必须是存在的绝对路径,
        # 否则按坏记录跳过 (防手改坏 registry 让智能体在杂散相对目录
        # 静默开机失忆); 阿露根恒由代码推导, 不在此列。
        if rec.id != DEFAULT_AGENT_ID and (
                not os.path.isabs(rec.memory_root)
                or not os.path.isdir(rec.memory_root)):
            logger.warning(
                f"[Registry] 智能体 {rec.id} 记忆根非法 "
                f"({rec.memory_root!r}), 跳过该记录")
            continue
        agents.append(rec)

    # 阿露补位: builtin 锁字段 (root/mode) 恒代码推导, json 仅可覆盖
    # 可编辑字段 (name/identity/persona/goals)。
    builtin = default_agent(base)
    saved_ailu = next((a for a in agents if a.id == DEFAULT_AGENT_ID), None)
    if saved_ailu is not None:
        builtin = replace(
            builtin,
            name=saved_ailu.name or builtin.name,
            identity=saved_ailu.identity or builtin.identity,
            persona=saved_ailu.persona,
            goals=saved_ailu.goals,
            created_at=saved_ailu.created_at,
        )
    others = [a for a in agents if a.id != DEFAULT_AGENT_ID]
    all_agents = [builtin] + others

    active_id = str(raw.get("active_agent_id") or DEFAULT_AGENT_ID)
    if active_id not in {a.id for a in all_agents}:
        active_id = DEFAULT_AGENT_ID
    return all_agents, active_id


def _save(agents: list, active_id: str, base_dir: Optional[str] = None) -> None:
    """原子落盘 (唯一名 tmp + os.replace, 防写一半被读到半截 json)。

    [review 整改] tmp 用 mkstemp 唯一名: 旧固定 path+".tmp" 在并发写时互相
    截断, 坏 json 被 replace 上位 -> 后续 load 全回退 -> 回写抹掉非内置记录。
    """
    path = registry_path(base_dir)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    payload = {
        "agents": [a.to_dict() for a in agents],
        "active_agent_id": active_id,
    }
    fd, tmp = tempfile.mkstemp(
        dir=os.path.dirname(path), prefix=".registry_", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def list_agents(base_dir: Optional[str] = None) -> list:
    agents, _active = load_registry(base_dir)
    return agents


def get_agent(agent_id: str, base_dir: Optional[str] = None) -> Optional[AgentRecord]:
    agents, _active = load_registry(base_dir)
    return next((a for a in agents if a.id == agent_id), None)


def get_active(base_dir: Optional[str] = None) -> AgentRecord:
    """当前活动智能体 (boot 关键路径, 永不抛: 任何异常回退阿露)。"""
    try:
        agents, active_id = load_registry(base_dir)
        return next(
            (a for a in agents if a.id == active_id), default_agent(_base(base_dir))
        )
    except Exception as e:
        logger.warning(f"[Registry] get_active 异常, 回退阿露: {e}")
        return default_agent(_base(base_dir))


def _truncate_persona(persona: Any) -> str:
    return str(persona or "").strip()[:PERSONA_MAX_CHARS]


def _normalize_goals(goals: Any) -> Tuple[str, ...]:
    if not goals:
        return ()
    cleaned = tuple(
        g[:GOAL_MAX_CHARS] for g in (str(x).strip() for x in goals) if g
    )
    return cleaned[:MAX_GOALS]


def _new_agent_id(name: str, existing: set) -> str:
    """文件系统安全的唯一 id: 英文名取 slug, 中文名落时间戳 id, 冲突加序号。"""
    slug = re.sub(r"[^a-z0-9]+", "", name.lower())[:24]
    base_id = slug or f"agent_{int(time.time() * 1000)}"
    if base_id == DEFAULT_AGENT_ID:
        base_id = f"{base_id}_copy"
    candidate, seq = base_id, 2
    while candidate in existing:
        candidate = f"{base_id}_{seq}"
        seq += 1
    return candidate


def _default_snapshot(src_root: str, dst_root: str) -> None:
    """真实 snapshot 实现惰性 import (kernel.memory_snapshot, Task 2 提供)。"""
    from kernel.memory_snapshot import snapshot_memory
    snapshot_memory(src_root, dst_root)


def create_agent(name: str, identity: str = "", persona: str = "",
                 goals: Sequence[str] = (),
                 memory_mode: str = MEMORY_MODE_FRESH,
                 base_agent_id: str = DEFAULT_AGENT_ID,
                 base_dir: Optional[str] = None,
                 _snapshot_fn: Optional[Callable[[str, str], None]] = None
                 ) -> AgentRecord:
    """创建智能体并分配记忆命名空间 (三模式)。

    闸门: 名字非空/不重名, 模式合法, base 存在。shared 直指源 root;
    fresh 建空目录树; snapshot 一致性拷贝源库, 拷贝失败整体回滚
    (半成品目录删净 + 不落 registry)。_snapshot_fn 供测试注入替身。
    """
    base = _base(base_dir)
    clean_name = str(name or "").strip()
    if not clean_name:
        raise ValueError("智能体名字不能为空")
    if memory_mode not in MEMORY_MODES:
        raise ValueError(f"未知记忆模式: {memory_mode} (可选 {MEMORY_MODES})")

    # [review 整改] 写路径全程持锁: load->改->save 的 RMW 串行化,
    # 防并发端点丢更新/同 id 双记录。
    with _REGISTRY_LOCK:
        agents, active_id = load_registry(base)
        if any(a.name == clean_name for a in agents):
            raise DuplicateNameError(f"重名: 已存在名为 {clean_name} 的智能体")
        base_rec = next((a for a in agents if a.id == base_agent_id), None)
        if base_rec is None:
            raise ValueError(f"base 智能体不存在: {base_agent_id}")

        agent_id = _new_agent_id(clean_name, {a.id for a in agents})
        agent_dir = os.path.join(base, "agents", agent_id)
        memory_root = os.path.join(agent_dir, "memory")

        # [review 整改] 残留目录 fail-fast: 上次失败回滚若被 Windows 文件锁
        # 挡住会留半成品目录, 同名重建 (slug 固定) 会继承半库 -> 命名空间串味。
        if os.path.exists(agent_dir):
            raise ValueError(
                f"残留目录 {agent_dir} (可能上次创建失败未清干净), "
                "请人工删除后重试")

        if memory_mode == MEMORY_MODE_SHARED:
            memory_root = base_rec.memory_root
        elif memory_mode == MEMORY_MODE_FRESH:
            os.makedirs(os.path.join(memory_root, "soul"), exist_ok=True)
        else:  # snapshot
            os.makedirs(os.path.join(memory_root, "soul"), exist_ok=True)
            snap = _snapshot_fn or _default_snapshot
            try:
                snap(base_rec.memory_root, memory_root)
            except Exception:
                # 半成品整体回滚: 拷贝没成功的半库不能留 (记忆不可再生纪律的
                # 反面保护), 更不能落 registry 让 boot 读到半成品。
                shutil.rmtree(agent_dir, ignore_errors=True)
                if os.path.exists(agent_dir):
                    logger.error(
                        f"[Registry] snapshot 失败且回滚未删净 "
                        f"(文件被占用?), 残留目录需人工清理: {agent_dir}")
                else:
                    logger.error(
                        f"[Registry] snapshot 拷贝失败, 已回滚 {agent_dir}")
                raise

        rec = AgentRecord(
            id=agent_id,
            name=clean_name,
            identity=str(identity or "").strip()[:IDENTITY_MAX_CHARS],
            persona=_truncate_persona(persona),
            goals=_normalize_goals(goals),
            memory_mode=memory_mode,
            memory_root=memory_root,
            base_agent_id=base_rec.id,
            created_at=datetime.now().isoformat(timespec="seconds"),
            builtin=False,
        )
        _save(agents + [rec], active_id, base)
    logger.info(
        f"[Registry] 创建智能体 {rec.name} (id={rec.id}, mode={memory_mode}, "
        f"base={base_rec.id}, root={memory_root})")
    return rec


def update_agent(agent_id: str, base_dir: Optional[str] = None,
                 **fields: Any) -> AgentRecord:
    """编辑智能体可编辑字段 (name/identity/persona/goals)。

    memory_mode/memory_root/base/builtin/id/created_at 创建时定型不可改
    (改 = 换存储根, 本版本不支持); 人设改了由 D6 灵魂文档 boot 自检同步进记忆。
    """
    base = _base(base_dir)
    locked = set(fields) - _UPDATABLE_FIELDS
    if locked:
        raise ValueError(
            f"不可修改的字段: {sorted(locked)} "
            "(记忆模式/根创建时定型; 内置智能体仅可改人设/目标)")

    with _REGISTRY_LOCK:
        agents, active_id = load_registry(base)
        rec = next((a for a in agents if a.id == agent_id), None)
        if rec is None:
            raise ValueError(f"智能体不存在: {agent_id}")
        # [review 整改] builtin 仅可改 persona/goals (name/identity 以
        # character.yaml 为唯一信源, 防注册表静默架空配置, 对齐报错文案)。
        if rec.builtin and (set(fields) - {"persona", "goals"}):
            raise ValueError(
                "内置智能体仅可改人设/目标 (名字/身份以 character.yaml 为准)")

        changes: dict = {}
        if "name" in fields:
            new_name = str(fields["name"] or "").strip()
            if not new_name:
                raise ValueError("智能体名字不能为空")
            if any(a.name == new_name and a.id != agent_id for a in agents):
                raise DuplicateNameError(f"重名: {new_name}")
            changes["name"] = new_name
        if "identity" in fields:
            changes["identity"] = str(
                fields["identity"] or "").strip()[:IDENTITY_MAX_CHARS]
        if "persona" in fields:
            changes["persona"] = _truncate_persona(fields["persona"])
        if "goals" in fields:
            changes["goals"] = _normalize_goals(fields["goals"])

        updated = replace(rec, **changes)
        new_agents = [updated if a.id == agent_id else a for a in agents]
        _save(new_agents, active_id, base)
    logger.info(f"[Registry] 更新智能体 {updated.name} 字段: {sorted(changes)}")
    return updated


def activate_agent(agent_id: str, base_dir: Optional[str] = None) -> AgentRecord:
    """切换活动智能体 (仅落 registry, boot 时读取 -> 重启生效)。"""
    base = _base(base_dir)
    with _REGISTRY_LOCK:
        agents, _active = load_registry(base)
        rec = next((a for a in agents if a.id == agent_id), None)
        if rec is None:
            raise ValueError(f"智能体不存在: {agent_id}")
        _save(agents, agent_id, base)
    logger.info(f"[Registry] 活动智能体切换为 {rec.name} (重启后生效)")
    return rec

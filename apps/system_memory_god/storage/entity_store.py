import os
import json
import asyncio
import time
from typing import Optional

from kernel.logger import get_logger

logger = get_logger(__name__)


class EntityStoreCorruptError(Exception):
    """实体库文件损坏且无可用备份时抛出, 写操作必须放弃 _save 以防空库覆盖。"""


class EntityStore:
    """
    [双时态版本链 EntityStore]

    实体知识图谱存储, 每条 (entity, key) 事实挂一条版本链:
      - 顶层字段: 当前 alive 版本 (默认查询/注入返回的就是它)
      - history:  被 supersede 的历史版本, 按 valid 时间倒序保留

    双时态字段 (Zep/Graphiti 范式):
      - valid_from / valid_to: 事实在现实世界中的有效区间; valid_to=None 表示 alive
      - tx_time:                系统得知该事实的时刻

    写入语义 (supersede):
      - 新值 == 当前值 -> 不创建新版本, 仅合并 weight/is_core_fact 并累加 hit_count
      - 新值 != 当前值 -> 旧版本收口 (valid_to=now) 移入 history, 新版本成为 alive
    """
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.file_path = os.path.join(data_dir, "entities.json")
        self._lock = asyncio.Lock()
        self._ensure_file()
        # v0.6.0 双轴社交: person 初始好感度/注意度 + 别名映射 (胖脸鱼摆摆->胖脸鱼)
        self._social_init = self._load_social_init()

    def _ensure_file(self):
        if not os.path.exists(self.file_path):
            with open(self.file_path, 'w', encoding='utf-8') as f:
                json.dump({}, f, ensure_ascii=False, indent=4)

    def _load_social_init(self) -> dict:
        """加载 config/social.yaml 的 person 初始双轴 + 别名。无则空 dict。"""
        import yaml
        cfg_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(
                os.path.dirname(os.path.abspath(__file__))))),
            "config", "social.yaml")
        if not os.path.exists(cfg_path):
            return {"persons": {}}
        try:
            with open(cfg_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {"persons": {}}
        except Exception as e:
            logger.warning(f"[EntityStore] social.yaml 读取失败: {e}")
            return {"persons": {}}

    def _resolve_alias(self, entity: str) -> str:
        """归一别名 (胖脸鱼摆摆 -> 胖脸鱼)。无别名映射则原样返回。"""
        persons = self._social_init.get("persons", {})
        cfg = persons.get(entity)
        if isinstance(cfg, dict) and cfg.get("alias_of"):
            return cfg["alias_of"]
        return entity

    async def _load(self):
        def _do_load():
            # 1. 主文件存在且可解析 -> 返回
            if os.path.exists(self.file_path):
                try:
                    with open(self.file_path, 'r', encoding='utf-8') as f:
                        return json.load(f)
                except Exception as e:
                    logger.error(f"[EntityStore] 主库损坏: {e}, 尝试 .bak 恢复")
            # 2. 回退备份 (R1 原子写留下的 .bak)
            bak = self.file_path + ".bak"
            if os.path.exists(bak):
                try:
                    with open(bak, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    logger.warning("[EntityStore] 主库损坏, 已从 .bak 恢复")
                    return data
                except Exception as e:
                    logger.error(f"[EntityStore] 备份也损坏: {e}")
            # 3. 主库存在但损坏且无可用备份 -> 抛异常, 阻止写操作拿到空库回写覆盖 (R2 连锁清空)
            if os.path.exists(self.file_path):
                raise EntityStoreCorruptError(
                    f"实体库 {self.file_path} 损坏且无可用备份, 拒绝加载以防空库覆盖")
            # 4. 全新环境 (文件不存在) -> 空库
            return {}
        return await asyncio.to_thread(_do_load)

    async def _save(self, data):
        def _do_save():
            # 原子写 (R1): 写 .tmp -> fsync -> 回读校验 -> 旧文件转 .bak -> os.replace 原子替换。
            # 中途崩溃只会留下 .tmp (半截但未替换), 主文件始终是上一份完整版本。
            tmp_path = self.file_path + ".tmp"
            with open(tmp_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
                f.flush()
                os.fsync(f.fileno())
            with open(tmp_path, 'r', encoding='utf-8') as f:  # 回读校验防半截 JSON
                json.load(f)
            if os.path.exists(self.file_path):
                try:
                    os.replace(self.file_path, self.file_path + ".bak")
                except OSError as e:
                    logger.warning(f"[EntityStore] 备份旧库失败 (继续替换): {e}")
            os.replace(tmp_path, self.file_path)  # Windows/POSIX 同卷原子替换
        await asyncio.to_thread(_do_save)

    def _normalize_value(self, value) -> dict:
        """
        规整一条事实为带双时态版本链的标准结构。
        兼容三种历史格式:
          - 旧字符串 -> 包成 value
          - 旧结构化 (无 valid_from/history) -> 补默认 (透明迁移)
          - 新结构化 -> 原样返回
        """
        if isinstance(value, dict):
            v = dict(value)
        else:
            v = {"value": str(value)}
        v.setdefault("value", "")
        v.setdefault("is_core_fact", False)
        v.setdefault("weight", 1)
        v.setdefault("hit_count", 0)
        v.setdefault("updated_at", time.strftime("%Y-%m-%d %H:%M:%S"))
        # 双时态版本链字段 (旧库透明迁移: 视为 alive, valid_from 未知记为 0)
        v.setdefault("valid_from", 0.0)
        v.setdefault("valid_to", None)
        v.setdefault("tx_time", 0.0)
        v.setdefault("history", [])
        return v

    @staticmethod
    def _stamp_version() -> dict:
        """构造一个当前时刻的时间戳集合, 供新建/supersede 版本复用。"""
        now = time.time()
        return {
            "valid_from": now,
            "valid_to": None,
            "tx_time": now,
        }

    async def update_entity(self, entity_name: str, key: str, value: str,
                            is_core_fact: bool = False, weight: int = 1,
                            return_snapshot: bool = False):
        """
        写入/更新一条事实。

        [双时态 supersede] 当新值与当前 alive 值不同时, 旧版本收口并移入 history,
        新版本成为 alive; 相同值则仅合并属性 (不产生新版本), 避免版本链无谓膨胀。
        """
        async with self._lock:
            try:
                data = await self._load()
            except EntityStoreCorruptError as e:
                logger.error(f"[EntityStore] 跳过写入 (实体库损坏, 拒绝空库覆盖): {e}")
                return False
            if entity_name not in data:
                data[entity_name] = {}

            old = self._normalize_value(data[entity_name].get(key, {}))
            new_value = str(value)
            now = time.time()

            if old.get("value") and old["value"] != new_value:
                # 值变更 -> supersede: 旧版本收口入 history
                superseded = {
                    "value": old["value"],
                    "valid_from": old.get("valid_from", 0.0),
                    "valid_to": now,
                    "tx_time": old.get("tx_time", 0.0),
                    "is_core_fact": old.get("is_core_fact", False),
                    "weight": old.get("weight", 1),
                }
                old["history"] = old.get("history", []) + [superseded]
                old["value"] = new_value
                stamps = self._stamp_version()
                old["valid_from"] = stamps["valid_from"]
                old["valid_to"] = None
                old["tx_time"] = stamps["tx_time"]
                logger.info(
                    f"[EntityStore] 事实演进: 【{entity_name}.{key}】 "
                    f"'{superseded['value']}' -> '{new_value}' (历史版本已归档)"
                )
            else:
                # 相同值或首次写入: 仅更新当前 alive 版本
                old["value"] = new_value
                if old.get("valid_to") is None and old.get("valid_from") == 0.0 and not old.get("history"):
                    # 首次写入补全双时态戳 (旧库迁移过来的首写)
                    stamps = self._stamp_version()
                    old.update(stamps)

            old["weight"] = max(weight, old.get("weight", 1))
            old["hit_count"] = old.get("hit_count", 0) + 1
            old["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            if is_core_fact:
                old["is_core_fact"] = True
            data[entity_name][key] = old

            await self._save(data)
            # 调用方若需要给异步下游绑定版本，必须消费锁内生成的快照；
            # update 后再 get 会与另一条并发更新交错，拼出“旧值+新 revision”。
            return dict(old) if return_snapshot else True

    async def demote_entity_core_facts(self, entity_name: str) -> int:
        """[0.3.17.0 P4-b D6] 把某实体名下全部 core 事实降级为非 core (不删值)。

        供 snapshot 继承源智能体身份处置: 拷贝来的源自我描述 (如实体"阿露"的
        名字/身份) 降为普通记忆史料, 新智能体的 [自我身份] core 随后播种上位。
        返回降级条数; 库损坏/实体不存在 -> 0 (不抛, boot 路径非致命)。
        """
        if not entity_name:
            return 0
        async with self._lock:
            try:
                data = await self._load()
            except EntityStoreCorruptError as e:
                logger.error(f"[EntityStore] 跳过 core 降级 (实体库损坏): {e}")
                return 0
            attrs = data.get(entity_name)
            if not isinstance(attrs, dict):
                return 0
            demoted = 0
            for key, val in attrs.items():
                norm = self._normalize_value(val)
                if norm.get("is_core_fact"):
                    norm["is_core_fact"] = False
                    attrs[key] = norm
                    demoted += 1
            if demoted:
                try:
                    await self._save(data)
                except Exception as e:
                    logger.error(f"[EntityStore] core 降级保存失败: {e}")
                    return 0
            return demoted

    async def tag_as_core_fact(self, entity_name: str, key: str):
        async with self._lock:
            try:
                data = await self._load()
            except EntityStoreCorruptError as e:
                logger.error(f"[EntityStore] 跳过写入 (实体库损坏, 拒绝空库覆盖): {e}")
                return False
            if entity_name in data and key in data[entity_name]:
                val = self._normalize_value(data[entity_name][key])
                val["is_core_fact"] = True
                val["weight"] = max(val.get("weight", 1), 10)
                data[entity_name][key] = val
                await self._save(data)
            return True

    async def get_entity(self, entity_name: str):
        try:
            data = await self._load()
        except EntityStoreCorruptError as e:
            logger.warning(f"[EntityStore] 读取降级 (实体库损坏): {e}")
            return None
        raw = data.get(entity_name, {})
        result = {}
        for k, v in raw.items():
            normalized = self._normalize_value(v)
            result[k] = normalized
        return result if result else None

    async def get_core_facts(self):
        try:
            data = await self._load()
        except EntityStoreCorruptError as e:
            logger.warning(f"[EntityStore] 读取降级 (实体库损坏): {e}")
            return []
        facts = []
        for entity_name, attrs in data.items():
            for key, val in attrs.items():
                normalized = self._normalize_value(val)
                if normalized.get("is_core_fact", False):
                    facts.append({
                        "entity": entity_name,
                        "key": key,
                        "value": normalized["value"],
                        "weight": normalized.get("weight", 1),
                        "hit_count": normalized.get("hit_count", 0)
                    })
        facts.sort(key=lambda x: x["weight"], reverse=True)
        return facts

    async def record_hit(self, entity_name: str, key: str):
        """
        [Smallville 频次接线] 累加实体属性 hit_count。
        供 spotlight_search 实体被话题激活命中时调用, 让核心事实按频次排序浮现。
        """
        async with self._lock:
            try:
                data = await self._load()
            except EntityStoreCorruptError as e:
                logger.error(f"[EntityStore] 跳过写入 (实体库损坏, 拒绝空库覆盖): {e}")
                return False
            if entity_name in data and key in data[entity_name]:
                val = self._normalize_value(data[entity_name][key])
                val["hit_count"] = val.get("hit_count", 0) + 1
                data[entity_name][key] = val
                await self._save(data)
                return True
            return False

    async def forget_entity(self, entity_name: str) -> bool:
        """[MemGPT 自治] 主脑主动遗忘: 物理删除整个实体节点。"""
        if not entity_name:
            return False
        async with self._lock:
            try:
                data = await self._load()
            except EntityStoreCorruptError as e:
                logger.error(f"[EntityStore] 跳过写入 (实体库损坏, 拒绝空库覆盖): {e}")
                return False
            if entity_name in data:
                del data[entity_name]
                await self._save(data)
                return True
            return False

    async def forget_fact(self, entity_name: str, key: str) -> bool:
        """[MemGPT 自治] 删除单条事实 (entity, key): 整条版本链 (alive+history) 物理清除。

        供 memory_forget 的"删单条事实"路径: 主脑说"忘掉胖脸鱼的某个设定"时,
        比删整个实体节点粒度更细, 不误伤同一实体的其它属性。
        """
        if not entity_name or not key:
            return False
        async with self._lock:
            try:
                data = await self._load()
            except EntityStoreCorruptError as e:
                logger.error(f"[EntityStore] 跳过写入 (实体库损坏, 拒绝空库覆盖): {e}")
                return False
            attrs = data.get(entity_name, {})
            if key in attrs:
                del attrs[key]
                if attrs:
                    data[entity_name] = attrs
                else:
                    # 该实体已无任何属性, 连带删除空节点
                    data.pop(entity_name, None)
                await self._save(data)
                return True
            return False

    @staticmethod
    def _lcs_len(a: str, b: str) -> int:
        """最长公共子串长度 (非子序列)。供 forget_by_value 模糊匹配兜底。"""
        if not a or not b:
            return 0
        prev = [0] * (len(b) + 1)
        best = 0
        for i in range(1, len(a) + 1):
            cur = [0] * (len(b) + 1)
            ai = a[i - 1]
            for j in range(1, len(b) + 1):
                if ai == b[j - 1]:
                    cur[j] = prev[j - 1] + 1
                    if cur[j] > best:
                        best = cur[j]
            prev = cur
        return best

    def _value_matches(self, frag: str, value: str, entity_names: set) -> bool:
        """forget_by_value 的匹配判定:
        1) frag 是 value 子串 (frag in value), 或反向 (value in frag) -> 命中
        2) 否则求最长公共子串, 若 >=3 字且不是任何已知 entity 名 (避免实体名本身误命中)
           -> 命中。例: frag='胖脸鱼是小僵尸' vs value='是小僵尸,与微信用户(Ab)...'
           LCS='是小僵尸'(5字) 命中; 而 frag vs '胖脸鱼喜欢咖啡' LCS='胖脸鱼'(实体名) 不命中。
        """
        if not frag or not value:
            return False
        if frag in value or value in frag:
            return True
        lcs = self._lcs_len(frag, value)
        if lcs >= 3:
            # 取一个长度==lcs 的公共子串判是否纯 entity 名
            # (近似: 若任一 entity 名长度>=3 且 frag/value 共含该 entity 名 -> 视为实体名命中, 不算)
            for name in entity_names:
                if len(name) >= 3 and name in frag and name in value:
                    return False
            return True
        return False

    async def forget_by_value(self, fragment: str) -> list:
        """[MemGPT 自治] 按 value 片段模糊匹配删除: 跨所有实体扫描, 删掉 alive 或
        history 中 value 含该片段 (或语义片段 LCS>=3) 的版本。

        主脑用自然语言下遗忘指令 (如 target='胖脸鱼是小僵尸') 时, 精确 entity 名/key
        对不上, 靠 value 语义片段命中。返回被清理的 {entity, key, removed, fact_dropped}
        列表。仅删命中版本; alive 被删后回退到 history 最近版本, 无 history 可回退则
        整条事实删除 (fact_dropped=True)。
        """
        if not fragment:
            return []
        frag = fragment.strip()
        async with self._lock:
            try:
                data = await self._load()
            except EntityStoreCorruptError as e:
                logger.error(f"[EntityStore] 跳过写入 (实体库损坏, 拒绝空库覆盖): {e}")
                return []
            entity_names = set(data.keys())
            cleaned: list = []
            for entity_name, attrs in list(data.items()):
                for key, val in list(attrs.items()):
                    v = self._normalize_value(val)
                    removed = 0
                    # history 命中版本 -> 先剔除该历史版本
                    hist = v.get("history", [])
                    new_hist = [h for h in hist
                                if not self._value_matches(frag, str(h.get("value", "")), entity_names)]
                    removed += len(hist) - len(new_hist)
                    v["history"] = new_hist
                    # alive 命中
                    if self._value_matches(frag, str(v.get("value", "")), entity_names):
                        removed += 1
                        if new_hist:
                            # 回退到 history 最近版本
                            best = max(new_hist, key=lambda h: float(h.get("valid_from", 0.0) or 0.0))
                            v["history"] = [h for h in new_hist if h is not best]
                            v["value"] = best.get("value", "")
                            v["valid_from"] = best.get("valid_from", 0.0)
                            v["valid_to"] = None
                            v["tx_time"] = time.time()
                            attrs[key] = v
                            cleaned.append({"entity": entity_name, "key": key, "removed": removed, "fact_dropped": False})
                        else:
                            # 无 history 可回退, 整条事实删除
                            del attrs[key]
                            cleaned.append({"entity": entity_name, "key": key, "removed": removed, "fact_dropped": True})
                    elif removed > 0:
                        attrs[key] = v
                        cleaned.append({"entity": entity_name, "key": key, "removed": removed, "fact_dropped": False})
                if attrs:
                    data[entity_name] = attrs
                else:
                    data.pop(entity_name, None)
            await self._save(data)
            return cleaned

    async def entity_history(self, entity_name: str, key: str) -> list:
        """
        [双时态版本链] 返回某条事实的完整版本链 (alive + superseded), 按时间倒序。
        供 entity_history 命令回溯, 了解事实如何随时间演进。

        组装而非按时间戳排序: history 是按发生顺序 append (oldest->newest) 的,
        直接 alive + reversed(history) 即得到确定的最新->最旧顺序,
        避免粗时钟下多次写入 valid_from 相同导致排序不稳定。
        """
        try:
            data = await self._load()
        except EntityStoreCorruptError as e:
            logger.warning(f"[EntityStore] 读取降级 (实体库损坏): {e}")
            return []
        attrs = data.get(entity_name, {})
        val = self._normalize_value(attrs.get(key, {}))
        chain = []
        # 当前 alive 版本 (最新)
        chain.append({
            "value": val["value"],
            "valid_from": val.get("valid_from", 0.0),
            "valid_to": val.get("valid_to"),
            "tx_time": val.get("tx_time", 0.0),
            "status": "alive",
        })
        # 历史版本 reversed: 由新到旧
        for h in reversed(val.get("history", [])):
            chain.append({
                "value": h.get("value", ""),
                "valid_from": h.get("valid_from", 0.0),
                "valid_to": h.get("valid_to"),
                "tx_time": h.get("tx_time", 0.0),
                "status": "superseded",
            })
        return chain

    # =================== [v0.6.0 双轴社交: 好感度/注意度] ===================
    # person 双轴存为 person 实体的约定属性 key "__social__":
    #   {favorability, attention, last_interact_ts}
    # 审批记录存顶层 "__social_history__" 数组 (entity/delta/reason/ts/source)。
    # 复用 _load/_save 原子写 + asyncio.Lock (v0.4.9 R1)。
    _SOCIAL_KEY = "__social__"
    _SOCIAL_HISTORY_KEY = "__social_history__"
    _BASELINE = 50  # attention 衰减回归基准

    def _default_social(self, entity: str) -> dict:
        """从 social.yaml 取 person 初始双轴, 无则默认 50/50。"""
        persons = self._social_init.get("persons", {})
        cfg = persons.get(entity)
        if isinstance(cfg, dict) and "favorability" in cfg:
            return {
                "favorability": float(cfg.get("favorability", 50)),
                "attention": float(cfg.get("attention", 50)),
                "last_interact_ts": 0.0,
            }
        return {"favorability": 50.0, "attention": 50.0, "last_interact_ts": 0.0}

    async def get_social(self, entity: str) -> dict:
        """读 person 双轴 (归一别名后)。无则返回默认 (首次访问不落盘, 演化时才落)。"""
        entity = self._resolve_alias(entity)
        try:
            data = await self._load()
        except EntityStoreCorruptError:
            return self._default_social(entity)
        attrs = data.get(entity, {})
        social = attrs.get(self._SOCIAL_KEY)
        if isinstance(social, dict) and "favorability" in social:
            return social
        return self._default_social(entity)

    async def get_socials(self, persons: list) -> list:
        """批量读多人双轴 (一次 _load 避 N 次文件读)。返回 [{name(归一), favorability, attention}]
        按 attention 降序, 无记录用 _default_social 兜底。供 prompt 注入亲疏列表 (0.1.7.0 双轴上线)。"""
        if not persons:
            return []
        try:
            data = await self._load()
        except EntityStoreCorruptError:
            return []
        seen = set()
        result = []
        for p in persons:
            if not p or p in ("未知", "self"):
                continue
            name = self._resolve_alias(p)
            if name in seen:
                continue
            seen.add(name)
            attrs = data.get(name, {})
            social = attrs.get(self._SOCIAL_KEY)
            if not isinstance(social, dict) or "favorability" not in social:
                social = self._default_social(name)
            result.append({
                "name": name,
                "favorability": float(social.get("favorability", 50.0)),
                "attention": float(social.get("attention", 50.0)),
            })
        result.sort(key=lambda x: x["attention"], reverse=True)
        return result

    async def adjust_social(self, entity: str, fav_delta: float, att_delta: float,
                            reason: str, source: str) -> dict:
        """VIP 审批式调整双轴: 增量 + bounded[0,100] + 记审批记录。

        折中: 直接 apply delta (老板指令有权威性) + 留 __social_history__ 痕迹供阿露感知。
        """
        entity = self._resolve_alias(entity)
        async with self._lock:
            try:
                data = await self._load()
            except EntityStoreCorruptError as e:
                logger.error(f"[EntityStore] 跳过双轴调整 (库损坏): {e}")
                return {"ok": False, "error": "实体库损坏"}
            attrs = data.setdefault(entity, {})
            social = attrs.get(self._SOCIAL_KEY)
            if not isinstance(social, dict) or "favorability" not in social:
                social = self._default_social(entity)
            old_fav, old_att = social["favorability"], social["attention"]
            social["favorability"] = max(0.0, min(100.0, old_fav + float(fav_delta)))
            social["attention"] = max(0.0, min(100.0, old_att + float(att_delta)))
            social["last_interact_ts"] = time.time()
            attrs[self._SOCIAL_KEY] = social
            # 审批留痕
            history = data.setdefault(self._SOCIAL_HISTORY_KEY, [])
            history.append({
                "entity": entity,
                "fav_delta": float(fav_delta),
                "att_delta": float(att_delta),
                "reason": reason,
                "source": source,
                "ts": time.time(),
            })
            try:
                await self._save(data)
            except EntityStoreCorruptError as e:
                logger.error(f"[EntityStore] 双轴调整保存失败: {e}")
                return {"ok": False, "error": "实体库损坏"}
            logger.info(
                f"[EntityStore] 双轴调整 [{entity}] fav {old_fav}->{social['favorability']}, "
                f"att {old_att}->{social['attention']} (理由: {reason}, 来源: {source})"
            )
            return {"ok": True, "favorability": social["favorability"],
                    "attention": social["attention"]}

    async def touch_interaction(self, entity: str) -> None:
        """演化钩子: 交互发生时调。attention +2 (近期熟络度升), favorability 不动 (粘性)。

        供 _background_user/agent_listener 在 user_speak/agent_speak 时调。
        别名归一后 touch。
        """
        if not entity:
            return
        entity = self._resolve_alias(entity)
        async with self._lock:
            try:
                data = await self._load()
            except EntityStoreCorruptError:
                return
            attrs = data.setdefault(entity, {})
            social = attrs.get(self._SOCIAL_KEY)
            if not isinstance(social, dict) or "favorability" not in social:
                social = self._default_social(entity)
            social["attention"] = min(100.0, social["attention"] + 2.0)
            social["last_interact_ts"] = time.time()
            attrs[self._SOCIAL_KEY] = social
            try:
                await self._save(data)
            except EntityStoreCorruptError:
                return

    async def decay_all_attention(self, factor: float = 0.95) -> int:
        """GC daemon 调: 所有 person 的 attention 向 baseline 50 缓慢回归。

        attention = 50 + (attention-50)*factor。久不联系渐忘, 不骤降。favorability 不动。
        返回处理的人数。
        """
        async with self._lock:
            try:
                data = await self._load()
            except EntityStoreCorruptError as e:
                logger.error(f"[EntityStore] attention 衰减跳过 (库损坏): {e}")
                return 0
            n = 0
            for entity, attrs in data.items():
                if entity == self._SOCIAL_HISTORY_KEY:
                    continue
                social = attrs.get(self._SOCIAL_KEY) if isinstance(attrs, dict) else None
                if not isinstance(social, dict) or "attention" not in social:
                    continue
                social["attention"] = self._BASELINE + (social["attention"] - self._BASELINE) * factor
                n += 1
            if n > 0:
                try:
                    await self._save(data)
                except EntityStoreCorruptError:
                    return 0
            return n

    async def boost_interest(self, topic: str, delta: float, damping: Optional[float] = None) -> float:
        """[0.2.9.0] 涨【阿露兴趣.topic】值 (read-modify-write 走 update_entity 留事实演进)。

        damping 非 None 时同写平行实体【阿露兴趣阻尼.topic】= 衰减乘数 (老板"多关注X":
        越小衰减越慢, 但仍衰减, 无永久兴趣)。返回涨后新值。
        """
        entity = await self.get_entity("阿露兴趣") or {}
        old = 0.0
        rec = entity.get(topic)
        if rec:
            try:
                old = float(rec.get("value", 0))
            except (TypeError, ValueError):
                old = 0.0
        new = round(max(0.0, old + float(delta)), 2)
        await self.update_entity("阿露兴趣", topic, str(new))
        if damping is not None:
            await self.update_entity("阿露兴趣阻尼", topic, str(round(float(damping), 2)))
        return new

    async def decay_interests(self, factor: float = 0.9, floor: float = 0.3) -> int:
        """[0.2.9.0] GC daemon 调: 所有【阿露兴趣】值衰减 (确定性规则, 不靠 LLM)。

        新值 = 旧值 * factor * 阻尼(topic, 缺省1.0); 值 <= floor -> 遗忘 (死兴趣消失,
        自动掉出闸门 top-N)。阻尼读平行实体【阿露兴趣阻尼】(老板"多关注"设的乘数)。
        返回处理的话题数 (含触底遗忘的)。
        """
        async with self._lock:
            try:
                data = await self._load()
            except EntityStoreCorruptError as e:
                logger.error(f"[EntityStore] 兴趣衰减跳过 (库损坏): {e}")
                return 0
            interests = data.get("阿露兴趣")
            if not isinstance(interests, dict) or not interests:
                return 0
            damping_map = data.get("阿露兴趣阻尼", {})
            to_forget = []
            n = 0
            for topic, rec in interests.items():
                norm = self._normalize_value(rec)
                try:
                    old = float(norm.get("value", 0))
                except (TypeError, ValueError):
                    continue
                damp = 1.0
                damp_rec = damping_map.get(topic) if isinstance(damping_map, dict) else None
                if damp_rec:
                    try:
                        damp = float(self._normalize_value(damp_rec).get("value", 1.0))
                    except (TypeError, ValueError):
                        damp = 1.0
                new = old * factor * damp
                if new <= floor:
                    to_forget.append(topic)
                    logger.info(f"[EntityStore] 兴趣触底消逝: 【阿露兴趣.{topic}】 {old} -> {new:.2f}")
                else:
                    norm["value"] = str(round(new, 2))
                    norm["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
                    interests[topic] = norm
                n += 1
            for topic in to_forget:
                interests.pop(topic, None)
            if not interests:
                data.pop("阿露兴趣", None)
            if n > 0:
                try:
                    await self._save(data)
                except EntityStoreCorruptError:
                    return 0
            return n

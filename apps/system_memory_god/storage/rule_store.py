import os
import json
import asyncio
import time
import math
import shutil

from kernel.logger import get_logger

logger = get_logger(__name__)


class RuleStore:

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.file_path = os.path.join(data_dir, "rules.json")
        # 性格体检状态独立文件，避免与 rules.json 结构耦合
        # 存储 last_checkup_at 时间戳，用于风暴防护的冷却判定
        self.checkup_state_path = os.path.join(data_dir, "checkup_state.json")
        self._cache = None
        # [互斥锁] 锁住 load-modify-save 整段, 杜绝并发读写交错导致 _cache 被覆盖
        self._lock = asyncio.Lock()
        self._ensure_file()

    def _ensure_file(self):
        if not os.path.exists(self.file_path):
            self._migrate_from_rules_md_sync()
        else:
            self._load_sync()

    @staticmethod
    def _validate_rules_data(data: object) -> dict:
        if not isinstance(data, dict) or not isinstance(data.get("rules"), list):
            raise ValueError("rules.json 必须是包含 rules 数组的对象")
        return data

    @staticmethod
    def _atomic_write_json_sync(path: str, data: object,
                                create_backup: bool = True) -> None:
        """同目录临时文件写入、落盘校验、备份并原子替换。"""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp_path = path + ".tmp"
        backup_path = path + ".bak"
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
                f.flush()
                os.fsync(f.fileno())
            with open(tmp_path, "r", encoding="utf-8") as f:
                json.load(f)
            if create_backup and os.path.exists(path):
                shutil.copy2(path, backup_path)
            os.replace(tmp_path, path)
        finally:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

    def _migrate_from_rules_md_sync(self):
        rules_md_path = os.path.join(self.data_dir, "RULES.md")
        rules = []
        if os.path.exists(rules_md_path):
            try:
                with open(rules_md_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        text = line.lstrip("- ").strip()
                        if text.startswith("[潜意识顿悟]"):
                            text = text.replace("[潜意识顿悟]", "").strip()
                        rules.append(text)
            except Exception as e:
                logger.warning(f"[RuleStore] RULES.md 迁移读取失败: {e}")

        data = {"rules": []}
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        for i, text in enumerate(rules):
            data["rules"].append({
                "id": i + 1,
                "text": text,
                "created_at": now,
                "hit_count": 0,
                "last_active_at": now,
            })

        self._atomic_write_json_sync(self.file_path, data)

        self._cache = data
        logger.info(f"[RuleStore] 从 RULES.md 迁移 {len(rules)} 条规则到 rules.json。")

    def _load_sync(self):
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                self._cache = self._validate_rules_data(json.load(f))
        except Exception as primary_error:
            backup_path = self.file_path + ".bak"
            try:
                with open(backup_path, "r", encoding="utf-8") as f:
                    recovered = self._validate_rules_data(json.load(f))
                self._atomic_write_json_sync(
                    self.file_path, recovered, create_backup=False,
                )
                self._cache = recovered
                logger.warning(
                    f"[RuleStore] 主规则文件损坏，已从备份恢复: {primary_error}"
                )
            except Exception as backup_error:
                raise RuntimeError(
                    "rules.json 与备份均不可读取，拒绝以空规则继续运行"
                ) from backup_error
        return self._cache

    async def _load(self):
        return await asyncio.to_thread(self._load_sync)

    async def _save(self):
        await asyncio.to_thread(
            self._atomic_write_json_sync, self.file_path, self._cache,
        )

    async def get_all_rules(self) -> list:
        data = await self._load()
        return data.get("rules", [])

    async def get_active_rules_text(self, limit: int = 8) -> str:
        """
        返回注入系统提示词的性格规则文本。

        注入精简: 取 Top-N (默认 8 条), 避免规则库膨胀后把全部规则灌进 system prompt。
        P2: 排序键从纯 hit_count 升级为 饱和频次 + 时效衰减 (读 last_active_at,
        复用 Smallville exp(-t/7d) 形式)。频次为主、时效为辅, 让长期不活跃的高频规则
        淡出、新规则可冒头, 防旧规则长期霸占 prompt 位致注入质量随时间退化。
        """
        rules = await self.get_all_rules()
        if not rules:
            return ""
        now = time.time()
        half_life = 7 * 24 * 3600

        def _score(r: dict) -> float:
            hits = float(r.get("hit_count", 0))
            hit_freq = hits / (hits + 5.0)  # 饱和归一, 同 Smallville
            recency = 0.0
            last = r.get("last_active_at")
            if last:
                try:
                    recency = math.exp(
                        -(now - time.mktime(time.strptime(last, "%Y-%m-%d %H:%M:%S"))) / half_life
                    )
                except (ValueError, TypeError):
                    recency = 0.0  # 旧数据/格式异常回退, 不报错
            return hit_freq + 0.5 * recency  # 频次为主, 时效为辅

        sorted_rules = sorted(rules, key=_score, reverse=True)
        top = sorted_rules[:limit] if limit and limit > 0 else sorted_rules
        lines = [f"- {r['text']}" for r in top]
        return "\n".join(lines)

    async def add_rule(self, text: str) -> bool:
        async with self._lock:
            data = await self._load()
            now = time.strftime("%Y-%m-%d %H:%M:%S")

            for r in data.get("rules", []):
                if r["text"] == text:
                    r["hit_count"] = r.get("hit_count", 0) + 1
                    r["last_active_at"] = now
                    self._cache = data
                    await self._save()
                    return False

            new_id = max((r["id"] for r in data.get("rules", [])), default=0) + 1
            data["rules"].append({
                "id": new_id,
                "text": text,
                "created_at": now,
                "hit_count": 1,
                "last_active_at": now,
            })
            self._cache = data
            await self._save()
            return True

    async def restore_rule(self, snapshot: dict) -> bool:
        """按原 ID/元数据恢复治理快照；ID 已占用时拒绝覆盖。"""
        if not isinstance(snapshot, dict):
            return False
        try:
            rule_id = int(snapshot.get("id"))
        except (TypeError, ValueError):
            return False
        text = str(snapshot.get("text") or "").strip()
        if rule_id < 0 or not text:
            return False
        async with self._lock:
            data = await self._load()
            if any(int(row.get("id", -1)) == rule_id
                   for row in data.get("rules", []) if isinstance(row, dict)):
                return False
            restored = dict(snapshot)
            restored["id"] = rule_id
            restored["text"] = text
            data["rules"].append(restored)
            data["rules"].sort(key=lambda row: int(row.get("id", 0)))
            self._cache = data
            await self._save()
            return True

    async def replace_all_rules(self, new_rules: list):
        async with self._lock:
            now = time.strftime("%Y-%m-%d %H:%M:%S")
            data = {"rules": []}
            for i, text in enumerate(new_rules):
                data["rules"].append({
                    "id": i + 1,
                    "text": text,
                    "created_at": now,
                    "hit_count": 0,
                    "last_active_at": now,
                })
            self._cache = data
            await self._save()

    async def record_hit(self, rule_index: int):
        async with self._lock:
            data = await self._load()
            rules = data.get("rules", [])
            if 0 <= rule_index < len(rules):
                rules[rule_index]["hit_count"] = rules[rule_index].get("hit_count", 0) + 1
                rules[rule_index]["last_active_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
                self._cache = data
                await self._save()

    async def record_hit_by_text(self, text: str) -> bool:
        """
        按规则文本累加 hit_count。供 spotlight_search 在规则被话题激活注入时调用,
        让 get_active_rules_text 的 Top-N 排序真正反映历史激活频次。
        返回是否命中至少一条规则。
        """
        if not text:
            return False
        async with self._lock:
            data = await self._load()
            now = time.strftime("%Y-%m-%d %H:%M:%S")
            hit = False
            for r in data.get("rules", []):
                if r["text"] == text:
                    r["hit_count"] = r.get("hit_count", 0) + 1
                    r["last_active_at"] = now
                    hit = True
            if hit:
                self._cache = data
                await self._save()
            return hit

    async def forget_rule_by_text(self, text: str) -> bool:
        """[MemGPT 自治] 主脑主动遗忘: 按文本物理删除一条过时/虚假规则。"""
        if not text:
            return False
        async with self._lock:
            data = await self._load()
            before = len(data.get("rules", []))
            data["rules"] = [r for r in data.get("rules", []) if r["text"] != text]
            if len(data["rules"]) < before:
                self._cache = data
                await self._save()
                return True
            return False

    async def update_rule(self, rule_id: int, new_text: str) -> bool:
        """[0.3.34.0 B1c] 老板侧编辑单条规矩: 改文本, 保留 hit_count/
        created_at (演化历史不清零)。文本未变/空文本/无此 id 返 False。"""
        new_text = str(new_text or "").strip()
        if not new_text:
            return False
        async with self._lock:
            data = await self._load()
            for r in data.get("rules", []):
                if int(r.get("id", -1)) == int(rule_id):
                    if r.get("text", "") == new_text:
                        return False
                    r["text"] = new_text
                    self._cache = data
                    await self._save()
                    return True
            return False

    async def forget_rule_by_id(self, rule_id: int) -> bool:
        """[0.3.34.0 B1c] 老板侧删除单条规矩 (按 id, 比按文本稳)。"""
        async with self._lock:
            data = await self._load()
            before = len(data.get("rules", []))
            data["rules"] = [r for r in data.get("rules", [])
                             if int(r.get("id", -1)) != int(rule_id)]
            if len(data["rules"]) < before:
                self._cache = data
                await self._save()
                return True
            return False

    async def get_rule_count(self) -> int:
        rules = await self.get_all_rules()
        return len(rules)

    async def should_trigger_checkup(
        self,
        checkup_interval_seconds: int = 24 * 3600,
        cooldown_seconds: int = 3600,
        count_threshold: int = 20,
    ) -> bool:
        """
        性格体检触发判定 (次世代: 废除 >=20 永真风暴源)。

        触发条件 (单一时间步长判定, 不再随规则数无限增长而永久触发):
        - 规则数 >= 5 (太少不值得体检)
        - 且 距上次体检超过 checkup_interval_seconds (默认 24 小时)

        防风暴:
        - 冷却期 cooldown_seconds (默认 1 小时) 内绝不再次触发,
          避免失败重试或循环触发造成的 API 风暴。
        - last_checkup_at 由 mark_checkup_attempt() 在体检「判定成立那一刻」即登记
          (已前移到 reflection._dream_loop), 这样即使体检超时/失败, 冷却期同样生效。

        count_threshold 参数保留仅为向后兼容签名, 不再参与触发逻辑。
        """
        rules = await self.get_all_rules()
        if len(rules) < 5:
            return False

        state = await self._load_checkup_state()
        last_checkup_at = state.get("last_checkup_at", 0.0)
        now = time.time()

        # 冷却期内绝不触发 (防风暴)
        if last_checkup_at and (now - last_checkup_at) < cooldown_seconds:
            return False

        # 时间步长到期 -> 触发 (不再受规则数膨胀影响)
        if (now - last_checkup_at) > checkup_interval_seconds:
            return True

        return False

    async def mark_checkup_attempt(self, result: str):
        """
        登记一次体检尝试 (无论成功失败都要登记)。
        关键: 在体检发起的第一时间调用, 使冷却期立即生效,
        防止体检超时后下一轮循环 (30s) 立即重试。
        """
        state = {"last_checkup_at": time.time(), "last_checkup_result": result}
        await asyncio.to_thread(self._save_checkup_state_sync, state)

    def _load_checkup_state_sync(self) -> dict:
        try:
            with open(self.checkup_state_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            return {"last_checkup_at": 0.0, "last_checkup_result": None}
        except Exception as e:
            logger.warning(f"[RuleStore] 体检状态读取失败: {e}")
            return {"last_checkup_at": 0.0, "last_checkup_result": None}

    async def _load_checkup_state(self) -> dict:
        return await asyncio.to_thread(self._load_checkup_state_sync)

    def _save_checkup_state_sync(self, state: dict):
        try:
            self._atomic_write_json_sync(self.checkup_state_path, state)
        except Exception as e:
            logger.warning(f"[RuleStore] 体检状态保存失败: {e}")

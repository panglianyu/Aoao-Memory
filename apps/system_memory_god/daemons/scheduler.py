import os
import json
import time
import asyncio
from datetime import datetime

from kernel.logger import get_logger
from kernel.message import Message

logger = get_logger(__name__)


class SchedulerDaemon:
    """日程守护进程。管理提醒任务，基于 asyncio 事件循环，不阻塞主线程。
    支持一次性提醒和周期性提醒（如生日每年重复）。"""

    def __init__(self, data_dir: str, bus=None):
        self.data_dir = data_dir
        self.file_path = os.path.join(data_dir, "reminders.json")
        self.bus = bus
        self._reminders = []
        self._task = None
        self._ensure_file()

    def _ensure_file(self):
        if not os.path.exists(self.file_path):
            with open(self.file_path, 'w', encoding='utf-8') as f:
                json.dump([], f, ensure_ascii=False, indent=4)
        self._load()

    def _load(self):
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                self._reminders = json.load(f)
        except Exception as e:
            logger.warning(f"[Scheduler] 加载提醒文件失败: {e}")
            self._reminders = []

    def _save(self):
        with open(self.file_path, 'w', encoding='utf-8') as f:
            json.dump(self._reminders, f, ensure_ascii=False, indent=4)

    @property
    def task(self):
        """[0.3.26.0 Phase 0 M0-09] 统一公开状态属性: daemon_status 等外部读取
        一律用 daemon.task (与 reflection/gc 对齐), 不再访问私有 _task。"""
        return self._task

    def start(self):
        self._task = asyncio.create_task(self._scheduler_loop())

    def stop(self):
        if self._task:
            self._task.cancel()

    def add_reminder(self, remind_time: str, message: str, recurring: str = "none") -> dict:
        """添加提醒。
        remind_time: "YYYY-MM-DD HH:MM" 或 "MM-DD HH:MM"（当年）
        message: 提醒内容
        recurring: "none" | "daily" | "weekly" | "monthly" | "yearly"
        """
        reminder = {
            "id": int(time.time() * 1000),
            "time": remind_time,
            "message": message,
            "recurring": recurring,
            "active": True,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "fired_count": 0,
        }
        self._reminders.append(reminder)
        self._save()
        return reminder

    def remove_reminder(self, reminder_id: int) -> bool:
        self._load()
        before = len(self._reminders)
        self._reminders = [r for r in self._reminders if r["id"] != reminder_id]
        self._save()
        return len(self._reminders) < before

    def list_reminders(self) -> str:
        active = [r for r in self._reminders if r.get("active", True)]
        if not active:
            return "当前没有待触发的提醒。"
        lines = []
        for r in active:
            lines.append(f"- [{r['time']}] {r['message']} (重复: {r['recurring']})")
        return "\n".join(lines)

    def _parse_reminder_time(self, time_str: str) -> datetime:
        """解析提醒时间字符串。支持：
        - "YYYY-MM-DD HH:MM"
        - "MM-DD HH:MM"（自动补当年）
        - "HH:MM"（今天）
        """
        now = datetime.now()
        try:
            if len(time_str) >= 16 and '-' in time_str[:10]:
                return datetime.strptime(time_str, "%Y-%m-%d %H:%M")
            elif len(time_str) >= 11 and '-' in time_str:
                return datetime.strptime(f"{now.year}-{time_str}", "%Y-%m-%d %H:%M")
            else:
                return datetime.strptime(f"{now.strftime('%Y-%m-%d')} {time_str}", "%Y-%m-%d %H:%M")
        except ValueError:
            raise ValueError(f"无法解析时间格式: {time_str}，请使用 YYYY-MM-DD HH:MM")

    def _advance_recurring(self, reminder: dict) -> bool:
        """计算周期性提醒的下一次触发时间。返回是否需要重新调度。"""
        recurring = reminder.get("recurring", "none")
        if recurring == "none":
            reminder["active"] = False
            self._save()
            return False

        try:
            last_fire = self._parse_reminder_time(reminder["time"])
        except ValueError:
            reminder["active"] = False
            return False

        from datetime import timedelta
        import calendar

        if recurring == "daily":
            next_time = last_fire
            while next_time <= datetime.now():
                next_time = next_time + timedelta(days=1)
            reminder["time"] = next_time.strftime("%Y-%m-%d %H:%M")
        elif recurring == "weekly":
            next_time = last_fire
            while next_time <= datetime.now():
                next_time = next_time + timedelta(weeks=1)
            reminder["time"] = next_time.strftime("%Y-%m-%d %H:%M")
        elif recurring == "monthly":
            month = last_fire.month + 1
            year = last_fire.year
            while True:
                if month > 12:
                    month = 1
                    year += 1
                try:
                    next_time = last_fire.replace(year=year, month=month)
                except ValueError:
                    last_day = calendar.monthrange(year, month)[1]
                    next_time = last_fire.replace(year=year, month=month, day=last_day)
                if next_time > datetime.now():
                    break
                month += 1
            reminder["time"] = next_time.strftime("%Y-%m-%d %H:%M")
        elif recurring == "yearly":
            next_time = last_fire.replace(year=last_fire.year + 1)
            while next_time <= datetime.now():
                next_time = next_time.replace(year=next_time.year + 1)
            reminder["time"] = next_time.strftime("%Y-%m-%d %H:%M")
        else:
            # R6: 未识别的 recurring 值 (如 "fortnightly") 穿透所有分支 -> 既不推进时间也不放任重复触发。
            # 旧实现落到末尾 return True 致 remind_at<=now 恒成立, 每 60s 重复发送风暴。改为停用并告警。
            logger.warning(f"[Scheduler] 未识别 recurring='{recurring}', 提醒已停用防风暴: {reminder.get('message')}")
            reminder["active"] = False
            self._save()
            return False

        self._save()
        return True

    async def _scheduler_loop(self):
        """主循环：每 60 秒检查一次待触发的提醒。"""
        while True:
            await asyncio.sleep(60)
            self._load()
            now = datetime.now()
            fired = False

            for reminder in self._reminders:
                if not reminder.get("active", True):
                    continue

                try:
                    remind_at = self._parse_reminder_time(reminder["time"])
                except ValueError:
                    reminder["active"] = False
                    continue

                # 时间到了
                if remind_at <= now:
                    reminder["fired_count"] = reminder.get("fired_count", 0) + 1
                    logger.info(f"[Scheduler 触发] {reminder['message']} (设定时间: {reminder['time']})")

                    # 通过 bus 发送提醒事件（pub/sub）
                    if self.bus:
                        try:
                            await self.bus.emit_msg(Message.reminder_fire(
                                content=reminder["message"],
                                source="system_scheduler",
                                time=reminder["time"],
                                reminder_id=reminder["id"],
                            ))
                            # 同时推送到主循环消息队列，触发阿露主动发消息
                            try:
                                self.bus.submit_input(
                                    Message.user_input(
                                        content=f"[系统提醒触发] {reminder['message']}",
                                        source="system_scheduler",
                                    )
                                )
                            except Exception:
                                logger.warning("[Scheduler] 提醒消息入队失败（队列已满）")
                        except Exception as e:
                            logger.error(f"[Scheduler] 发送提醒事件失败: {e}")

                    # 处理周期性 (R6: 包 try/except 防 _advance_recurring 抛异常击穿 while True 致 task 崩溃)
                    try:
                        self._advance_recurring(reminder)
                    except Exception as e:
                        logger.error(f"[Scheduler] _advance_recurring 异常, 停用该提醒防风暴: {e}")
                        reminder["active"] = False
                    fired = True

            if fired:
                self._save()

    def get_time_anchor_text(self) -> str:
        """生成时间锚点文本，注入系统 Prompt。"""
        now = datetime.now()
        weekday_map = {
            0: "星期一", 1: "星期二", 2: "星期三", 3: "星期四",
            4: "星期五", 5: "星期六", 6: "星期日"
        }
        weekday = weekday_map[now.weekday()]
        time_str = now.strftime(f"%Y年%m月%d日 {weekday} %H:%M:%S")

        lines = [f"[时间锚点] {time_str}"]

        # 计算距离最近的未来提醒
        active = [r for r in self._reminders if r.get("active", True)]
        future = []
        for r in active:
            try:
                remind_at = self._parse_reminder_time(r["time"])
                if remind_at > now:
                    future.append((remind_at, r))
            except ValueError:
                pass

        if future:
            future.sort(key=lambda x: x[0])
            nearest_dt, nearest = future[0]
            delta = nearest_dt - now
            if delta.days == 0:
                lines.append(f"[下一个提醒] {nearest['message']}，还有 {delta.seconds // 3600}小时{delta.seconds % 3600 // 60}分钟")
            elif delta.days == 1:
                lines.append(f"[下一个提醒] {nearest['message']}，明天")
            else:
                lines.append(f"[下一个提醒] {nearest['message']}，还有 {delta.days} 天")

        return "\n".join(lines)

# kernel/principal.py
"""统一权限协议 -- 调用主体 (0.3.14.0 P1/P2 简化版)。

架构审查报告 P1/P2 落地: 把散落在各渠道的 metadata["is_master"] 形式化为
Principal (owner/guest 两档)。原则:

- owner 身份只由渠道认证器签发 (各渠道感知层计算 is_master 落 metadata:
  wechat user_map 静态反查 / feishu 回声过滤恒老板 / console 本地当面 /
  web 回环中间件); 派生层不信消息内容文本, 防语义伪造。
- Principal 从 Message metadata 派生 (from_message), 不动 Message 结构
  (报告完整版正式字段留 P6)。
- 批次权限 (from_batch): 全 owner -> owner; 混入任一 guest -> 整组降档 guest。
  治报告严重项: 旧 any(is_master) 使老板与第三人同群同批时, 第三人内容
  继承主人能力 (可诱导高危工具)。strict=False 回退旧 any() (一键回滚)。
- role 用 str 不用 enum: 为 0.9x 弹幕第三方身份 (guest + 每弹幕用户
  actor_id) 与未来 verified 档留扩展口。
- actor_id 取稳定标识: 显式 metadata.actor_id 优先, sender 显示名兜底。
"""
from dataclasses import dataclass
from typing import Any, Iterable, Optional, Tuple

ROLE_OWNER = "owner"
ROLE_GUEST = "guest"

# 高危能力清单 (阶段2 闸门收敛逐个切换 require_owner; 阶段1 先定义锚点)。
# 语义对齐现有散落闸门: listen_scope=_handle_instruct_app(add/remove_listen_chat),
# app_lifecycle=connect_app/disconnect_app 高危清单, impression=adjust_impression,
# interest=boost_interest, mcp_admin=mcp_gateway 安装/管理命令双保险。
HIGH_RISK_CAPABILITIES = frozenset({
    "listen_scope",
    "app_lifecycle",
    "impression",
    "interest",
    "mcp_admin",
    "browser_control",
    "sandbox_exec",
    "project_files",
    "wechat_vm_admin",
})


@dataclass(frozen=True)
class Principal:
    """调用主体: 谁、经哪个渠道、什么身份档、凭什么认证触发本轮。

    frozen 不可变: 主体一经派生不得中途篡改 (对应报告"不可伪造调用主体"
    简化版 -- 不可变 + 渠道签发, 正式签名机制留 P6)。
    """

    actor_id: str
    role: str
    channel: str
    authenticated_by: str

    @property
    def is_owner(self) -> bool:
        return self.role == ROLE_OWNER

    # ==========================================
    # 工厂
    # ==========================================

    @staticmethod
    def owner(actor_id: str, channel: str, authenticated_by: str) -> "Principal":
        return Principal(
            actor_id=str(actor_id), role=ROLE_OWNER,
            channel=channel, authenticated_by=authenticated_by,
        )

    @staticmethod
    def guest(actor_id: str, channel: str, authenticated_by: str = "derived") -> "Principal":
        return Principal(
            actor_id=str(actor_id), role=ROLE_GUEST,
            channel=channel, authenticated_by=authenticated_by,
        )

    @staticmethod
    def system(actor_id: str = "ailu") -> "Principal":
        """内部主体 (idle/boot/daemon/空批): 恒 guest 档。

        阿露自作主张的高危操作本就无授权 (现有闸门 idle 默认 False 语义),
        system() 把该隐含约定显式化。
        """
        return Principal(
            actor_id=str(actor_id), role=ROLE_GUEST,
            channel="system", authenticated_by="kernel",
        )

    # ==========================================
    # 派生
    # ==========================================

    @staticmethod
    def from_message(msg: Any) -> "Principal":
        """从单条 Message 派生主体。

        is_master 由渠道认证器签发进 metadata; 缺失/None 一律 fail-safe 落
        guest (新增渠道忘签发不得默认获权)。actor_id: 显式声明 > sender >
        按档兜底 (owner 落 boss_<channel>, guest 落 anon_<channel>)。
        """
        md = getattr(msg, "metadata", None) or {}
        channel = getattr(msg, "source", "") or ""
        # metadata 签发为准; 属性式 is_master 兜底 (历史消息状夹具/老路径
        # duck-typing 兼容, 如 SimpleNamespace(is_master=True))。
        is_master = bool(md.get("is_master", False)) or bool(
            getattr(msg, "is_master", False)
        )

        actor = (
            md.get("actor_id")
            or md.get("sender")
            or (f"boss_{channel}" if is_master else f"anon_{channel}")
        )
        if md.get("authenticated_by"):
            authenticated_by = str(md["authenticated_by"])
        elif channel:
            authenticated_by = f"channel:{channel}"
        else:
            authenticated_by = "derived"

        if is_master:
            return Principal.owner(actor, channel, authenticated_by)
        return Principal.guest(actor, channel, authenticated_by)

    @staticmethod
    def from_batch(msgs: Iterable[Any], strict: bool = True) -> "Principal":
        """同 chat 组批次权限派生。

        strict=True (默认): 组内全部 owner -> owner; 混入任一 guest -> 整组
            降档 guest (actor_id="mixed")。修报告严重项: 混合身份批次不再
            继承主人权限, 第三人无法借老板同批在场诱导高危工具。
        strict=False: 回退旧 any() 行为 (config/brain.yaml permission 段
            strict_batch_permission=false 一键回滚)。

        空批 -> guest 内部主体 (system 语义, 防空调用获权)。
        """
        principals = [Principal.from_message(m) for m in msgs]
        if not principals:
            return Principal.system("empty_batch")

        channel = principals[0].channel
        if strict:
            if all(p.is_owner for p in principals):
                return Principal.owner(
                    principals[0].actor_id, channel, "batch:all_owner",
                )
            actor = principals[0].actor_id if len(principals) == 1 else "mixed"
            return Principal.guest(actor, channel, "batch:strict")

        if any(p.is_owner for p in principals):
            return Principal.owner(
                principals[0].actor_id, channel, "batch:any_legacy",
            )
        return Principal.guest(principals[0].actor_id, channel, "batch:any_legacy")


def require_owner(principal: Optional[Principal], capability: str) -> Tuple[bool, str]:
    """统一权限闸门: 高危能力须 owner, 非高危直通。

    返回 (放行, 原因); 拒绝时原因可直接作工具回执文案。主体为 None
    (调用链未透传) 对高危能力按 guest 拒 (fail-safe)。

    阶段1 提供 helper; 阶段2 现有散落闸门 (查 _current_is_master 的四处)
    逐个收敛到本入口, 行为零变化。
    """
    if capability not in HIGH_RISK_CAPABILITIES:
        return True, ""
    if principal is None:
        return False, f"[权限闸门] {capability} 需要主人身份, 当前主体未知, 拒绝"
    if principal.is_owner:
        return True, ""
    return False, (
        f"[权限闸门] {capability} 需要主人身份, "
        f"当前={principal.role}/{principal.channel}/{principal.actor_id}, 拒绝"
    )

"""炭窑焖烧志业务规则。"""

from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from charclamp.domain.models import BurnShift, Clamp

MIN_PEAK_TEMP_FOR_DRAWN = 400.0

# 可执行出炭的角色：主管 / 管理员。操作工(worker)禁止出炭。
DRAW_ALLOWED_ROLES = {"admin", "supervisor", "manager", "foreman"}


class RuleError(ValueError):
    """业务规则校验失败。"""


def latest_shift_for_clamp(clamp: Clamp) -> BurnShift | None:
    if not clamp.shifts:
        return None
    # 开始时间相同则以班次 id 决胜，保证「最近一班」唯一。
    return max(clamp.shifts, key=lambda s: (s.started_at, s.id))


def can_mark_clamp_drawn(clamp: Clamp) -> tuple[bool, str]:
    """
    炭窑转为「已出炭」(drawn) 的前提：
    最近一条焖烧班次的峰值温度已记录，且 >= 400℃。
    """
    latest = latest_shift_for_clamp(clamp)
    if latest is None:
        return False, "该窑尚无焖烧班次，不能标记为已出炭"
    if latest.peak_temp_c is None:
        return False, "最近班次尚未记录峰值温度，不能标记为已出炭"
    if latest.peak_temp_c < MIN_PEAK_TEMP_FOR_DRAWN:
        return (
            False,
            f"最近班次峰值温度 {latest.peak_temp_c}℃ 低于 {MIN_PEAK_TEMP_FOR_DRAWN:.0f}℃，不能标记为已出炭",
        )
    return True, ""


def can_role_draw(role: str | None) -> bool:
    return (role or "") in DRAW_ALLOWED_ROLES


def assert_can_set_clamp_status(clamp: Clamp, new_status: str) -> None:
    allowed = {Clamp.STATUS_STACKED, Clamp.STATUS_BURNING, Clamp.STATUS_DRAWN}
    if new_status not in allowed:
        raise RuleError(f"无效状态：{new_status}")
    if new_status == Clamp.STATUS_DRAWN:
        ok, msg = can_mark_clamp_drawn(clamp)
        if not ok:
            raise RuleError(msg)


async def mark_clamp_drawn(db: AsyncSession, clamp_id: int) -> tuple[Clamp, BurnShift]:
    """
    在同一事务内把窑标记为「已出炭」，并给该窑最近一班写入收火时刻。

    - 行级锁串行化同一窑的并发出炭：两名主管抢标同一窑至多一笔成功，
      失败者会看到窑已是 drawn 而被拒绝，不会重复写收火时刻。
    - 无班次 / 峰值未记录 / 峰值 < 400℃ 时抛 RuleError：不修改窑态、
      不写收火时刻，由调用方整体回滚。
    - 收火时刻取数据库 now()，并以库内回读值为准（与库差为 0）。

    返回 (窑, 最近一班)；调用方负责 commit/rollback。
    """
    clamp = (
        await db.execute(select(Clamp).where(Clamp.id == clamp_id).with_for_update())
    ).scalar_one_or_none()
    if clamp is None:
        raise RuleError("炭窑不存在")

    if clamp.status == Clamp.STATUS_DRAWN:
        raise RuleError(f"窑 {clamp.code} 已是已出炭状态")

    # 锁定该窑全部班次：既固定「最近一班」的选择，也阻塞并发事务插入/改动班次。
    shifts = list(
        (
            await db.execute(
                select(BurnShift)
                .where(BurnShift.clamp_id == clamp_id)
                .with_for_update()
            )
        )
        .scalars()
        .all()
    )
    if not shifts:
        raise RuleError("该窑尚无焖烧班次，不能标记为已出炭")

    latest = max(shifts, key=lambda s: (s.started_at, s.id))
    if latest.peak_temp_c is None:
        raise RuleError("最近班次尚未记录峰值温度，不能标记为已出炭")
    if latest.peak_temp_c < MIN_PEAK_TEMP_FOR_DRAWN:
        raise RuleError(
            f"最近班次峰值温度 {latest.peak_temp_c}℃ 低于 {MIN_PEAK_TEMP_FOR_DRAWN:.0f}℃，不能标记为已出炭"
        )

    # 校验全部通过后才改窑态；与收火时刻同一事务提交，失败则整笔回滚。
    clamp.status = Clamp.STATUS_DRAWN
    # 取数据库时钟，Postgres/SQLite 均支持；Postgres 下为 timestamptz。
    latest.closed_at = func.current_timestamp()
    db.add_all([clamp, latest])
    await db.flush()
    # 回读数据库生成的收火时刻，确保返回值与库存值完全一致（与库差为 0）。
    await db.refresh(latest, ["closed_at"])
    return clamp, latest

"""炭窑出炭的事务性领域操作。

与 rules.py 中的纯规则判定不同，这里的服务在单个数据库事务内完成
「锁窑 → 校验最近班次峰值 → 写收火时刻 → 翻窑态为已出炭」，
任一步失败都由调用方整体回滚，保证窑态不会先于收火时刻落库。
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from charclamp.domain.models import BurnShift, Clamp, utcnow
from charclamp.domain.rules import MIN_PEAK_TEMP_FOR_DRAWN, RuleError


class ClampNotFound(LookupError):
    """目标炭窑不存在。"""


async def _latest_shift_locked(db: AsyncSession, clamp_id: int) -> list[BurnShift]:
    """锁定该窑全部班次行（FOR UPDATE），供并发下取最近一班。"""
    result = await db.execute(
        select(BurnShift)
        .where(BurnShift.clamp_id == clamp_id)
        .with_for_update()
    )
    return list(result.scalars().all())


async def mark_clamp_drawn(
    db: AsyncSession, clamp_id: int
) -> tuple[Clamp, BurnShift, datetime]:
    """把一口窑原子地标记为「已出炭」。

    顺序固定，且全部发生在同一事务、同一批行锁内：

    1. ``SELECT ... FOR UPDATE`` 锁住窑行——两名主管抢标同一窑时，
       后到者阻塞，待先到者提交后看到窑已是 drawn 而被拒，至多一笔入库。
    2. 锁该窑班次并取最近一班；无班次 / 峰值未记录 / 峰值 < 400℃ 一律
       抛 :class:`RuleError`，此时窑态尚未被改动，调用方回滚即可。
    3. 校验通过后才在同一事务内写 ``fire_closed_at`` 并把窑态置为 drawn，
       收火时刻只跟成功提交的那一笔走。

    本函数只 ``flush`` 不 ``commit``，提交/回滚由调用方掌控。
    返回 ``(窑, 最近班次, 收火时刻)``。
    """
    clamp_result = await db.execute(
        select(Clamp).where(Clamp.id == clamp_id).with_for_update()
    )
    clamp = clamp_result.scalar_one_or_none()
    if clamp is None:
        raise ClampNotFound(f"炭窑 {clamp_id} 不存在")

    if clamp.status == Clamp.STATUS_DRAWN:
        raise RuleError("该窑已出炭，不能重复标记")

    shifts = await _latest_shift_locked(db, clamp_id)
    if not shifts:
        raise RuleError("该窑尚无焖烧班次，不能标记为已出炭")

    latest = max(shifts, key=lambda s: s.started_at)
    if latest.peak_temp_c is None:
        raise RuleError("最近班次尚未记录峰值温度，不能标记为已出炭")
    if latest.peak_temp_c < MIN_PEAK_TEMP_FOR_DRAWN:
        raise RuleError(
            f"最近班次峰值温度 {latest.peak_temp_c}℃ 低于 "
            f"{MIN_PEAK_TEMP_FOR_DRAWN:.0f}℃，不能标记为已出炭"
        )

    # 校验全部通过后才落笔：收火时刻与窑态在同一事务写入，要么都成功要么都回滚。
    closed_at = utcnow()
    latest.fire_closed_at = closed_at
    clamp.status = Clamp.STATUS_DRAWN
    await db.flush()
    return clamp, latest, closed_at

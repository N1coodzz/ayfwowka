from __future__ import annotations

from math import inf

from models import MatchState, MarketLine, Candidate, Decision


MIN_EDGE = 0.05
MIN_PROBABILITY = 0.55
MIN_ODDS = 1.65


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _is_equal_game(state: MatchState) -> bool:
    return state.kill_diff <= 3 and state.networth_diff_k <= 2.0


def _is_stomp(state: MatchState) -> bool:
    if state.time_minutes >= 12 and state.kill_diff >= 8 and state.networth_diff_k >= 3.0:
        return True
    if state.time_minutes >= 14 and state.kill_diff >= 7 and state.networth_diff_k >= 4.0:
        return True
    return False


def _is_low_start(state: MatchState) -> bool:
    if state.time_minutes <= 8 and state.total_kills_now <= 4:
        return True
    if state.time_minutes <= 6 and state.total_kills_now <= 3:
        return True
    return False


def _is_active_equal_start(state: MatchState) -> bool:
    if 6 <= state.time_minutes <= 10 and state.total_kills_now >= 8 and _is_equal_game(state):
        return True
    return False


def estimate_expected_total(state: MatchState) -> tuple[float, int, int, str]:
    """
    Rule-based прогноз тотала.

    Это не ML и не "истина", а стартовая модель для MVP.
    Потом её нужно калибровать по CSV-истории.
    """
    t = max(state.time_minutes, 1.0)
    kills = state.total_kills_now
    kpm_now = kills / t

    equal_game = _is_equal_game(state)
    stomp = _is_stomp(state)
    low_start = _is_low_start(state)
    active_equal = _is_active_equal_start(state)

    # Базовая ожидаемая длительность.
    if t <= 8:
        expected_duration = 39.0
    elif t <= 13:
        expected_duration = 38.0
    elif t <= 16:
        expected_duration = 36.0
    else:
        expected_duration = 34.0

    if equal_game:
        expected_duration += 2.0

    if stomp:
        expected_duration = 31.0 if t < 16 else 29.0

    # Базовый будущий темп киллов/мин.
    if t <= 8:
        # Early Dota часто "догоняет" по киллам после тихих первых минут,
        # поэтому нельзя тупо умножать текущий KPM на всю карту.
        future_kpm = 1.48
    elif t <= 13:
        future_kpm = 1.36
    elif t <= 16:
        future_kpm = 1.25
    else:
        future_kpm = 1.12

    # Смешиваем текущий темп и базовый будущий темп.
    # В раннем окне текущий KPM очень шумный, поэтому его вес небольшой.
    if t <= 8:
        current_weight = 0.10 if kills <= 4 else 0.25
        future_kpm = current_weight * kpm_now + (1 - current_weight) * future_kpm
    elif t <= 13:
        future_kpm = 0.40 * kpm_now + 0.60 * future_kpm
    else:
        future_kpm = 0.55 * kpm_now + 0.45 * future_kpm

    reason_parts = []

    if low_start:
        # После низового старта темп может ускориться, но линия 53–56 часто остаётся завышенной.
        future_kpm -= 0.03
        reason_parts.append("низовой старт")

    if active_equal:
        future_kpm += 0.10
        expected_duration += 1.5
        reason_parts.append("активная равная карта")

    if equal_game and not active_equal:
        future_kpm += 0.03
        reason_parts.append("равная карта")

    if stomp:
        future_kpm -= 0.16
        reason_parts.append("stomp-риск")

    # Защита от абсурда.
    future_kpm = _clamp(future_kpm, 0.85, 1.75)

    remaining = max(expected_duration - t, 0)
    expected_mid = kills + future_kpm * remaining

    # Сценарные поправки.
    if low_start:
        expected_mid -= 1.5
    if active_equal:
        expected_mid += 2.0
    if stomp:
        expected_mid -= 5.0

    # Диапазон шире в early, уже ближе к midgame.
    if t <= 8:
        spread = 2 if low_start else 4
    elif t <= 13:
        spread = 3
    else:
        spread = 2

    expected_low = int(round(expected_mid - spread))
    expected_high = int(round(expected_mid + spread))

    reason = ", ".join(reason_parts) if reason_parts else "обычный сценарий"
    return expected_mid, expected_low, expected_high, reason


def _probability_for_side(state: MatchState, line: float, side: str, expected_mid: float) -> float:
    """
    Перевод разницы между модельным тоталом и линией в вероятность.

    side:
    - under: выигрывает, если фактический тотал ниже линии
    - over: выигрывает, если фактический тотал выше линии
    """
    diff = line - expected_mid

    if side == "under":
        # Если линия выше нашего прогноза, Under вероятнее.
        prob = 0.50 + diff * 0.015
    else:
        # Если наш прогноз выше линии, Over вероятнее.
        prob = 0.50 - diff * 0.015

    equal_game = _is_equal_game(state)
    low_start = _is_low_start(state)
    active_equal = _is_active_equal_start(state)
    stomp = _is_stomp(state)

    if side == "under" and low_start and line >= 52.5:
        prob += 0.02

    if side == "over" and active_equal and line <= expected_mid + 2:
        prob += 0.03

    # В stomp-сценарии высокий овер опасен: карта может закрыться до добора киллов.
    if side == "over" and stomp and state.time_minutes >= 14 and line >= 64.5:
        prob -= 0.08

    if side == "under" and stomp and state.time_minutes >= 14 and line >= 64.5:
        prob += 0.04

    # Если линия почти совпадает с моделью, букмекер уже близко поймал карту.
    if abs(diff) < 1.75:
        prob = min(prob, 0.54)

    # После 16 минуты высокий over нужен только с сильным подтверждением.
    if side == "over" and state.time_minutes >= 16 and line >= 64.5 and not active_equal:
        prob -= 0.04

    return _clamp(prob, 0.30, 0.66)


def _make_candidate(state: MatchState, market: MarketLine, side: str, expected_mid: float) -> Candidate | None:
    odds = market.under_odds if side == "under" else market.over_odds
    if not odds:
        return None

    probability = _probability_for_side(state, market.line, side, expected_mid)
    fair_odds = 1 / probability if probability > 0 else inf
    edge = probability * odds - 1

    return Candidate(
        side=side,
        side_ru="ТМ" if side == "under" else "ТБ",
        line=market.line,
        odds=odds,
        probability=probability,
        fair_odds=fair_odds,
        edge=edge,
    )


def _candidate_allowed(state: MatchState, candidate: Candidate, expected_mid: float) -> bool:
    if candidate.edge < MIN_EDGE:
        return False
    if candidate.probability < MIN_PROBABILITY:
        return False
    if candidate.odds < MIN_ODDS:
        return False

    # Явный запрет на высокий овер при stomp-over риске.
    if (
        candidate.side == "over"
        and _is_stomp(state)
        and state.time_minutes >= 14
        and candidate.line >= 64.5
    ):
        return False

    return True


def analyze_state(state: MatchState) -> Decision:
    expected_mid, expected_low, expected_high, reason = estimate_expected_total(state)

    candidates: list[Candidate] = []

    for market in state.lines:
        for side in ("under", "over"):
            candidate = _make_candidate(state, market, side, expected_mid)
            if candidate:
                candidates.append(candidate)

    allowed = [c for c in candidates if _candidate_allowed(state, c, expected_mid)]

    if not allowed:
        return Decision(
            decision="пас",
            best=None,
            expected_low=expected_low,
            expected_high=expected_high,
            reason=f"нет value по фильтрам; сценарий: {reason}",
        )

    # Выбираем лучший вариант по edge, но с небольшим приоритетом вероятности.
    best = sorted(allowed, key=lambda c: (c.edge, c.probability), reverse=True)[0]

    return Decision(
        decision="брать",
        best=best,
        expected_low=expected_low,
        expected_high=expected_high,
        reason=reason,
    )


def format_signal(state: MatchState, decision: Decision) -> str:
    lines = [
        f"Матч: {state.match}",
        f"Время: {state.time_str or '-'}",
        f"Счёт: {state.score_text}",
    ]

    if decision.best is None:
        lines.extend(
            [
                "Лучший валуй: нет",
                f"Мой тотал: {decision.expected_low}–{decision.expected_high} килла",
                "Решение: пас",
            ]
        )
        return "\n".join(lines)

    b = decision.best
    edge_pct = round(b.edge * 100)
    prob_pct = round(b.probability * 100)

    lines.extend(
        [
            f"Лучший валуй: {b.side_ru} {b.line:.1f} @ {b.odds:.2f}",
            f"Вероятность: {prob_pct}%",
            f"Честный кф: {b.fair_odds:.2f}",
            f"Edge: {edge_pct:+d}%",
            f"Мой тотал: {decision.expected_low}–{decision.expected_high} килла",
            "Решение: брать",
        ]
    )
    return "\n".join(lines)

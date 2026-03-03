"""
Анализатор плотностей + детектор переставляшей (Фаза 2)
"""
import statistics
import time
from dataclasses import dataclass, field
from typing import Optional
import config


# ═══════════════════════════════════════════════════
# Структуры данных
# ═══════════════════════════════════════════════════

@dataclass
class WallInfo:
    """Информация о плотности (стенке)"""
    side: str
    price: float
    size_usdt: float
    multiplier: float
    distance_pct: float
    levels_count: int = 1


@dataclass
class MoverEvent:
    """Событие переставляша — плотность переместилась"""
    symbol: str
    side: str
    old_price: float
    new_price: float
    size_usdt: float
    shift_pct: float
    timestamp: float
    direction: str  # "UP" или "DOWN"


@dataclass
class ScanResult:
    """Результат сканирования одной пары"""
    symbol: str
    mid_price: float
    best_bid: float
    best_ask: float
    spread_pct: float
    volume_24h_usdt: float
    bid_walls: list[WallInfo] = field(default_factory=list)
    ask_walls: list[WallInfo] = field(default_factory=list)
    total_bid_depth_usdt: float = 0.0
    total_ask_depth_usdt: float = 0.0
    score: float = 0.0
    mover_events: list[MoverEvent] = field(default_factory=list)
    timestamp: float = 0.0
    trade_count_24h: int = 0

    @property
    def all_walls(self) -> list[WallInfo]:
        return self.bid_walls + self.ask_walls

    @property
    def biggest_wall(self) -> Optional[WallInfo]:
        walls = self.all_walls
        return max(walls, key=lambda w: w.size_usdt) if walls else None

    @property
    def wall_count(self) -> int:
        return len(self.all_walls)

    @property
    def has_movers(self) -> bool:
        return len(self.mover_events) > 0


def _safe_float(val, default=0.0) -> float:
    """Безопасная конвертация в float — MEXC может вернуть '', None, и т.д."""
    if val is None or val == "":
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


# ═══════════════════════════════════════════════════
# Анализ стакана
# ═══════════════════════════════════════════════════

def analyze_order_book(
    symbol: str,
    order_book: dict,
    ticker_data: dict,
) -> Optional[ScanResult]:
    """Анализирует стакан на наличие плотностей"""
    bids_raw = order_book.get("bids", [])
    asks_raw = order_book.get("asks", [])

    if not bids_raw or not asks_raw:
        return None

    try:
        bids = [(_safe_float(b[0]), _safe_float(b[1])) for b in bids_raw]
        asks = [(_safe_float(a[0]), _safe_float(a[1])) for a in asks_raw]
    except (IndexError, KeyError):
        return None

    # Убираем нулевые цены
    bids = [(p, q) for p, q in bids if p > 0]
    asks = [(p, q) for p, q in asks if p > 0]

    if not bids or not asks:
        return None

    best_bid_price = bids[0][0]
    best_ask_price = asks[0][0]
    mid_price = (best_bid_price + best_ask_price) / 2

    if mid_price <= 0:
        return None

    spread_pct = (best_ask_price - best_bid_price) / best_bid_price * 100
    volume_24h = _safe_float(ticker_data.get("quoteVolume", 0))

    bid_levels_usdt = [(p, q * p) for p, q in bids]
    ask_levels_usdt = [(p, q * p) for p, q in asks]

    total_bid_depth = sum(u for _, u in bid_levels_usdt)
    total_ask_depth = sum(u for _, u in ask_levels_usdt)

    all_sizes = [u for _, u in bid_levels_usdt + ask_levels_usdt if u > 0]
    if len(all_sizes) < 5:
        return None

    median_size = statistics.median(all_sizes)
    if median_size <= 0:
        median_size = 1.0

    bid_walls = _find_walls(bid_levels_usdt, "BID", mid_price, median_size)
    ask_walls = _find_walls(ask_levels_usdt, "ASK", mid_price, median_size)

    if not bid_walls and not ask_walls:
        return None

    result = ScanResult(
        symbol=symbol,
        mid_price=mid_price,
        best_bid=best_bid_price,
        best_ask=best_ask_price,
        spread_pct=spread_pct,
        volume_24h_usdt=volume_24h,
        bid_walls=bid_walls,
        ask_walls=ask_walls,
        total_bid_depth_usdt=total_bid_depth,
        total_ask_depth_usdt=total_ask_depth,
        timestamp=time.time(),
    )
    result.score = _calculate_score(result)
    return result


def _find_walls(levels, side, mid_price, median_size) -> list[WallInfo]:
    walls = []
    for price, size_usdt in levels:
        if size_usdt < config.MIN_WALL_SIZE_USDT:
            continue
        multiplier = size_usdt / median_size
        if multiplier < config.WALL_MULTIPLIER:
            continue
        distance_pct = abs(price - mid_price) / mid_price * 100
        if distance_pct > config.MAX_WALL_DISTANCE_PCT:
            continue
        walls.append(WallInfo(
            side=side, price=price, size_usdt=size_usdt,
            multiplier=round(multiplier, 1),
            distance_pct=round(distance_pct, 2),
        ))

    walls = _merge_adjacent_walls(walls, [p for p, _ in levels])
    walls.sort(key=lambda w: w.size_usdt, reverse=True)
    return walls[:5]


def _merge_adjacent_walls(walls, prices) -> list[WallInfo]:
    if len(walls) <= 1:
        return walls
    merged = []
    used = set()
    for i, wall in enumerate(walls):
        if i in used:
            continue
        cluster = [wall]
        used.add(i)
        try:
            idx = prices.index(wall.price)
        except ValueError:
            merged.append(wall)
            continue
        for j, other in enumerate(walls):
            if j in used:
                continue
            try:
                oidx = prices.index(other.price)
            except ValueError:
                continue
            if abs(idx - oidx) <= 3:
                cluster.append(other)
                used.add(j)
        if len(cluster) > 1:
            total = sum(w.size_usdt for w in cluster)
            avg_m = sum(w.multiplier for w in cluster) / len(cluster)
            center = min(cluster, key=lambda w: w.distance_pct)
            merged.append(WallInfo(
                side=center.side, price=center.price,
                size_usdt=total, multiplier=round(avg_m, 1),
                distance_pct=center.distance_pct,
                levels_count=len(cluster),
            ))
        else:
            merged.append(wall)
    return merged


def _calculate_score(result: ScanResult) -> float:
    score = 0.0
    biggest = result.biggest_wall
    if biggest:
        score += min(biggest.size_usdt / 50, 30)
    score += min(max(result.spread_pct - 0.5, 0) * 5, 25)
    if biggest:
        score += max(20 - biggest.distance_pct * 2, 0)
    if result.bid_walls and result.ask_walls:
        score += 15
    if result.volume_24h_usdt > 0:
        score += min(result.volume_24h_usdt / 10000, 10)
    if result.has_movers:
        score += 10
    return round(score, 1)


# ═══════════════════════════════════════════════════
# Детектор переставляшей (Фаза 2)
# ═══════════════════════════════════════════════════

def detect_movers(
    current: ScanResult,
    previous: ScanResult,
) -> list[MoverEvent]:
    """
    Сравнивает два снимка одной пары и ищет переставляшей.
    Переставляш = крупная заявка исчезла с одного уровня и появилась
    на другом (с похожим объёмом).
    """
    events = []
    tolerance = config.MOVER_SIZE_TOLERANCE

    # Сравниваем BID-стенки
    events += _compare_walls(
        current.symbol,
        prev_walls=previous.bid_walls,
        curr_walls=current.bid_walls,
        side="BID",
        mid_price=current.mid_price,
        tolerance=tolerance,
        timestamp=current.timestamp,
    )

    # Сравниваем ASK-стенки
    events += _compare_walls(
        current.symbol,
        prev_walls=previous.ask_walls,
        curr_walls=current.ask_walls,
        side="ASK",
        mid_price=current.mid_price,
        tolerance=tolerance,
        timestamp=current.timestamp,
    )

    return events


def _compare_walls(
    symbol, prev_walls, curr_walls, side, mid_price, tolerance, timestamp
) -> list[MoverEvent]:
    events = []
    curr_prices = {w.price for w in curr_walls}
    prev_prices = {w.price for w in prev_walls}

    # Стенки которые исчезли
    disappeared = [w for w in prev_walls if w.price not in curr_prices]
    # Стенки которые появились
    appeared = [w for w in curr_walls if w.price not in prev_prices]

    for old_w in disappeared:
        # Ищем среди появившихся похожую по объёму
        for new_w in appeared:
            size_diff = abs(old_w.size_usdt - new_w.size_usdt) / max(
                old_w.size_usdt, 1
            )
            if size_diff > tolerance:
                continue

            price_shift = (new_w.price - old_w.price) / old_w.price * 100
            if abs(price_shift) < config.MOVER_MIN_PRICE_SHIFT:
                continue

            events.append(MoverEvent(
                symbol=symbol,
                side=side,
                old_price=old_w.price,
                new_price=new_w.price,
                size_usdt=new_w.size_usdt,
                shift_pct=round(price_shift, 3),
                timestamp=timestamp,
                direction="UP" if price_shift > 0 else "DOWN",
            ))
            break  # Одна старая стенка → одно событие

    return events

"""
Хранилище снимков + трекинг времени жизни плотностей v4.0
"""
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from analyzer import ScanResult, MoverEvent, WallInfo, detect_movers
import config


@dataclass
class TrackedWall:
    """Плотность с трекингом времени жизни"""
    side: str
    price: float
    size_usdt: float
    multiplier: float
    distance_pct: float
    first_seen: float = 0.0     # timestamp первого обнаружения
    last_seen: float = 0.0      # timestamp последнего обнаружения
    seen_count: int = 0         # сколько сканов подряд видна

    @property
    def lifetime_sec(self) -> float:
        if self.first_seen <= 0: return 0
        return self.last_seen - self.first_seen

    @property
    def lifetime_str(self) -> str:
        s = self.lifetime_sec
        if s < 60: return f"{s:.0f}с"
        if s < 3600: return f"{s/60:.0f}м"
        return f"{s/3600:.1f}ч"


@dataclass
class SymbolHistory:
    snapshots: deque = field(default_factory=lambda: deque(maxlen=config.MAX_SNAPSHOTS_PER_PAIR))
    mover_events: list[MoverEvent] = field(default_factory=list)
    tracked_walls: dict = field(default_factory=dict)  # key=f"{side}_{price}" -> TrackedWall
    first_seen: float = 0.0
    last_seen: float = 0.0
    total_scans: int = 0

    @property
    def mover_count(self) -> int:
        return len(self.mover_events)


class DensityTracker:
    def __init__(self):
        self.histories: dict[str, SymbolHistory] = defaultdict(SymbolHistory)
        self.all_mover_events: list[MoverEvent] = []
        self.scan_count = 0
        self.last_scan_time = 0.0

    def update(self, results: list[ScanResult]) -> list[MoverEvent]:
        self.scan_count += 1
        self.last_scan_time = time.time()
        new_events = []
        now = time.time()

        for result in results:
            sym = result.symbol
            hist = self.histories[sym]

            # Переставки
            if hist.snapshots:
                prev = hist.snapshots[-1]
                events = detect_movers(result, prev)
                if events:
                    result.mover_events = events
                    hist.mover_events.extend(events)
                    new_events.extend(events)
                    if len(hist.mover_events) > 200:
                        hist.mover_events = hist.mover_events[-200:]

            # Трекинг стенок — обновить время жизни
            current_keys = set()
            for w in result.all_walls:
                key = f"{w.side}_{w.price:.10f}"
                current_keys.add(key)
                if key in hist.tracked_walls:
                    tw = hist.tracked_walls[key]
                    tw.last_seen = now
                    tw.seen_count += 1
                    tw.size_usdt = w.size_usdt
                    tw.multiplier = w.multiplier
                    tw.distance_pct = w.distance_pct
                else:
                    hist.tracked_walls[key] = TrackedWall(
                        side=w.side, price=w.price, size_usdt=w.size_usdt,
                        multiplier=w.multiplier, distance_pct=w.distance_pct,
                        first_seen=now, last_seen=now, seen_count=1)

            # Убрать стенки не виденные > 5 мин
            stale = [k for k, tw in hist.tracked_walls.items()
                     if k not in current_keys and now - tw.last_seen > 300]
            for k in stale:
                del hist.tracked_walls[k]

            hist.snapshots.append(result)
            hist.total_scans += 1
            hist.last_seen = result.timestamp
            if hist.first_seen == 0: hist.first_seen = result.timestamp

        self.all_mover_events.extend(new_events)
        if len(self.all_mover_events) > 500:
            self.all_mover_events = self.all_mover_events[-500:]
        return new_events

    def get_tracked_walls(self, symbol: str) -> list[TrackedWall]:
        hist = self.histories.get(symbol)
        if not hist: return []
        return sorted(hist.tracked_walls.values(), key=lambda w: w.size_usdt, reverse=True)

    def get_symbol_history(self, symbol: str) -> SymbolHistory:
        return self.histories.get(symbol, SymbolHistory())

    def get_active_movers(self, window_sec: int = 3600) -> list[MoverEvent]:
        cutoff = time.time() - window_sec
        return [e for e in self.all_mover_events if e.timestamp >= cutoff]

    def get_symbol_movers(self, symbol: str) -> list[MoverEvent]:
        hist = self.histories.get(symbol)
        return hist.mover_events if hist else []

    def get_top_movers(self, n: int = 20) -> list[tuple[str, int]]:
        counts = {}
        for sym, hist in self.histories.items():
            if hist.mover_count > 0: counts[sym] = hist.mover_count
        return sorted(counts.items(), key=lambda x: x[1], reverse=True)[:n]

    def get_stats(self) -> dict:
        return {
            "total_pairs_tracked": len(self.histories),
            "total_scans": self.scan_count,
            "total_mover_events": len(self.all_mover_events),
            "pairs_with_movers": sum(1 for h in self.histories.values() if h.mover_count > 0),
        }

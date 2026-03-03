#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════
  MEXC WebSocket Monitor v2.0
  Реалтайм мониторинг стаканов отобранных пар

  Запускается отдельно на VPS, работает 24/7.
  Сначала сканирует REST → отбирает лучших кандидатов →
  подписывается через WebSocket на обновления стаканов →
  детектит переставляшей и новые плотности.
═══════════════════════════════════════════════════════════

Использование:
    python ws_monitor.py                  # Скан + мониторинг топ-25 пар
    python ws_monitor.py --pairs 10       # Мониторить топ-10
    python ws_monitor.py --symbols XYZUSDT,ABCUSDT  # Конкретные пары
"""

import asyncio
import json
import time
import argparse
import signal
from datetime import datetime
from collections import defaultdict

import websockets
import aiohttp

import config
from mexc_client import MexcClientAsync
from analyzer import (
    analyze_order_book, ScanResult, WallInfo, MoverEvent, detect_movers
)


# ═══════════════════════════════════════════════════
# WebSocket менеджер
# ═══════════════════════════════════════════════════

class WsOrderBook:
    """Локальный стакан, обновляемый через WebSocket"""
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.bids: dict[str, str] = {}  # price → qty
        self.asks: dict[str, str] = {}
        self.last_update = 0.0
        self.initialized = False

    def apply_snapshot(self, bids: list, asks: list):
        self.bids = {b[0]: b[1] for b in bids}
        self.asks = {a[0]: a[1] for a in asks}
        self.initialized = True
        self.last_update = time.time()

    def apply_update(self, bids: list, asks: list):
        for price, qty in bids:
            if float(qty) == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty
        for price, qty in asks:
            if float(qty) == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty
        self.last_update = time.time()

    def to_depth_dict(self) -> dict:
        """Конвертирует в формат совместимый с analyzer.py"""
        sorted_bids = sorted(
            self.bids.items(), key=lambda x: float(x[0]), reverse=True
        )[:config.ORDER_BOOK_DEPTH]
        sorted_asks = sorted(
            self.asks.items(), key=lambda x: float(x[0])
        )[:config.ORDER_BOOK_DEPTH]
        return {
            "bids": [[p, q] for p, q in sorted_bids],
            "asks": [[p, q] for p, q in sorted_asks],
        }


class MexcWsMonitor:
    """Менеджер WebSocket-подключений к MEXC"""

    def __init__(self, on_event_callback=None):
        self.ws_url = config.MEXC_WS_URL
        self.order_books: dict[str, WsOrderBook] = {}
        self.prev_results: dict[str, ScanResult] = {}
        self.ticker_cache: dict[str, dict] = {}
        self.on_event = on_event_callback or self._default_callback
        self._running = False
        self._ws = None
        self.stats = {
            "messages": 0,
            "movers_detected": 0,
            "new_walls_detected": 0,
            "start_time": 0,
        }

    async def start(self, symbols: list[str]):
        """Запуск мониторинга выбранных пар"""
        self._running = True
        self.stats["start_time"] = time.time()

        # Инициализируем книги заявок
        for sym in symbols:
            self.order_books[sym] = WsOrderBook(sym)

        print(f"\n  📡 Подключаюсь к WebSocket ({len(symbols)} пар)...")

        while self._running:
            try:
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    max_size=10 * 1024 * 1024,
                ) as ws:
                    self._ws = ws
                    print("  ✓ WebSocket подключён")

                    # Подписываемся на стаканы (лимитная глубина 20 уровней)
                    for sym in symbols:
                        sub = {
                            "method": "SUBSCRIPTION",
                            "params": [
                                f"spot@public.limit.depth.v3.api.pb@{sym}@20"
                            ],
                        }
                        await ws.send(json.dumps(sub))
                        await asyncio.sleep(0.1)

                    print(f"  ✓ Подписка на {len(symbols)} пар оформлена")
                    print("  🔄 Слушаю обновления...\n")

                    # Получаем начальные снимки через REST
                    await self._init_snapshots(symbols)

                    # Слушаем обновления
                    async for msg in ws:
                        if not self._running:
                            break
                        await self._handle_message(msg)

            except websockets.ConnectionClosed:
                if self._running:
                    print("  ⚠ Соединение потеряно, переподключаюсь через 5с...")
                    await asyncio.sleep(5)
            except Exception as e:
                if self._running:
                    print(f"  ✗ Ошибка WS: {e}, переподключаюсь через 10с...")
                    await asyncio.sleep(10)

    async def stop(self):
        self._running = False
        if self._ws:
            await self._ws.close()

    async def _init_snapshots(self, symbols: list[str]):
        """Получаем начальные снимки стаканов через REST"""
        client = MexcClientAsync()
        try:
            # Тикеры для объёмов
            tickers = await client.get_all_tickers_24h()
            if tickers:
                self.ticker_cache = {
                    t["symbol"]: t for t in tickers if "symbol" in t
                }

            for sym in symbols:
                book = await client.get_order_book(sym, config.ORDER_BOOK_DEPTH)
                if book:
                    ob = self.order_books[sym]
                    ob.apply_snapshot(
                        book.get("bids", []),
                        book.get("asks", []),
                    )
                    # Сохраняем начальный анализ
                    ticker = self.ticker_cache.get(sym, {"quoteVolume": "0"})
                    result = analyze_order_book(sym, book, ticker)
                    if result:
                        self.prev_results[sym] = result
                await asyncio.sleep(0.2)

            initialized = sum(
                1 for ob in self.order_books.values() if ob.initialized
            )
            print(f"  ✓ Инициализировано {initialized}/{len(symbols)} стаканов")
        finally:
            await client.close()

    async def _handle_message(self, raw_msg: str):
        """Обработка WebSocket-сообщения"""
        try:
            data = json.loads(raw_msg)
        except json.JSONDecodeError:
            return

        self.stats["messages"] += 1

        # Парсим канал
        channel = data.get("channel", "") or data.get("c", "")
        symbol = data.get("symbol", "") or data.get("s", "")

        if not symbol or symbol not in self.order_books:
            return

        # Обновление лимитной глубины
        depth = data.get("publiclimitdepths") or data.get("d", {})
        if not depth:
            return

        bids_list = depth.get("bidsList", depth.get("bids", []))
        asks_list = depth.get("asksList", depth.get("asks", []))

        # Формат может быть разный
        bids = []
        for b in bids_list:
            if isinstance(b, dict):
                bids.append([b.get("price", "0"), b.get("quantity", "0")])
            elif isinstance(b, list):
                bids.append(b)

        asks = []
        for a in asks_list:
            if isinstance(a, dict):
                asks.append([a.get("price", "0"), a.get("quantity", "0")])
            elif isinstance(a, list):
                asks.append(a)

        if not bids and not asks:
            return

        # Обновляем локальный стакан (для лимитной глубины — полная замена)
        ob = self.order_books[symbol]
        ob.apply_snapshot(bids, asks)

        # Анализируем каждые 5 секунд (не каждое сообщение)
        if time.time() - ob.last_update < 5.0:
            return

        await self._analyze_and_alert(symbol)

    async def _analyze_and_alert(self, symbol: str):
        """Анализирует стакан и генерирует алерты"""
        ob = self.order_books[symbol]
        if not ob.initialized:
            return

        depth = ob.to_depth_dict()
        ticker = self.ticker_cache.get(symbol, {"quoteVolume": "0"})
        result = analyze_order_book(symbol, depth, ticker)

        if not result:
            return

        prev = self.prev_results.get(symbol)

        # ─── Детекция переставляшей ───
        if prev:
            movers = detect_movers(result, prev)
            if movers:
                result.mover_events = movers
                self.stats["movers_detected"] += len(movers)
                for event in movers:
                    await self.on_event("MOVER", event, result)

        # ─── Детекция новых крупных стенок ───
        if prev:
            prev_prices = {w.price for w in prev.all_walls}
            for wall in result.all_walls:
                if wall.price not in prev_prices and wall.size_usdt >= 100:
                    self.stats["new_walls_detected"] += 1
                    await self.on_event("NEW_WALL", wall, result)

        # Сохраняем как предыдущий
        self.prev_results[symbol] = result

    @staticmethod
    async def _default_callback(event_type: str, event, result: ScanResult):
        """Дефолтный обработчик событий (вывод в консоль)"""
        now = datetime.now().strftime("%H:%M:%S")

        if event_type == "MOVER":
            e: MoverEvent = event
            arrow = "⬆️" if e.direction == "UP" else "⬇️"
            print(
                f"  {now} {arrow} ПЕРЕСТАВЛЯШ {e.symbol} {e.side}: "
                f"${e.size_usdt:,.0f} переехал "
                f"{e.old_price:.8g} → {e.new_price:.8g} "
                f"({e.shift_pct:+.2f}%) | "
                f"спред={result.spread_pct:.1f}%"
            )

        elif event_type == "NEW_WALL":
            w: WallInfo = event
            emoji = "🟢" if w.side == "BID" else "🔴"
            print(
                f"  {now} {emoji} НОВАЯ СТЕНКА {result.symbol} {w.side}: "
                f"${w.size_usdt:,.0f} @ {w.price:.8g} "
                f"({w.multiplier}x, {w.distance_pct}%) | "
                f"спред={result.spread_pct:.1f}%"
            )


# ═══════════════════════════════════════════════════
# REST-скан для отбора кандидатов
# ═══════════════════════════════════════════════════

async def scan_and_select(n_pairs: int = 25) -> list[ScanResult]:
    """Сканирует все пары, возвращает топ-N по скору"""
    print(f"\n  📊 REST-сканирование для отбора топ-{n_pairs} пар...\n")
    client = MexcClientAsync()

    try:
        # Шаг 1: Все пары
        info = await client.get_exchange_info()
        if not info:
            return []

        symbols = [
            s["symbol"] for s in info.get("symbols", [])
            if s.get("quoteAsset") == config.QUOTE_ASSET
            and s.get("isSpotTradingAllowed", True)
            and s.get("status") == "1"
        ]
        print(f"  Найдено {len(symbols)} USDT-пар")

        # Шаг 2: Фильтр по объёму
        tickers = await client.get_all_tickers_24h()
        if not tickers:
            return []

        ticker_map = {t["symbol"]: t for t in tickers if "symbol" in t}
        candidates = []
        for sym in symbols:
            t = ticker_map.get(sym)
            if not t:
                continue
            vol = float(t.get("quoteVolume", 0))
            if config.MIN_DAILY_VOLUME_USDT <= vol <= config.MAX_DAILY_VOLUME_USDT:
                candidates.append((sym, t))

        candidates.sort(key=lambda x: float(x[1].get("quoteVolume", 0)), reverse=True)
        print(f"  Отобрано {len(candidates)} кандидатов по объёму")

        # Шаг 3: Сканируем стаканы
        results = []
        batch_size = config.MAX_CONCURRENT_REQUESTS

        for batch_start in range(0, len(candidates), batch_size):
            batch = candidates[batch_start:batch_start + batch_size]
            tasks = []
            for sym, ticker in batch:
                tasks.append(_scan_one(client, sym, ticker))

            batch_results = await asyncio.gather(*tasks)
            for r in batch_results:
                if r:
                    results.append(r)

            pct = min(batch_start + batch_size, len(candidates)) / len(candidates) * 100
            print(f"  {pct:.0f}% | найдено: {len(results)}", end="\r")

            if batch_start + batch_size < len(candidates):
                await asyncio.sleep(config.BATCH_DELAY)

        print()
        results.sort(key=lambda r: r.score, reverse=True)
        top = results[:n_pairs]

        print(f"\n  ✅ Топ-{len(top)} пар для мониторинга:")
        for i, r in enumerate(top, 1):
            biggest = r.biggest_wall
            if biggest:
                print(
                    f"    {i:2}. {r.symbol:<14} ⭐{r.score:>5} | "
                    f"спред={r.spread_pct:.1f}% | "
                    f"стенка ${biggest.size_usdt:,.0f} ({biggest.side})"
                )

        return top
    finally:
        await client.close()


async def _scan_one(client, symbol, ticker):
    try:
        book = await client.get_order_book(symbol, config.ORDER_BOOK_DEPTH)
        if not book:
            return None
        result = analyze_order_book(symbol, book, ticker)
        if result and result.spread_pct < config.MIN_SPREAD_PCT:
            return None
        return result
    except Exception:
        return None


# ═══════════════════════════════════════════════════
# Точка входа
# ═══════════════════════════════════════════════════

async def main():
    parser = argparse.ArgumentParser(description="MEXC WS Monitor")
    parser.add_argument("--pairs", type=int, default=25,
                        help="Кол-во пар для мониторинга (макс ~30)")
    parser.add_argument("--symbols", type=str, default=None,
                        help="Конкретные пары через запятую")
    args = parser.parse_args()

    print("=" * 55)
    print("  MEXC WebSocket Monitor v2.0")
    print("  Реалтайм детекция плотностей и переставляшей")
    print("=" * 55)

    monitor = MexcWsMonitor()

    # Обработка Ctrl+C
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(monitor.stop()))

    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",")]
    else:
        top_results = await scan_and_select(min(args.pairs, 28))
        symbols = [r.symbol for r in top_results]

    if not symbols:
        print("  ✗ Не найдено подходящих пар")
        return

    await monitor.start(symbols)


if __name__ == "__main__":
    asyncio.run(main())

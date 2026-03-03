# -*- coding: utf-8 -*-
"""MEXC Density Scanner v5.0"""
import io, time, zipfile, math
from datetime import datetime
from collections import Counter
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from mexc_client import MexcClientSync
from analyzer import analyze_order_book
from history import DensityTracker

st.set_page_config(page_title="MEXC Scanner", layout="wide",
                   initial_sidebar_state="expanded")

# ======= helpers =======
def sf(v, d=0.0):
    if v is None or v == "": return d
    try: return float(v)
    except: return d
def si(v, d=0):
    try: return int(sf(v, d))
    except: return d
def parse_book(raw):
    out = []
    if not raw or not isinstance(raw, list): return out
    for e in raw:
        if not isinstance(e, (list, tuple)) or len(e) < 2: continue
        p, q = sf(e[0]), sf(e[1])
        if p > 0 and q > 0: out.append((p, q))
    return out
def extract_tc(td):
    if isinstance(td, list): td = td[0] if td else {}
    if not isinstance(td, dict): return 0
    for k in ("count", "tradeCount", "trades", "txcnt"):
        v = td.get(k)
        if v is not None:
            r = si(v)
            if r > 0: return r
    return 0
def parse_klines(raw):
    if not raw or not isinstance(raw, list): return pd.DataFrame()
    rows = []
    for k in raw:
        if not isinstance(k, (list, tuple)) or len(k) < 6: continue
        rows.append({"open_time": sf(k[0]), "open": sf(k[1]), "high": sf(k[2]),
            "low": sf(k[3]), "close": sf(k[4]), "volume": sf(k[5]),
            "close_time": sf(k[6]) if len(k) > 6 else 0,
            "quote_volume": sf(k[7]) if len(k) > 7 else 0,
            "trades": si(k[8]) if len(k) > 8 else 0})
    if not rows: return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["time"] = pd.to_datetime(df["open_time"], unit="ms")
    return df
def fmt_price(price):
    if price <= 0: return "0"
    if price >= 1000: return f"{price:,.0f}"
    if price >= 1: return f"{price:.2f}"
    if price >= 0.01: return f"{price:.4f}"
    if price >= 0.0001: return f"{price:.6f}"
    return f"{price:.8f}"
def fmt_usd(v):
    if v <= 0: return "---"
    if v >= 1_000_000: return f"${v/1_000_000:.1f}M"
    if v >= 1000: return f"${v/1000:.1f}K"
    return f"${v:,.0f}"
def mexc_link(s):
    return f"https://www.mexc.com/exchange/{s.replace('USDT', '_USDT')}"
def make_csv(df):
    return df.to_csv(index=False).encode("utf-8-sig")
def kline_stats(df, n=None):
    if df is None or df.empty:
        return {"volume": 0.0, "trades": 0}
    sub = df.tail(n) if n else df
    return {
        "volume": float(sub["quote_volume"].sum()) if "quote_volume" in sub else 0.0,
        "trades": int(sub["trades"].sum()) if "trades" in sub else 0
    }
def analyze_robots(trades_raw):
    if not trades_raw or not isinstance(trades_raw, list) or len(trades_raw) < 5:
        return None
    times = sorted([sf(t.get("time", 0)) for t in trades_raw
                    if sf(t.get("time", 0)) > 0], reverse=True)
    if len(times) < 5: return None
    deltas = [abs(times[i] - times[i + 1]) / 1000 for i in range(len(times) - 1)]
    deltas = [d for d in deltas if 0 <= d < 600]
    if not deltas: return None
    volumes = [sf(t.get("price", 0)) * sf(t.get("qty", 0))
               for t in trades_raw
               if sf(t.get("price", 0)) > 0 and sf(t.get("qty", 0)) > 0]
    avg_d = sum(deltas) / len(deltas)
    min_d, max_d = min(deltas), max(deltas)
    mc = Counter([round(d) for d in deltas])
    mode_val, mode_cnt = mc.most_common(1)[0]
    buckets = {}
    for i, d in enumerate(deltas):
        bk = int(d // 5) * 5
        if bk not in buckets: buckets[bk] = {"count": 0, "vols": []}
        buckets[bk]["count"] += 1
        if i < len(volumes): buckets[bk]["vols"].append(volumes[i])
    robots = []
    for bk, info in sorted(buckets.items()):
        pct = info["count"] / len(deltas) * 100
        if pct < 15 or info["count"] < 3: continue
        avg_vol = sum(info["vols"]) / len(info["vols"]) if info["vols"] else 0
        robots.append({"interval": f"{bk}-{bk + 5}c",
                        "count": info["count"], "pct": round(pct, 1),
                        "avg_vol": avg_vol})
    return {
        "avg": avg_d, "min": min_d, "max": max_d,
        "mode": mode_val, "mode_count": mode_cnt,
        "mode_pct": round(mode_cnt / len(deltas) * 100, 1),
        "is_robot": avg_d < 30 and max_d < 120,
        "avg_vol": sum(volumes) / len(volumes) if volumes else 0,
        "robots": robots
    }


def get_trades_5m_count(client, symbol):
    """Получить кол-во сделок за последние 5 минут из klines"""
    try:
        kl = client.get_klines(symbol, "5m", 1)
        if kl and isinstance(kl, list) and len(kl) > 0:
            last = kl[-1]
            if isinstance(last, (list, tuple)) and len(last) > 8:
                return si(last[8])
        return 0
    except:
        return 0


# ======= Charts - v5.0 dual Y-axis =======
BID_COLOR = "rgba(0,200,83,0.7)"
ASK_COLOR = "rgba(255,23,68,0.7)"
BID_CANDLE = "#00c853"
ASK_CANDLE = "#ff1744"
PRICE_LINE = "#00d2ff"


def build_candlestick_dual(df, symbol, interval, cur_price=None):
    """
    Свечной график с двойной Y-осью:
      - Левая: цена
      - Правая: отклонение в % от текущей цены
      - Нулевая линия подсвечена
      - Внизу объёмы
    """
    if df is None or df.empty or len(df) < 2:
        return None
    try:
        ref_price = cur_price if cur_price and cur_price > 0 else float(df["close"].iloc[-1])
        if ref_price <= 0:
            return None

        fig = make_subplots(
            rows=2, cols=1, shared_xaxes=True,
            vertical_spacing=0.03, row_heights=[0.75, 0.25],
            specs=[[{"secondary_y": True}], [{"secondary_y": False}]])

        # Свечи — на основную ось (левая, цена)
        fig.add_trace(go.Candlestick(
            x=df["time"], open=df["open"], high=df["high"],
            low=df["low"], close=df["close"],
            increasing_line_color=BID_CANDLE,
            decreasing_line_color=ASK_CANDLE,
            name="Price"), row=1, col=1, secondary_y=False)

        # Невидимая линия для правой оси — % отклонение
        pct_vals = [(float(c) - ref_price) / ref_price * 100 for c in df["close"]]
        fig.add_trace(go.Scatter(
            x=df["time"], y=pct_vals,
            mode="lines", line=dict(width=0),
            showlegend=False, hoverinfo="skip",
            name="% dev"), row=1, col=1, secondary_y=True)

        # Объёмы
        colors = [BID_CANDLE if c >= o else ASK_CANDLE
                  for c, o in zip(df["close"], df["open"])]
        fig.add_trace(go.Bar(
            x=df["time"], y=df["volume"],
            marker_color=colors, opacity=0.5,
            name="Vol"), row=2, col=1)

        # Текущая цена
        if cur_price and cur_price > 0:
            fig.add_hline(
                y=float(cur_price), line_dash="dot",
                line_color=PRICE_LINE, line_width=1.5,
                annotation_text=f"  {cur_price:.8g}",
                annotation_font_color=PRICE_LINE,
                annotation_font_size=11,
                row=1, col=1)

        # Нулевая линия на правой оси
        fig.add_hline(
            y=0, line_dash="solid", line_color="rgba(255,255,255,0.4)",
            line_width=1, row=1, col=1, secondary_y=True)

        # Диапазоны правой оси — по данным
        price_min = float(df["low"].min())
        price_max = float(df["high"].max())
        pct_min = (price_min - ref_price) / ref_price * 100
        pct_max = (price_max - ref_price) / ref_price * 100
        pct_abs = max(abs(pct_min), abs(pct_max), 0.5)
        pct_margin = pct_abs * 1.15

        fig.update_layout(
            title=f"{symbol}  •  {interval}",
            template="plotly_dark",
            height=480,
            xaxis_rangeslider_visible=False,
            showlegend=False,
            margin=dict(l=60, r=60, t=45, b=20))

        fig.update_yaxes(
            title_text="Цена", side="left",
            row=1, col=1, secondary_y=False)
        fig.update_yaxes(
            title_text="Откл. %", side="right",
            range=[-pct_margin, pct_margin],
            ticksuffix="%",
            zeroline=True, zerolinecolor="rgba(255,255,0,0.5)",
            zerolinewidth=2,
            row=1, col=1, secondary_y=True)
        fig.update_yaxes(
            title_text="Объём", row=2, col=1)

        return fig
    except:
        return None


def build_orderbook_chart(bids, asks, cur_price, depth=50):
    try:
        b, a = bids[:depth], asks[:depth]
        if not b and not a: return None
        fig = go.Figure()
        if b:
            fig.add_trace(go.Bar(
                y=[float(p) for p, q in b],
                x=[float(p * q) for p, q in b],
                orientation="h", name="BID",
                marker_color=BID_COLOR,
                hovertemplate="%{y:.8g}<br>$%{x:,.0f}<extra>BID</extra>"))
        if a:
            fig.add_trace(go.Bar(
                y=[float(p) for p, q in a],
                x=[float(p * q) for p, q in a],
                orientation="h", name="ASK",
                marker_color=ASK_COLOR,
                hovertemplate="%{y:.8g}<br>$%{x:,.0f}<extra>ASK</extra>"))
        if cur_price and float(cur_price) > 0:
            fig.add_hline(y=float(cur_price), line_dash="dot",
                          line_color=PRICE_LINE, line_width=2,
                          annotation_text=f"  {float(cur_price):.8g}",
                          annotation_font_color=PRICE_LINE)
        fig.update_layout(
            xaxis_title="$ USDT", yaxis_title="",
            template="plotly_dark",
            height=max(500, depth * 12),
            barmode="relative", showlegend=True,
            margin=dict(l=80, r=20, t=40, b=30))
        return fig
    except:
        return None

def build_heatmap(bids, asks, cur_price, depth=30):
    try:
        levels = []
        for p, q in bids[:depth]:
            levels.append(("BID", float(p), float(p * q)))
        for p, q in asks[:depth]:
            levels.append(("ASK", float(p), float(p * q)))
        if not levels: return None
        levels.sort(key=lambda x: x[1])
        mx = max(v for _, _, v in levels)
        if mx <= 0: mx = 1.0
        prices, vols, colors, hovers = [], [], [], []
        for side, price, vol in levels:
            intensity = min(float(vol) / float(mx), 1.0)
            prices.append(price)
            vols.append(vol)
            if side == "BID":
                g = int(80 + 175 * intensity)
                c = f"rgba(0,{g},83,0.85)"
            else:
                r = int(80 + 175 * intensity)
                gb = int(60 * (1.0 - intensity))
                c = f"rgba({r},{gb},68,0.85)"
            colors.append(c)
            hovers.append(f"{side}: ${vol:,.0f} @ {price:.8g}")
        fig = go.Figure(go.Bar(
            y=prices, x=vols, orientation="h",
            marker_color=colors, hovertext=hovers,
            hoverinfo="text", showlegend=False))
        if cur_price and float(cur_price) > 0:
            fig.add_hline(y=float(cur_price), line_dash="dot",
                          line_color=PRICE_LINE, line_width=2,
                          annotation_text=f"  {float(cur_price):.8g}",
                          annotation_font_color=PRICE_LINE)
        fig.update_layout(
            template="plotly_dark", height=500,
            yaxis_title="", xaxis_title="$ USDT",
            margin=dict(l=80, r=20, t=40, b=30))
        return fig
    except:
        return None


# ======= State =======
for k, v in [("tracker", DensityTracker()), ("scan_results", []),
             ("last_scan", 0.0), ("total_pairs", 0),
             ("client", MexcClientSync()), ("detail_symbol", ""),
             ("favorites", set()), ("blacklist", set()),
             ("cancel_scan", False), ("current_page", 0)]:
    if k not in st.session_state:
        st.session_state[k] = v

def go_detail(sym):
    st.session_state.detail_symbol = sym
    st.session_state.current_page = 1


# ======= Scan =======
def run_scan(min_vol, max_vol, min_spread, wall_mult, min_wall_usd, top_n):
    import config as cfg
    cfg.MIN_DAILY_VOLUME_USDT = min_vol
    cfg.MAX_DAILY_VOLUME_USDT = max_vol
    cfg.MIN_SPREAD_PCT = min_spread
    cfg.WALL_MULTIPLIER = wall_mult
    cfg.MIN_WALL_SIZE_USDT = min_wall_usd
    client = st.session_state.client
    st.session_state.cancel_scan = False
    progress = st.progress(0, "API...")
    if not st.session_state.get("_api_tested"):
        ok, msg = client.ping()
        st.session_state._api_tested = True
        if not ok:
            st.error(f"MEXC API: {msg}")
            progress.empty(); return
    progress.progress(3, "Pairs...")
    try:
        info = client.get_exchange_info()
    except Exception as e:
        st.error(f"API: {e}"); progress.empty(); return
    if not info or "symbols" not in info:
        st.error(f"Error: {client.last_error}")
        st.session_state._api_tested = False
        progress.empty(); return
    bl = st.session_state.blacklist
    all_sym = []
    for s in info["symbols"]:
        try:
            if s.get("quoteAsset") != "USDT": continue
            sym = s["symbol"]
            if sym in bl: continue
            st_ = s.get("status", "")
            ok = (str(st_) in ("1", "ENABLED", "True", "true")
                  or st_ is True or st_ == 1)
            if ok and s.get("isSpotTradingAllowed", True):
                all_sym.append(sym)
        except:
            continue
    if not all_sym:
        for s in info["symbols"]:
            try:
                sym = s.get("symbol", "")
                if s.get("quoteAsset") == "USDT" and sym not in bl:
                    all_sym.append(sym)
            except:
                continue
    if not all_sym:
        st.error("0 par"); progress.empty(); return
    progress.progress(5)
    try:
        tickers = client.get_all_tickers_24h()
    except:
        st.error("Tickers error"); progress.empty(); return
    if not tickers:
        st.error(str(client.last_error)); progress.empty(); return
    tm = {t["symbol"]: t for t in tickers if "symbol" in t}
    cands = [(sym, tm[sym]) for sym in all_sym
             if sym in tm
             and min_vol <= sf(tm[sym].get("quoteVolume", 0)) <= max_vol]
    cands.sort(key=lambda x: sf(x[1].get("quoteVolume", 0)), reverse=True)
    if not cands:
        st.warning("0 в диапазоне"); progress.empty(); return
    progress.progress(15, f"{len(cands)}...")
    results, total = [], len(cands)
    for i, (sym, tk) in enumerate(cands):
        if st.session_state.cancel_scan:
            st.warning(f"Stop {i}/{total}"); break
        try:
            book = client.get_order_book(sym, cfg.ORDER_BOOK_DEPTH)
            if book:
                r = analyze_order_book(sym, book, tk)
                if r and r.spread_pct >= min_spread:
                    tc = extract_tc(tk)
                    r.trade_count_24h = tc
                    results.append(r)
        except:
            pass
        if (i + 1) % 8 == 0 or i == total - 1:
            progress.progress(
                15 + int((i + 1) / total * 80),
                f"{i + 1}/{total} -> {len(results)}")
    results.sort(key=lambda r: r.score, reverse=True)
    top = results[:top_n]
    progress.progress(92, "Обогащение данных...")

    # Обогащаем top результаты: trade_count_24h + trades_5m
    for idx, r in enumerate(top[:30]):
        try:
            # 24h trade count
            if r.trade_count_24h == 0:
                single_tk = client.get_ticker_24h(r.symbol)
                tc = extract_tc(single_tk)
                if tc > 0:
                    r.trade_count_24h = tc

            # 5m trade count из klines
            trades5 = get_trades_5m_count(client, r.symbol)
            r._trades_5m = trades5
        except:
            if not hasattr(r, '_trades_5m'):
                r._trades_5m = 0
        if (idx + 1) % 5 == 0:
            progress.progress(
                92 + int((idx + 1) / min(len(top), 30) * 6),
                f"Обогащение {idx + 1}/{min(len(top), 30)}")

    st.session_state.tracker.update(top)
    st.session_state.scan_results = top
    st.session_state.last_scan = time.time()
    st.session_state.total_pairs = total
    st.session_state.cancel_scan = False
    progress.progress(100, "OK!")
    time.sleep(0.2)
    progress.empty()


# ======= Sidebar =======
import config as cfg_module
TF_MULT = cfg_module.VOLUME_TIMEFRAMES

with st.sidebar:
    st.markdown("## ⚙️ Параметры сканера")
    st.markdown("---")

    st.markdown("**Период объёма**")
    vol_tf = st.selectbox(
        "Таймфрейм объёма",
        list(TF_MULT.keys()),
        index=4,
        key="vol_tf",
        help="За какой период считать объём для фильтра",
        label_visibility="collapsed")

    min_vol_input = st.number_input(
        f"Мин объём {vol_tf} ($)",
        value=100, min_value=0, step=50,
        help="Минимальный объём за выбранный период")
    max_vol_input = st.number_input(
        f"Макс объём {vol_tf} ($)",
        value=500_000, min_value=100, step=10000,
        help="Максимальный объём за выбранный период")

    mult = TF_MULT[vol_tf]
    min_vol = min_vol_input * mult
    max_vol = max_vol_input * mult
    if vol_tf != "24h":
        st.caption(f"≈ ${min_vol:,.0f} – ${max_vol:,.0f} за 24ч")

    st.markdown("---")
    min_spread = st.slider("Мин спред %", 0.0, 20.0, 0.5, 0.1)
    wall_mult = st.slider(
        "Чувствительность стенок (x)", 2, 50, 5,
        help="Во сколько раз объём уровня превышает медиану стакана")
    min_wall_usd = st.number_input(
        "Мин стенка ($)", value=50, min_value=1, step=10)
    top_n = st.slider("Макс результатов", 5, 100, 30)
    st.markdown("---")
    auto_on = st.checkbox("Авто-скан", value=True)
    auto_sec = st.select_slider(
        "Интервал (с)", [15, 20, 30, 45, 60, 90], value=30)
    if auto_on:
        try:
            from streamlit_autorefresh import st_autorefresh
            st_autorefresh(interval=auto_sec * 1000, key="ar")
        except ImportError:
            st.caption("pip install streamlit-autorefresh")
    c1s, c2s = st.columns(2)
    scan_btn = c1s.button(
        "🔍 СКАН", use_container_width=True, type="primary")
    if c2s.button("⛔ СТОП", use_container_width=True):
        st.session_state.cancel_scan = True
    st.markdown("---")
    with st.expander("🚫 Чёрный список"):
        bl_inp = st.text_input(
            "Добавить", placeholder="XYZUSDT,ABCUSDT", key="bl_inp")
        if bl_inp:
            for s in bl_inp.upper().replace(" ", "").split(","):
                if s.endswith("USDT"):
                    st.session_state.blacklist.add(s)
            st.rerun()
        if st.session_state.blacklist:
            st.caption(", ".join(sorted(st.session_state.blacklist)))
            if st.button("Очистить"):
                st.session_state.blacklist = set()
                st.rerun()
    with st.expander("⭐ Избранное"):
        fav = st.session_state.favorites
        if fav:
            st.caption(", ".join(sorted(fav)))
        up = st.file_uploader(
            "Import CSV", type=["csv", "txt"], key="fi",
            label_visibility="collapsed")
        if up:
            new = {l.strip().upper()
                   for l in up.getvalue().decode("utf-8")
                       .replace(",", "\n").split("\n")
                   if l.strip().upper().endswith("USDT")
                   and len(l.strip()) > 4}
            if new:
                st.session_state.favorites.update(new)
                st.rerun()
        if fav:
            st.download_button(
                "Export", data="\n".join(sorted(fav)).encode(),
                file_name="fav.csv", mime="text/csv",
                use_container_width=True)
    st.markdown("---")
    stats = st.session_state.tracker.get_stats()
    st.caption(
        f"Сканов: {stats['total_scans']} | "
        f"Переставок: {stats['total_mover_events']}")
    if st.button("🏓 Test API", use_container_width=True):
        ok, msg = st.session_state.client.ping()
        if ok:
            st.success(msg)
        else:
            st.error(msg)


# ======= Auto-scan trigger =======
need_scan = scan_btn or (
    auto_on and time.time() - st.session_state.last_scan
    > max(auto_sec - 3, 10))
if need_scan:
    run_scan(min_vol, max_vol, min_spread, wall_mult,
             min_wall_usd, top_n)


# ══════════════════════════════════════════════════
# НАВИГАЦИЯ — 3 кнопки-таба с эмодзи и названиями
# ══════════════════════════════════════════════════
TAB_LABELS = [
    "📊 Сканер",
    "🔍 Стакан",
    "📈 Переставки",
]
cp = st.session_state.current_page

# Стилизованные кнопки навигации
st.markdown("""
<style>
div[data-testid="stHorizontalBlock"] > div > div > button {
    font-size: 1.05rem !important;
    font-weight: 600 !important;
    padding: 0.55rem 0 !important;
    border-radius: 8px !important;
}
</style>
""", unsafe_allow_html=True)

nav_cols = st.columns(len(TAB_LABELS))
for i, label in enumerate(TAB_LABELS):
    with nav_cols[i]:
        btype = "primary" if cp == i else "secondary"
        if st.button(label, key=f"nav_{i}",
                     use_container_width=True, type=btype):
            st.session_state.current_page = i
            st.rerun()

st.markdown("---")
page = cp


# ═════════════════════════════════════════════════
# PAGE 0: 📊 СКАНЕР (таблица + скоринг)
# ═════════════════════════════════════════════════
if page == 0:
    results = st.session_state.scan_results
    tracker = st.session_state.tracker
    if not results:
        if auto_on:
            st.info("⏳ Ожидание первого скана...")
        else:
            st.info("Нажми 🔍 СКАН в боковой панели")
    else:
        mc1, mc2, mc3, mc4 = st.columns(4)
        mc1.metric("Найдено", len(results))
        mc2.metric("Проверено", st.session_state.total_pairs)
        mc3.metric("Лучший скор", f"{results[0].score}")
        mc4.metric("Переставки", sum(
            1 for r in results if r.has_movers))

        rows = []
        for r in results:
            if not r.all_walls:
                continue
            bw = r.biggest_wall
            tw_list = tracker.get_tracked_walls(r.symbol)
            tw_big = None
            if tw_list:
                for tw in tw_list:
                    if bw and abs(tw.price - bw.price) < bw.price * 0.001:
                        tw_big = tw
                        break
                if not tw_big:
                    tw_big = tw_list[0]
            lt_s = tw_big.lifetime_sec if tw_big else 0
            lt_str = tw_big.lifetime_str if tw_big else "---"
            bt = (max(r.bid_walls, key=lambda w: w.size_usdt)
                  if r.bid_walls else None)
            at = (max(r.ask_walls, key=lambda w: w.size_usdt)
                  if r.ask_walls else None)

            # Кол-во сделок за 5 минут
            trades_5m = getattr(r, '_trades_5m', 0)

            rows.append({
                "Скор": r.score,
                "Пара": r.symbol,
                "Спред%": round(r.spread_pct, 2),
                "Об24ч$": round(r.volume_24h_usdt),
                "Сд24ч": (r.trade_count_24h
                           if r.trade_count_24h > 0 else 0),
                "BID$": round(bt.size_usdt) if bt else 0,
                "BIDx": bt.multiplier if bt else 0,
                "Сд5м": trades_5m,
                "ASK$": round(at.size_usdt) if at else 0,
                "ASKx": at.multiplier if at else 0,
                "ASK%": at.distance_pct if at else 0,
                "Жизнь": lt_str,
            })
        if rows:
            df = pd.DataFrame(rows)
            df = df.sort_values("Скор", ascending=False)
            df = df.reset_index(drop=True)
            st.caption(
                "Кликни на заголовок столбца для сортировки")
            st.dataframe(df, hide_index=True,
                         use_container_width=True,
                         height=min(len(df) * 35 + 40, 700))

            st.markdown("##### Выбери пару для анализа")
            syms = df["Пара"].tolist()
            nc = min(10, len(syms))
            btn_cols = st.columns(nc)
            for i, sym in enumerate(syms[:nc]):
                with btn_cols[i]:
                    if st.button(sym, key=f"go_{sym}",
                                 use_container_width=True):
                        go_detail(sym)
                        st.rerun()
            if len(syms) > nc:
                sel_c, go_c = st.columns([3, 1])
                with sel_c:
                    chosen = st.selectbox(
                        "Все пары", [""] + syms, key="all_pairs")
                with go_c:
                    if chosen and st.button("→", key="go_ch"):
                        go_detail(chosen)
                        st.rerun()
            st.download_button(
                "📥 Скачать CSV", data=make_csv(df),
                file_name=f"scan_{datetime.now().strftime('%H%M')}.csv",
                mime="text/csv")


# ═════════════════════════════════════════════════
# PAGE 1: 🔍 СТАКАН (визуализация + хитмап + график)
# ═════════════════════════════════════════════════
elif page == 1:
    results = st.session_state.scan_results
    sym_list = [r.symbol for r in results] if results else []

    hdr = st.columns([1, 3, 2, 1, 1])
    with hdr[0]:
        if st.button("← Назад"):
            st.session_state.current_page = 0
            st.rerun()
    with hdr[1]:
        idx = 0
        ds = st.session_state.detail_symbol
        if ds and ds in sym_list:
            idx = sym_list.index(ds) + 1
        target = st.selectbox(
            "Пара", [""] + sym_list, index=idx,
            key="detail_sel", label_visibility="collapsed")
    with hdr[2]:
        manual = st.text_input(
            "Ручной ввод", placeholder="XYZUSDT",
            label_visibility="collapsed")
    symbol = manual.strip().upper() if manual.strip() else target
    with hdr[3]:
        if symbol:
            is_fav = symbol in st.session_state.favorites
            lbl = "⭐" if is_fav else "☆"
            if st.button(lbl, key="fav_detail"):
                if is_fav:
                    st.session_state.favorites.discard(symbol)
                else:
                    st.session_state.favorites.add(symbol)
                st.rerun()
    with hdr[4]:
        if symbol and st.button("🚫", key="bl_detail",
                                help="В чёрный список"):
            st.session_state.blacklist.add(symbol)
            st.rerun()

    if not symbol:
        st.info("Выбери пару из списка или введи вручную")
        st.stop()
    st.session_state.detail_symbol = symbol
    client = st.session_state.client
    tracker = st.session_state.tracker

    with st.spinner(f"Загрузка {symbol}..."):
        try:
            book_raw = client.get_order_book(symbol, 500)
            ticker_raw = client.get_ticker_24h(symbol)
            trades_raw = client.get_recent_trades(symbol, 1000)
            # Загрузка всех таймфреймов свечей
            klines_data = {}
            for tf_key, tf_cfg in cfg_module.CHART_INTERVALS.items():
                kl_raw = client.get_klines(symbol, tf_cfg["api"], tf_cfg["limit"])
                klines_data[tf_key] = parse_klines(kl_raw)
        except Exception as e:
            st.error(str(e))
            st.stop()

    if not book_raw:
        err_msg = client.last_error or "нет ответа"
        st.error(f"Нет стакана {symbol}: {err_msg}")
        ok, msg = client.ping()
        st.caption(f"API: {msg}")
        st.stop()
    if not book_raw.get("bids") or not book_raw.get("asks"):
        st.error(f"Пустой стакан {symbol}")
        st.stop()

    bids = parse_book(book_raw["bids"])
    asks = parse_book(book_raw["asks"])
    if not bids or not asks:
        st.error("Пустой стакан")
        st.stop()

    bb = float(bids[0][0])
    ba = float(asks[0][0])
    mid = (bb + ba) / 2
    spread = (ba - bb) / bb * 100
    bdepth = sum(float(p) * float(q) for p, q in bids)
    adepth = sum(float(p) * float(q) for p, q in asks)
    td = ticker_raw
    if isinstance(td, list):
        td = td[0] if td else {}
    if not isinstance(td, dict):
        td = {}
    tc24 = extract_tc(td)
    vol24 = sf(td.get("quoteVolume", 0))

    df_5m = klines_data.get("5m", pd.DataFrame())
    df_1h = klines_data.get("1h", pd.DataFrame())

    st.markdown(
        f"### {symbol}  •  {fmt_price(mid)}  •  "
        f"[MEXC ↗]({mexc_link(symbol)})")

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Спред", f"{spread:.2f}%")
    m2.metric("Bid $", f"${bdepth:,.0f}")
    m3.metric("Ask $", f"${adepth:,.0f}")
    m4.metric("Сделки 24ч", f"{tc24:,}" if tc24 else "---")
    m5.metric("Объём 24ч", f"${vol24:,.0f}")
    s4h = kline_stats(df_1h, 4)
    m6.metric("Объём 4ч", f"${s4h['volume']:,.0f}")

    # ── Графики с выбором таймфрейма ──
    st.markdown("#### 📈 График цены")
    chart_tf_labels = list(cfg_module.CHART_INTERVALS.keys())
    # Кнопки выбора таймфрейма
    tf_cols = st.columns(len(chart_tf_labels))
    if "chart_tf" not in st.session_state:
        st.session_state.chart_tf = "1h"
    for i, tf_lbl in enumerate(chart_tf_labels):
        with tf_cols[i]:
            btype = "primary" if st.session_state.chart_tf == tf_lbl else "secondary"
            if st.button(tf_lbl, key=f"ctf_{tf_lbl}",
                         use_container_width=True, type=btype):
                st.session_state.chart_tf = tf_lbl
                st.rerun()

    sel_tf = st.session_state.chart_tf
    dfk = klines_data.get(sel_tf, pd.DataFrame())
    fig_chart = build_candlestick_dual(dfk, symbol, sel_tf, mid)
    if fig_chart:
        st.plotly_chart(fig_chart, use_container_width=True)
    else:
        st.warning(f"Нет данных для {sel_tf}")

    # ── Стенки (плотности) ──
    st.markdown("#### 🧱 Стенки (плотности)")
    tw_list = tracker.get_tracked_walls(symbol)
    if tw_list:
        twr = []
        for tw in tw_list:
            side_label = "🟢 BID" if tw.side == "BID" else "🔴 ASK"
            twr.append({
                "Сторона": side_label,
                "Цена": fmt_price(tw.price),
                "Объём": fmt_usd(tw.size_usdt),
                "Множ.": f"{tw.multiplier}x",
                "Расст%": f"{tw.distance_pct}%",
                "Жизнь": tw.lifetime_str,
                "Сканов": tw.seen_count,
            })
        st.dataframe(pd.DataFrame(twr), hide_index=True,
                     use_container_width=True)
    else:
        st.caption("Стенки появятся после нескольких сканов")

    # ── Переставки по паре ──
    sym_movers = tracker.get_symbol_movers(symbol)
    if sym_movers:
        st.markdown("#### 🔀 Переставки (история)")
        mvr = []
        for e in reversed(sym_movers[-20:]):
            d = "⬆ LONG" if e.direction == "UP" else "⬇ SHORT"
            mvr.append({
                "Время": datetime.fromtimestamp(
                    e.timestamp).strftime("%H:%M:%S"),
                "Сторона": e.side,
                "Объём": fmt_usd(e.size_usdt),
                "Было": fmt_price(e.old_price),
                "Стало": fmt_price(e.new_price),
                "Сдвиг%": f"{e.shift_pct:+.3f}%",
                "Направл.": d,
            })
        st.dataframe(pd.DataFrame(mvr), hide_index=True,
                     use_container_width=True)

    # ── Объёмы по таймфреймам ──
    st.markdown("#### 📊 Объёмы и сделки")
    s5 = kline_stats(df_5m, 1)
    s15 = kline_stats(df_5m, 3)
    s60 = kline_stats(df_5m, 12)
    vc = st.columns(5)
    vc[0].metric("5м", f"${s5['volume']:,.0f}",
                 f"{s5['trades']} сд.")
    vc[1].metric("15м", f"${s15['volume']:,.0f}",
                 f"{s15['trades']} сд.")
    vc[2].metric("1ч", f"${s60['volume']:,.0f}",
                 f"{s60['trades']} сд.")
    vc[3].metric("4ч", f"${s4h['volume']:,.0f}",
                 f"{s4h['trades']} сд.")
    vc[4].metric("24ч", f"${vol24:,.0f}",
                 f"{tc24:,} сд.")

    # ── Анализ торгов (робот-детектор) ──
    st.markdown("#### 🤖 Анализ торгов")
    robot = analyze_robots(trades_raw)
    if robot:
        ri = robot
        if ri["is_robot"]:
            st.markdown("**🤖 РОБОТ** — стабильные интервалы между сделками")
        else:
            st.markdown("**👤 Человек** — разные интервалы")
        st.markdown(
            f"Интервалы: ср={ri['avg']:.1f}s  "
            f"мин={ri['min']:.1f}s  макс={ri['max']:.1f}s")
        st.markdown(
            f"Мода: {ri['mode']}s ({ri['mode_count']}×, "
            f"{ri['mode_pct']}%) | Ср.объём: {fmt_usd(ri['avg_vol'])}")
        if ri["robots"]:
            st.markdown(f"**Обнаружено ботов: {len(ri['robots'])}**")
            for j, bot in enumerate(ri["robots"]):
                st.markdown(
                    f"  `Бот #{j + 1}`: интервал **{bot['interval']}**, "
                    f"{bot['count']} сделок ({bot['pct']}%), "
                    f"ср.объём {fmt_usd(bot['avg_vol'])}")
    else:
        st.caption("Мало сделок для анализа")

    # ── Стакан + Хитмап ──
    st.markdown("#### 📕 Стакан / Хитмап")
    dv = st.select_slider(
        "Глубина", [20, 30, 50, 100], value=50, key="ob_depth")
    col_ob, col_hm = st.columns(2)
    with col_ob:
        fg = build_orderbook_chart(bids, asks, mid, dv)
        if fg:
            st.plotly_chart(fg, use_container_width=True)
    with col_hm:
        fh = build_heatmap(bids, asks, mid, 30)
        if fh:
            st.plotly_chart(fh, use_container_width=True)

    # ── Последние сделки ──
    if trades_raw and isinstance(trades_raw, list):
        st.markdown("#### 🕐 Последние сделки")
        trs = []
        for t in trades_raw[:50]:
            try:
                p = sf(t.get("price", 0))
                q = sf(t.get("qty", 0))
                ts = sf(t.get("time", 0))
                is_buy = not t.get("isBuyerMaker", True)
                trs.append({
                    "Время": (pd.to_datetime(ts, unit="ms")
                               .strftime("%H:%M:%S")
                               if ts > 0 else "---"),
                    "Цена": fmt_price(p),
                    "Кол-во": q,
                    "$": round(p * q, 2),
                    "Тип": is_buy,
                })
            except:
                continue
        if trs:
            html = '<table style="width:100%;border-collapse:collapse;color:#eee;font-size:13px">'
            html += '<tr style="border-bottom:1px solid #333">'
            for h in ["Время", "Цена", "Кол-во", "$", "Тип"]:
                html += f'<th style="padding:4px 8px;text-align:left">{h}</th>'
            html += '</tr>'
            for row in trs:
                html += '<tr style="border-bottom:1px solid #222">'
                html += f'<td style="padding:3px 8px">{row["Время"]}</td>'
                html += f'<td style="padding:3px 8px">{row["Цена"]}</td>'
                html += f'<td style="padding:3px 8px">{row["Кол-во"]}</td>'
                html += f'<td style="padding:3px 8px">{row["$"]}</td>'
                if row["Тип"]:
                    html += '<td style="padding:3px 8px;color:#00c853">● BUY</td>'
                else:
                    html += '<td style="padding:3px 8px;color:#ff1744">● SELL</td>'
                html += '</tr>'
            html += '</table>'
            st.markdown(html, unsafe_allow_html=True)

    # ── Export ──
    st.markdown("---")
    export_data = {}
    ob_df = pd.DataFrame([
        {"Side": s, "Price": float(p), "Qty": float(q),
         "$": round(float(p * q), 4)}
        for s, data in [("BID", bids), ("ASK", asks)]
        for p, q in data])
    export_data["orderbook"] = ob_df
    for lbl, kdf in klines_data.items():
        if kdf is not None and not kdf.empty:
            export_data[f"klines_{lbl}"] = kdf
    def make_zip():
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for n, d in export_data.items():
                zf.writestr(f"{symbol}_{n}.csv",
                            d.to_csv(index=False))
        buf.seek(0)
        return buf.getvalue()
    st.download_button(
        f"📦 Скачать {symbol} ZIP", data=make_zip(),
        file_name=f"{symbol}.zip", mime="application/zip",
        use_container_width=True)


# ═════════════════════════════════════════════════
# PAGE 2: 📈 МОНИТОРИНГ ПЕРЕСТАВОК (лог + рейтинг)
# ═════════════════════════════════════════════════
elif page == 2:
    tracker = st.session_state.tracker
    st.markdown("### 📈 Монитор переставок")
    st.caption(
        "Переставка = стенка которая двигается по стакану. "
        "Признак робота / ММ.")

    # Кнопки-табы внутри страницы
    sub_labels = ["📋 Журнал", "🏆 Рейтинг"]
    if "mover_subtab" not in st.session_state:
        st.session_state.mover_subtab = 0
    sc = st.columns(len(sub_labels))
    for i, sl in enumerate(sub_labels):
        with sc[i]:
            btype = "primary" if st.session_state.mover_subtab == i else "secondary"
            if st.button(sl, key=f"msub_{i}", use_container_width=True, type=btype):
                st.session_state.mover_subtab = i
                st.rerun()
    st.markdown("---")

    if st.session_state.mover_subtab == 0:
        # ── Журнал ──
        movers = tracker.get_active_movers(7200)
        if not movers:
            st.info("Нет переставок. Запусти несколько сканов.")
        else:
            st.success(f"📊 {len(movers)} переставок за 2ч")
            mr = []
            for e in reversed(movers):
                d = "⬆ LONG" if e.direction == "UP" else "⬇ SHORT"
                mr.append({
                    "Время": datetime.fromtimestamp(
                        e.timestamp).strftime("%H:%M:%S"),
                    "Пара": e.symbol,
                    "Сторона": e.side,
                    "Объём": fmt_usd(e.size_usdt),
                    "Было": fmt_price(e.old_price),
                    "Стало": fmt_price(e.new_price),
                    "Сдвиг%": f"{e.shift_pct:+.3f}%",
                    "Направл.": d,
                })
            mdf = pd.DataFrame(mr)
            st.dataframe(mdf, hide_index=True,
                         use_container_width=True)
            unique_syms = sorted({e.symbol for e in movers})
            sel_c, go_c = st.columns([3, 1])
            with sel_c:
                chosen_mover = st.selectbox(
                    "Перейти к паре", [""] + unique_syms,
                    key="mover_select")
            with go_c:
                if chosen_mover and st.button(
                        "→", key="mover_go"):
                    go_detail(chosen_mover)
                    st.rerun()
            st.download_button(
                "📥 CSV", data=make_csv(mdf),
                file_name="movers.csv", mime="text/csv")

    else:
        # ── Рейтинг ──
        top_movers = tracker.get_top_movers(20)
        if top_movers:
            for i, (sym, cnt) in enumerate(top_movers):
                rc = st.columns([3, 1, 1])
                medal = "🥇" if i == 0 else ("🥈" if i == 1 else ("🥉" if i == 2 else f"{i+1}."))
                rc[0].markdown(
                    f"**{medal} {sym}** — {cnt} переставок")
                if rc[1].button("🔍", key=f"rank_{sym}"):
                    go_detail(sym)
                    st.rerun()
                if rc[2].button("⭐", key=f"fav_{sym}"):
                    st.session_state.favorites.add(sym)
                    st.rerun()
            fig = go.Figure(go.Bar(
                x=[x[0] for x in top_movers],
                y=[x[1] for x in top_movers],
                marker_color=PRICE_LINE))
            fig.update_layout(
                template="plotly_dark", height=280,
                title="Топ переставляшей",
                margin=dict(l=40, r=20, t=40, b=60))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Накопи данные — запусти несколько сканов")

st.caption("MEXC Scanner v5.0")

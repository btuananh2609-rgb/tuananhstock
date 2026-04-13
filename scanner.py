"""
scanner.py — Lõi phân tích Minervini + Robert Miner
Lấy dữ liệu từ VCI (vnstock), tính toán toàn bộ tín hiệu.
"""

import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional

log = logging.getLogger(__name__)

# ── Danh sách mã mặc định để quét toàn thị trường ──────────────────
# Bạn có thể mở rộng danh sách này hoặc lấy tự động từ vnstock
DEFAULT_WATCHLIST = [
    # Ngân hàng
    "VCB", "BID", "CTG", "MBB", "TCB", "ACB", "VPB", "HDB", "LPB", "STB",
    # Thép / Vật liệu
    "HPG", "HSG", "NKG", "TVN",
    # Công nghệ
    "FPT", "CMG", "ELC",
    # Tiêu dùng / Bán lẻ
    "MWG", "PNJ", "VNM", "SAB", "MSN",
    # Bất động sản
    "VHM", "NVL", "DXG", "KDH", "PDR",
    # Năng lượng / Dầu khí
    "GAS", "PLX", "PVD", "PVS",
    # Logistics / Cảng
    "GMD", "HAH", "VSC",
    # Hàng không / Du lịch
    "HVN", "VJC",
    # Chứng khoán
    "SSI", "VND", "HCM", "VCI", "MBS",
    # Dược / Y tế
    "DHC", "DMC", "IMP",
]


# ──────────────────────────────────────────────────────────────────────
# DATA FETCHING (vnstock)
# ──────────────────────────────────────────────────────────────────────

def fetch_history(ticker: str, days: int = 300) -> Optional[pd.DataFrame]:
    """Lấy lịch sử giá từ VCI qua vnstock."""
    try:
        from vnstock import Quote
        end = datetime.today().strftime("%Y-%m-%d")
        start = (datetime.today() - timedelta(days=days)).strftime("%Y-%m-%d")
        quote = Quote(symbol=ticker, source="VCI")
        df = quote.history(start=start, end=end, interval="1D")
        if df is None or df.empty:
            return None
        # Chuẩn hoá tên cột
        df.columns = [c.lower() for c in df.columns]
        rename_map = {"tradingdate": "date", "datetime": "date", "time": "date"}
        for old, new in rename_map.items():
            if old in df.columns:
                df.rename(columns={old: new}, inplace=True)
        df = df.sort_values("date").reset_index(drop=True)
        # Đảm bảo có đủ cột
        for col in ["open", "high", "low", "close", "volume"]:
            if col not in df.columns:
                return None
        return df
    except Exception as e:
        log.warning(f"Lỗi fetch {ticker}: {e}")
        return None


def fetch_price_board(tickers: list) -> dict:
    """Lấy bảng giá nhiều mã cùng lúc (nhanh hơn gọi từng mã)."""
    try:
        from vnstock import Trading
        board = Trading(source="VCI").price_board(tickers)
        if board is None or board.empty:
            return {}
        board.columns = [c.lower() for c in board.columns]
        result = {}
        for _, row in board.iterrows():
            ticker = str(row.get("ticker", row.get("symbol", ""))).upper()
            if ticker:
                result[ticker] = row.to_dict()
        return result
    except Exception as e:
        log.warning(f"Lỗi price board: {e}")
        return {}


def fetch_market_index() -> dict:
    """Lấy VN-Index, VN30."""
    try:
        from vnstock import Quote
        indices = {}
        for symbol, label in [("VNINDEX", "VN-Index"), ("VN30", "VN30")]:
            try:
                end = datetime.today().strftime("%Y-%m-%d")
                start = (datetime.today() - timedelta(days=5)).strftime("%Y-%m-%d")
                q = Quote(symbol=symbol, source="VCI")
                df = q.history(start=start, end=end, interval="1D")
                if df is not None and not df.empty:
                    df.columns = [c.lower() for c in df.columns]
                    last = df.iloc[-1]
                    prev = df.iloc[-2] if len(df) > 1 else last
                    close = float(last.get("close", 0))
                    prev_close = float(prev.get("close", close))
                    chg_pct = round((close - prev_close) / prev_close * 100, 2) if prev_close else 0
                    indices[label] = {
                        "value": round(close, 2),
                        "change_pct": chg_pct,
                        "volume": int(last.get("volume", 0)),
                    }
            except Exception:
                pass
        return indices
    except Exception as e:
        log.error(f"Lỗi market index: {e}")
        return {}


# ──────────────────────────────────────────────────────────────────────
# TECHNICAL INDICATORS
# ──────────────────────────────────────────────────────────────────────

def compute_ma(series: pd.Series, period: int) -> pd.Series:
    return series.rolling(window=period, min_periods=period).mean()

def compute_rsi(series: pd.Series, period: int = 14) -> float:
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return round(float(rsi.iloc[-1]), 1) if not rsi.empty else 50.0

def compute_atr(df: pd.DataFrame, period: int = 14) -> float:
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift()).abs(),
        (df["low"] - df["close"].shift()).abs(),
    ], axis=1).max(axis=1)
    return float(tr.rolling(period).mean().iloc[-1])

def compute_rs_rating(close_series: pd.Series, market_series: Optional[pd.Series] = None) -> int:
    """RS Rating đơn giản: % tăng 12 tháng so với 3 tháng gần nhất, scale 1-99."""
    try:
        n = len(close_series)
        if n < 63:
            return 50
        p12m = float(close_series.iloc[-252]) if n >= 252 else float(close_series.iloc[0])
        p3m  = float(close_series.iloc[-63])
        p1m  = float(close_series.iloc[-21])
        cur  = float(close_series.iloc[-1])
        perf = (cur - p12m) / p12m * 0.4 + \
               (cur - p3m)  / p3m  * 0.3 + \
               (cur - p1m)  / p1m  * 0.3
        # Chuyển về thang điểm 1–99 (clamp)
        score = int(min(99, max(1, 50 + perf * 100)))
        return score
    except Exception:
        return 50


# ──────────────────────────────────────────────────────────────────────
# MINERVINI CRITERIA
# ──────────────────────────────────────────────────────────────────────

def check_stage2(df: pd.DataFrame) -> dict:
    """
    Stage 2 Uptrend theo Minervini:
    Giá > MA50 > MA150 > MA200, MA200 dốc lên, 52W High mạnh.
    """
    close = df["close"]
    ma50  = compute_ma(close, 50)
    ma150 = compute_ma(close, 150)
    ma200 = compute_ma(close, 200)

    if ma200.isna().all() or ma150.isna().all() or ma50.isna().all():
        return {"pass": False, "reason": "Không đủ dữ liệu MA"}

    cur       = float(close.iloc[-1])
    m50_cur   = float(ma50.iloc[-1])
    m150_cur  = float(ma150.iloc[-1])
    m200_cur  = float(ma200.iloc[-1])
    m200_1m   = float(ma200.iloc[-21]) if len(ma200) > 21 else m200_cur

    price_above_ma50  = cur > m50_cur
    price_above_ma150 = cur > m150_cur
    price_above_ma200 = cur > m200_cur
    ma50_above_ma150  = m50_cur > m150_cur
    ma150_above_ma200 = m150_cur > m200_cur
    ma200_trending_up = m200_cur > m200_1m

    # 52W High
    high_52w = float(close.rolling(252, min_periods=63).max().iloc[-1])
    pct_from_high = (cur - high_52w) / high_52w * 100

    passed = all([
        price_above_ma50, price_above_ma150, price_above_ma200,
        ma50_above_ma150, ma150_above_ma200, ma200_trending_up,
    ])

    return {
        "pass": passed,
        "price_above_ma50": price_above_ma50,
        "price_above_ma150": price_above_ma150,
        "price_above_ma200": price_above_ma200,
        "ma50_above_ma150": ma50_above_ma150,
        "ma150_above_ma200": ma150_above_ma200,
        "ma200_trending_up": ma200_trending_up,
        "ma50": round(m50_cur),
        "ma150": round(m150_cur),
        "ma200": round(m200_cur),
        "high_52w": round(high_52w),
        "pct_from_52w_high": round(pct_from_high, 1),
    }


def check_vcp(df: pd.DataFrame) -> dict:
    """
    VCP — Volatility Contraction Pattern (Minervini):
    Tìm 2–4 lần co hẹp biên độ trong 3–65 tuần, volume giảm dần.
    """
    close  = df["close"]
    volume = df["volume"]

    try:
        # Tính biên độ theo từng tuần (5 phiên)
        weekly_ranges = []
        weekly_vols   = []
        for i in range(0, min(len(df), 65*5), 5):
            chunk = df.iloc[-(i+5):-(i) or None]
            if len(chunk) < 3:
                break
            rng = float((chunk["high"].max() - chunk["low"].min()) / chunk["close"].mean() * 100)
            vol = float(chunk["volume"].mean())
            weekly_ranges.insert(0, rng)
            weekly_vols.insert(0, vol)

        if len(weekly_ranges) < 4:
            return {"pass": False, "contractions": 0, "reason": "Không đủ dữ liệu"}

        # Đếm số lần co hẹp liên tiếp (mỗi tuần nhỏ hơn tuần trước)
        contractions = 0
        for i in range(1, len(weekly_ranges)):
            if weekly_ranges[i] < weekly_ranges[i-1] * 0.8:
                contractions += 1
            else:
                contractions = 0   # reset nếu không liên tiếp

        vol_declining = (len(weekly_vols) >= 3 and
                        weekly_vols[-1] < weekly_vols[-2] < weekly_vols[-3])

        # VCP hợp lệ: ≥ 2 lần co hẹp, volume giảm
        passed = contractions >= 2 and vol_declining

        return {
            "pass": passed,
            "contractions": contractions,
            "vol_declining": vol_declining,
            "latest_range_pct": round(weekly_ranges[-1], 2),
        }
    except Exception as e:
        return {"pass": False, "contractions": 0, "reason": str(e)}


def check_breakout(df: pd.DataFrame) -> dict:
    """
    Breakout Minervini:
    Giá phiên cuối phá vỡ đỉnh 52 tuần + volume đột biến.
    """
    close  = df["close"]
    volume = df["volume"]
    high   = df["high"]

    pivot_52w = float(high.rolling(252, min_periods=63).max().iloc[-2])   # đỉnh TỚI TRƯỚC ngày hôm nay
    cur_close = float(close.iloc[-1])
    cur_high  = float(high.iloc[-1])
    cur_vol   = float(volume.iloc[-1])
    avg_vol50 = float(volume.rolling(50).mean().iloc[-1])

    price_breakout = cur_high > pivot_52w
    vol_ratio      = cur_vol / avg_vol50 if avg_vol50 > 0 else 0
    vol_surge      = vol_ratio >= 1.5

    return {
        "pass": price_breakout and vol_surge,
        "price_breakout": price_breakout,
        "vol_surge": vol_surge,
        "vol_ratio": round(vol_ratio, 2),
        "pivot_52w": round(pivot_52w),
        "cur_price": round(cur_close),
    }


def check_volume_surge(df: pd.DataFrame) -> dict:
    """Volume tăng > 150% so với TB 20 phiên."""
    vol = df["volume"]
    cur = float(vol.iloc[-1])
    avg = float(vol.rolling(20).mean().iloc[-2])
    ratio = cur / avg if avg > 0 else 0
    return {
        "pass": ratio >= 1.5,
        "vol_ratio": round(ratio, 2),
        "avg_vol_20": round(avg),
    }


# ──────────────────────────────────────────────────────────────────────
# ROBERT MINER CRITERIA (DT Oscillator + Fibonacci)
# ──────────────────────────────────────────────────────────────────────

def compute_fibonacci_levels(df: pd.DataFrame, lookback: int = 63) -> dict:
    """
    Tính các mức Fibonacci Retracement từ đáy–đỉnh gần nhất.
    """
    try:
        window = df.tail(lookback)
        swing_high = float(window["high"].max())
        swing_low  = float(window["low"].min())
        diff = swing_high - swing_low
        if diff == 0:
            return {}

        levels = {
            "swing_high": round(swing_high),
            "swing_low":  round(swing_low),
            "fib_23.6":   round(swing_high - 0.236 * diff),
            "fib_38.2":   round(swing_high - 0.382 * diff),
            "fib_50.0":   round(swing_high - 0.500 * diff),
            "fib_61.8":   round(swing_high - 0.618 * diff),
            "fib_78.6":   round(swing_high - 0.786 * diff),
        }
        return levels
    except Exception:
        return {}


def check_fibonacci_signal(df: pd.DataFrame) -> dict:
    """
    Tín hiệu Fibonacci Miner: giá đang gần (±2%) một mức Fib quan trọng.
    """
    cur = float(df["close"].iloc[-1])
    levels = compute_fibonacci_levels(df)
    if not levels:
        return {"pass": False}

    key_levels = ["fib_38.2", "fib_50.0", "fib_61.8"]
    hit_level  = None
    min_dist   = float("inf")

    for key in key_levels:
        if key not in levels:
            continue
        level_val = levels[key]
        dist_pct  = abs(cur - level_val) / level_val * 100
        if dist_pct < min_dist:
            min_dist  = dist_pct
            hit_level = key

    near_fib = min_dist <= 2.0   # trong vùng ±2%

    return {
        "pass": near_fib,
        "nearest_level": hit_level,
        "distance_pct": round(min_dist, 2),
        "levels": levels,
    }


def check_elliott_signal(df: pd.DataFrame) -> dict:
    """
    Nhận diện sóng Elliott đơn giản:
    - Tìm 5 bước sóng lên (1-2-3-4-5) bằng ZigZag
    - Hoặc 3 sóng điều chỉnh (A-B-C)
    Đây là xấp xỉ — dùng như bộ lọc sơ bộ, không thay thế phân tích chuyên sâu.
    """
    try:
        close = df["close"].values
        n = len(close)
        if n < 30:
            return {"pass": False, "wave": None}

        # ZigZag: tìm các đỉnh/đáy cục bộ (cửa sổ 5 phiên)
        pivots = []
        for i in range(5, n - 5):
            window = close[i-5:i+6]
            if close[i] == max(window):
                pivots.append(("H", i, close[i]))
            elif close[i] == min(window):
                pivots.append(("L", i, close[i]))

        if len(pivots) < 5:
            return {"pass": False, "wave": None}

        # Lấy 5 pivot gần nhất
        last5 = pivots[-5:]
        types = [p[0] for p in last5]

        # Pattern 5 sóng tăng: L-H-L-H-L hoặc H-L-H-L-H kết thúc bằng đỉnh
        is_impulse_up = (types == ["L","H","L","H","H"] or
                         types == ["L","H","L","H","L"])
        # Pattern sóng điều chỉnh kết thúc (sóng C xong): H-L-H-L (đáy cuối)
        is_correction_end = types[-2:] == ["H","L"]

        signal = is_impulse_up or is_correction_end
        wave_type = "Sóng xung (đang bắt đầu)" if is_impulse_up else \
                    "Sóng điều chỉnh kết thúc" if is_correction_end else None

        return {
            "pass": signal,
            "wave": wave_type,
            "pivot_count": len(pivots),
        }
    except Exception as e:
        return {"pass": False, "wave": None, "error": str(e)}


# ──────────────────────────────────────────────────────────────────────
# MAIN ANALYZER
# ──────────────────────────────────────────────────────────────────────

def analyze_ticker(ticker: str, df: pd.DataFrame) -> Optional[dict]:
    """
    Phân tích đầy đủ 1 mã: lấy dữ liệu + chạy tất cả bộ lọc.
    Trả về dict chuẩn để API phục vụ web app.
    """
    if df is None or len(df) < 50:
        return None

    try:
        close  = df["close"]
        volume = df["volume"]
        cur    = float(close.iloc[-1])
        prev   = float(close.iloc[-2]) if len(close) > 1 else cur
        chg    = round((cur - prev) / prev * 100, 2) if prev else 0.0

        # Chạy tất cả bộ lọc
        stage2   = check_stage2(df)
        vcp      = check_vcp(df)
        breakout = check_breakout(df)
        vol_s    = check_volume_surge(df)
        fib      = check_fibonacci_signal(df)
        elliott  = check_elliott_signal(df)
        rs       = compute_rs_rating(close)
        rsi_val  = compute_rsi(close)

        # Tập hợp tín hiệu active
        signals = []
        if breakout["pass"]:  signals.append("breakout")
        if vcp["pass"]:       signals.append("vcp")
        if stage2["pass"]:    signals.append("stage2")
        if vol_s["pass"]:     signals.append("volume")
        if fib["pass"]:       signals.append("fib")
        if elliott["pass"]:   signals.append("elliott")

        if not signals:
            return None   # Không có tín hiệu nào → bỏ qua

        # MA values
        ma50  = float(compute_ma(close, 50).iloc[-1]) if len(close) >= 50 else None
        ma150 = float(compute_ma(close, 150).iloc[-1]) if len(close) >= 150 else None
        ma200 = float(compute_ma(close, 200).iloc[-1]) if len(close) >= 200 else None

        # Spark (30 ngày gần nhất)
        spark = [round(float(v) / 1000, 1) for v in close.tail(30).tolist()]

        return {
            "ticker":     ticker,
            "price":      round(cur),
            "change_pct": chg,
            "signals":    signals,
            "rs_rating":  rs,
            "rsi":        rsi_val,
            "vol_ratio":  vol_s.get("vol_ratio", 1.0),
            "ma": {
                "ma50":  round(ma50)  if ma50  else None,
                "ma150": round(ma150) if ma150 else None,
                "ma200": round(ma200) if ma200 else None,
                "above_ma50":  cur > ma50  if ma50  else False,
                "above_ma150": cur > ma150 if ma150 else False,
                "above_ma200": cur > ma200 if ma200 else False,
            },
            "analysis": {
                "stage2":   stage2,
                "vcp":      vcp,
                "breakout": breakout,
                "volume":   vol_s,
                "fib":      fib,
                "elliott":  elliott,
            },
            "spark":       spark,
            "updated_at":  datetime.now().isoformat(),
        }
    except Exception as e:
        log.warning(f"Lỗi phân tích {ticker}: {e}")
        return None


# ──────────────────────────────────────────────────────────────────────
# PUBLIC FUNCTIONS (gọi từ main.py)
# ──────────────────────────────────────────────────────────────────────

def run_full_scan(tickers: list = None) -> dict:
    """Quét toàn bộ danh sách mã, trả về kết quả đã lọc."""
    if tickers is None:
        tickers = DEFAULT_WATCHLIST

    log.info(f"Quét {len(tickers)} mã...")
    results = []
    errors  = []

    for ticker in tickers:
        try:
            df = fetch_history(ticker)
            if df is not None:
                res = analyze_ticker(ticker, df)
                if res:
                    results.append(res)
        except Exception as e:
            errors.append({"ticker": ticker, "error": str(e)})
            log.warning(f"Bỏ qua {ticker}: {e}")

    # Thống kê tín hiệu
    sig_counts = {}
    for r in results:
        for s in r.get("signals", []):
            sig_counts[s] = sig_counts.get(s, 0) + 1

    return {
        "stocks": results,
        "summary": {
            "total_scanned":  len(tickers),
            "total_signals":  len(results),
            "signal_counts":  sig_counts,
            "scan_errors":    len(errors),
            "scanned_at":     datetime.now().isoformat(),
        },
        "errors": errors,
    }


def analyze_single(ticker: str) -> dict:
    """Phân tích 1 mã đơn lẻ."""
    df = fetch_history(ticker)
    if df is None:
        raise ValueError(f"Không lấy được dữ liệu cho {ticker}")
    result = analyze_ticker(ticker, df)
    if result is None:
        # Trả về thông tin cơ bản dù không có tín hiệu
        cur = float(df["close"].iloc[-1])
        prev = float(df["close"].iloc[-2])
        return {
            "ticker": ticker,
            "price": round(cur),
            "change_pct": round((cur-prev)/prev*100, 2),
            "signals": [],
            "rs_rating": compute_rs_rating(df["close"]),
            "message": "Không có tín hiệu Minervini/Miner hiện tại",
        }
    return result


def analyze_watchlist_stocks(tickers: list) -> dict:
    """Phân tích danh sách watchlist tùy chỉnh của người dùng."""
    results = []
    for ticker in tickers:
        try:
            df = fetch_history(ticker)
            if df is not None:
                res = analyze_ticker(ticker, df)
                if res is None:
                    # Trả về dữ liệu cơ bản nếu không có tín hiệu
                    cur  = float(df["close"].iloc[-1])
                    prev = float(df["close"].iloc[-2])
                    res  = {
                        "ticker": ticker,
                        "price": round(cur),
                        "change_pct": round((cur-prev)/prev*100, 2),
                        "signals": [],
                        "rs_rating": compute_rs_rating(df["close"]),
                    }
                results.append(res)
        except Exception as e:
            results.append({"ticker": ticker, "error": str(e), "signals": []})

    return {
        "stocks": results,
        "analyzed_at": datetime.now().isoformat(),
    }


def get_market_overview() -> dict:
    """VN-Index, VN30 + tóm tắt thị trường."""
    indices = fetch_market_index()
    return {
        "indices": indices,
        "updated_at": datetime.now().isoformat(),
    }

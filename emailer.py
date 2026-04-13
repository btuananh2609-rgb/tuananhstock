"""
emailer.py — Gửi email cảnh báo tín hiệu qua SendGrid
Tự động gửi khi phát hiện tín hiệu mới sau mỗi lần quét.
"""

import os
import json
import logging
from datetime import datetime
from typing import Optional

log = logging.getLogger(__name__)

# ── Cấu hình ──────────────────────────────────
SENDGRID_API_KEY  = os.environ.get("SENDGRID_API_KEY", "")
ALERT_EMAIL_TO    = os.environ.get("ALERT_EMAIL_TO", "")      # Email nhận
ALERT_EMAIL_FROM  = os.environ.get("ALERT_EMAIL_FROM", "vnscan@tuananhstock.onrender.com")

SIG_LABELS = {
    "breakout": "⚡ Breakout",
    "vcp":      "🔵 VCP",
    "stage2":   "🟡 Stage 2",
    "volume":   "🔴 Volume Surge",
    "fib":      "🟣 Fibonacci",
    "elliott":  "🟠 Elliott",
    "stoch":    "📈 Stoch %K↑%D",
}

SIG_COLORS = {
    "breakout": "#00e5a0",
    "vcp":      "#0095ff",
    "stage2":   "#ffb800",
    "volume":   "#ff4757",
    "fib":      "#c084fc",
    "elliott":  "#fb923c",
    "stoch":    "#22d3ee",
}


# ── HTML Email Template ───────────────────────

def build_email_html(stocks: list, scan_time: str) -> str:
    """Tạo nội dung email HTML đẹp cho danh sách tín hiệu."""

    def sig_badges(sigs):
        badges = ""
        for s in sigs:
            color = SIG_COLORS.get(s, "#888")
            label = SIG_LABELS.get(s, s)
            badges += f'''<span style="background:{color}22;color:{color};border:1px solid {color}44;
                border-radius:20px;padding:3px 10px;font-size:11px;margin:2px;display:inline-block;">{label}</span>'''
        return badges

    def ma_pill(label, ok):
        color = "#00e5a0" if ok else "#ff4757"
        return f'<span style="background:{color}22;color:{color};border-radius:4px;padding:2px 8px;font-size:10px;margin:1px;">{label} {"✓" if ok else "✗"}</span>'

    stock_cards = ""
    for s in stocks:
        ma = s.get("ma", [False, False, False])
        chg = s.get("change_pct", s.get("chg", 0))
        chg_color = "#00e5a0" if chg >= 0 else "#ff4757"
        chg_str = f"+{chg:.2f}%" if chg >= 0 else f"{chg:.2f}%"
        price = s.get("price", 0)
        rs = s.get("rs_rating", s.get("rs", 0))

        level = s.get("_signal_level", "🔵 Tốt")
        desc  = s.get("_signal_desc", "")

        stock_cards += f"""
        <div style="background:#111418;border:1px solid rgba(255,255,255,0.08);border-radius:12px;
                    padding:16px;margin-bottom:12px;border-left:3px solid {SIG_COLORS.get(s.get('signals',s.get('sigs',['']))[0], '#00e5a0')};">
          <div style="margin-bottom:8px;">
            <span style="background:rgba(0,229,160,0.1);color:#00e5a0;border:1px solid rgba(0,229,160,0.25);
                         border-radius:20px;padding:3px 12px;font-size:11px;font-weight:600;">{level}</span>
            <span style="color:#636b7a;font-size:11px;margin-left:8px;">{desc}</span>
          </div>
          <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px;">
            <div>
              <div style="font-family:'Courier New',monospace;font-size:22px;font-weight:800;color:#e8eaf0;">{s.get('ticker','')}</div>
              <div style="font-size:11px;color:#636b7a;margin-top:2px;">{s.get('name', s.get('ticker',''))} • {s.get('sector','HOSE')}</div>
            </div>
            <div style="text-align:right;">
              <div style="font-size:18px;font-weight:600;color:{chg_color};">{price:,.0f} đ</div>
              <div style="font-size:12px;color:{chg_color};">{chg_str}</div>
            </div>
          </div>
          <div style="margin-bottom:10px;">{sig_badges(s.get('signals', s.get('sigs', [])))}</div>
          <div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:10px;">
            {ma_pill('MA50', ma[0] if len(ma)>0 else False)}
            {ma_pill('MA150', ma[1] if len(ma)>1 else False)}
            {ma_pill('MA200', ma[2] if len(ma)>2 else False)}
            <span style="background:rgba(255,255,255,0.05);color:#888;border-radius:4px;padding:2px 8px;font-size:10px;margin:1px;">RS {rs}</span>
          </div>
          <a href="https://tuananhstock.onrender.com/app" 
             style="display:inline-block;background:#00e5a0;color:#000;border-radius:6px;
                    padding:6px 14px;font-size:11px;font-weight:700;text-decoration:none;">
            Xem chi tiết →
          </a>
        </div>"""

    vn_time = datetime.strptime(scan_time[:19], "%Y-%m-%dT%H:%M:%S") if scan_time else datetime.now()
    time_str = vn_time.strftime("%H:%M %d/%m/%Y")

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#0a0c0f;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
  <div style="max-width:560px;margin:0 auto;padding:20px;">

    <!-- Header -->
    <div style="background:#111418;border-radius:16px;padding:20px 24px;margin-bottom:16px;
                border:1px solid rgba(255,255,255,0.07);">
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <div>
          <span style="font-size:20px;font-weight:800;color:#e8eaf0;">VN<span style="color:#00e5a0;">Scan</span></span>
          <div style="font-size:11px;color:#636b7a;margin-top:4px;">Cảnh báo tín hiệu tự động</div>
        </div>
        <div style="text-align:right;">
          <div style="font-size:11px;color:#636b7a;">Cập nhật lúc</div>
          <div style="font-size:13px;font-weight:600;color:#00e5a0;">{time_str}</div>
        </div>
      </div>
    </div>

    <!-- Summary -->
    <div style="background:#00e5a022;border:1px solid #00e5a044;border-radius:10px;
                padding:12px 16px;margin-bottom:16px;">
      <div style="font-size:13px;color:#00e5a0;font-weight:600;">
        🔍 Phát hiện <b>{len(stocks)}</b> mã có tín hiệu trong phiên quét vừa rồi
      </div>
    </div>

    <!-- Stock Cards -->
    {stock_cards}

    <!-- Footer -->
    <div style="text-align:center;margin-top:20px;padding:16px;border-top:1px solid rgba(255,255,255,0.07);">
      <a href="https://tuananhstock.onrender.com/app"
         style="display:inline-block;background:#00e5a0;color:#000;border-radius:8px;
                padding:10px 24px;font-weight:700;text-decoration:none;font-size:13px;">
        Mở VNScan →
      </a>
      <div style="font-size:10px;color:#636b7a;margin-top:12px;">
        Email này được gửi tự động từ VNScan mỗi 15 phút khi có tín hiệu.<br>
        Đây là công cụ hỗ trợ phân tích, không phải tư vấn đầu tư.
      </div>
    </div>

  </div>
</body>
</html>"""


# ── Gửi email qua SendGrid ────────────────────

def send_alert_email(stocks: list, scan_time: str) -> bool:
    """
    Gửi email cảnh báo qua SendGrid API.
    Trả về True nếu gửi thành công.
    """
    if not SENDGRID_API_KEY:
        log.warning("Chưa cấu hình SENDGRID_API_KEY — bỏ qua gửi email")
        return False
    if not ALERT_EMAIL_TO:
        log.warning("Chưa cấu hình ALERT_EMAIL_TO — bỏ qua gửi email")
        return False
    if not stocks:
        log.info("Không có tín hiệu mới — không gửi email")
        return False

    try:
        import httpx

        # Xác định mức độ mạnh nhất trong batch
        has_strongest = any(s.get("_signal_level","").startswith("⭐") for s in stocks)
        level_label = "⭐ Setup hoàn hảo" if has_strongest else "🔵 Tín hiệu tốt"

        subject = f"{level_label} — {len(stocks)} mã: {', '.join([s.get('ticker','') for s in stocks[:3]])}{'...' if len(stocks)>3 else ''} | VNScan"

        payload = {
            "personalizations": [{
                "to": [{"email": ALERT_EMAIL_TO}],
                "subject": subject
            }],
            "from": {"email": ALERT_EMAIL_FROM, "name": "VNScan Alert"},
            "content": [{
                "type": "text/html",
                "value": build_email_html(stocks, scan_time)
            }]
        }

        response = httpx.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={
                "Authorization": f"Bearer {SENDGRID_API_KEY}",
                "Content-Type": "application/json"
            },
            json=payload,
            timeout=15
        )

        if response.status_code == 202:
            log.info(f"✅ Email đã gửi đến {ALERT_EMAIL_TO} — {len(stocks)} tín hiệu")
            return True
        else:
            log.error(f"❌ SendGrid lỗi {response.status_code}: {response.text}")
            return False

    except Exception as e:
        log.error(f"❌ Lỗi gửi email: {e}")
        return False


# ── Theo dõi tín hiệu mới (tránh gửi trùng) ──

_last_sent_tickers: set = set()


def is_strong_signal(stock: dict) -> tuple:
    """
    Kiểm tra tín hiệu theo logic kết hợp Minervini + Miner + Stochastic.
    Trả về (đủ điều kiện gửi email, mức độ, mô tả).
    """
    sigs = set(stock.get("signals", stock.get("sigs", [])))
    stoch_info = stock.get("analysis", {}).get("stoch", {})
    stoch_zone = stoch_info.get("zone", "")

    # ⭐ MẠNH NHẤT: Breakout + Stage 2 + Volume (cả 3)
    if {"breakout", "stage2", "volume"}.issubset(sigs):
        return True, "⭐ Mạnh nhất", "Breakout + Stage 2 + Volume Surge"

    # ⭐ Stochastic quá bán + Stage 2 (rất có giá trị)
    if "stoch" in sigs and "stage2" in sigs and stoch_zone == "Quá bán (<20)":
        return True, "⭐ Mạnh nhất", "Stoch %K↑%D vùng quá bán + Stage 2"

    # 🔵 TỐT: các combo 2 tín hiệu có ý nghĩa
    if {"breakout", "stage2"}.issubset(sigs):
        return True, "🔵 Tốt", "Breakout + Stage 2"

    if {"vcp", "stage2"}.issubset(sigs):
        return True, "🔵 Tốt", "VCP + Stage 2 (chuẩn bị breakout)"

    if {"fib", "elliott"}.issubset(sigs):
        return True, "🔵 Tốt", "Fibonacci + Elliott (Miner xác nhận)"

    if {"breakout", "volume"}.issubset(sigs):
        return True, "🔵 Tốt", "Breakout + Volume Surge"

    if {"vcp", "fib"}.issubset(sigs):
        return True, "🔵 Tốt", "VCP + Fibonacci"

    if {"stage2", "volume"}.issubset(sigs):
        return True, "🔵 Tốt", "Stage 2 + Volume Surge"

    # 🔵 Stochastic độc lập — gửi email riêng (vùng quá bán hoặc trung lập)
    if "stoch" in sigs and stoch_zone == "Quá bán (<20)":
        k = stoch_info.get("k", 0)
        d = stoch_info.get("d", 0)
        return True, "🔵 Tốt", f"Stoch(8,5,3) %K({k}) cắt lên %D({d}) — Vùng quá bán"

    if "stoch" in sigs and stoch_zone == "Trung lập (20–80)":
        k = stoch_info.get("k", 0)
        d = stoch_info.get("d", 0)
        return True, "🔵 Tốt", f"Stoch(8,5,3) %K({k}) cắt lên %D({d}) — Vùng trung lập"

    # 🟡 ĐƠN LẺ: chỉ 1 tín hiệu khác → không gửi email
    return False, "🟡 Đơn lẻ", "Chưa đủ xác nhận"


def filter_new_signals(stocks: list) -> list:
    """
    Chỉ lấy các mã:
    1. Có tín hiệu kết hợp đủ mạnh (tối thiểu 2 tín hiệu có nghĩa)
    2. Chưa gửi email trong phiên này (tránh gửi trùng)
    """
    global _last_sent_tickers
    new_stocks = []
    for s in stocks:
        ticker = s.get("ticker", "")
        sigs   = tuple(sorted(s.get("signals", s.get("sigs", []))))
        key    = f"{ticker}:{sigs}"

        # Kiểm tra logic kết hợp
        qualified, level, desc = is_strong_signal(s)
        if not qualified:
            continue  # bỏ qua tín hiệu đơn lẻ

        # Kiểm tra chưa gửi trong phiên
        if key not in _last_sent_tickers:
            s["_signal_level"] = level
            s["_signal_desc"]  = desc
            new_stocks.append(s)
            _last_sent_tickers.add(key)

    return new_stocks


def reset_daily_tracker():
    """Gọi mỗi sáng 8:00 để reset danh sách tín hiệu đã gửi."""
    global _last_sent_tickers
    _last_sent_tickers = set()
    log.info("🔄 Reset tracker tín hiệu hàng ngày")

"""
VNScan Backend — FastAPI server
Lấy dữ liệu thật từ VCI/TCBS qua vnstock, phân tích theo
tiêu chí Minervini + Robert Miner, phục vụ web app qua REST API.
"""

from fastapi import FastAPI, BackgroundTasks, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
import asyncio, logging, os
from datetime import datetime, timedelta
from typing import Optional
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from scanner import run_full_scan, get_market_overview
from cache import cache

# ──────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="VNScan API", version="1.0.0")

# Cho phép web app gọi từ bất kỳ domain nào
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

scheduler = AsyncIOScheduler(timezone="Asia/Ho_Chi_Minh")


# ──────────────────────────────────────────────
# STARTUP & SHUTDOWN
# ──────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    log.info("🚀 VNScan backend khởi động")
    # Quét ngay lần đầu khi server start
    asyncio.create_task(scheduled_scan())
    # Cron: mỗi 15 phút trong giờ giao dịch (9:00–15:10 thứ 2–6)
    scheduler.add_job(scheduled_scan, "cron",
                      day_of_week="mon-fri",
                      hour="9-15", minute="*/15")
    # Quét thêm lúc 8:45 để chuẩn bị trước giờ mở cửa
    scheduler.add_job(scheduled_scan, "cron",
                      day_of_week="mon-fri",
                      hour=8, minute=45)
    scheduler.start()


@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()
    log.info("Backend đã dừng")


async def scheduled_scan():
    log.info("⏱  Bắt đầu quét tự động...")
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, run_full_scan)
        cache.set("scan_result", result)
        cache.set("last_scan", datetime.now().isoformat())
        log.info(f"✅ Quét xong: {result['summary']['total_signals']} tín hiệu")
    except Exception as e:
        log.error(f"❌ Lỗi quét: {e}")


# ──────────────────────────────────────────────
# ENDPOINTS
# ──────────────────────────────────────────────

@app.get("/")
def root():
    return {"service": "VNScan API", "status": "running",
            "last_scan": cache.get("last_scan")}


@app.get("/api/scan")
def get_scan_result(
    filter: Optional[str] = Query(None, description="all|breakout|vcp|stage2|volume|fib|elliott"),
    min_rs: Optional[int] = Query(None, description="Lọc RS Rating tối thiểu (0-100)")
):
    """Trả về kết quả quét mới nhất từ cache."""
    result = cache.get("scan_result")
    if not result:
        raise HTTPException(status_code=503, detail="Chưa có dữ liệu — đang quét lần đầu, thử lại sau 30 giây")

    stocks = result.get("stocks", [])

    # Lọc theo loại tín hiệu
    if filter and filter != "all":
        stocks = [s for s in stocks if filter in s.get("signals", [])]

    # Lọc theo RS Rating
    if min_rs is not None:
        stocks = [s for s in stocks if s.get("rs_rating", 0) >= min_rs]

    # Sắp xếp: ưu tiên nhiều tín hiệu + RS cao
    stocks = sorted(stocks, key=lambda s: (len(s["signals"]), s.get("rs_rating", 0)), reverse=True)

    return {
        "stocks": stocks,
        "summary": result["summary"],
        "last_scan": cache.get("last_scan"),
        "filter_applied": filter or "all",
    }


@app.get("/api/market")
def get_market():
    """VN-Index, VN30, KLGD thị trường."""
    cached = cache.get("market")
    if cached:
        return cached
    try:
        data = get_market_overview()
        cache.set("market", data, ttl=60)   # cache 60 giây
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stock/{ticker}")
def get_stock_detail(ticker: str):
    """Chi tiết phân tích 1 mã cụ thể."""
    ticker = ticker.upper()
    result = cache.get("scan_result")
    if result:
        for s in result.get("stocks", []):
            if s["ticker"] == ticker:
                return s
    # Nếu không có trong cache, phân tích riêng
    try:
        from scanner import analyze_single
        loop = asyncio.new_event_loop()
        data = analyze_single(ticker)
        return data
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Không tìm thấy dữ liệu cho {ticker}: {e}")


@app.post("/api/scan/trigger")
async def trigger_manual_scan(background_tasks: BackgroundTasks):
    """Kích hoạt quét thủ công từ web app."""
    background_tasks.add_task(scheduled_scan)
    return {"message": "Đang quét... Gọi lại /api/scan sau 20 giây"}


@app.get("/api/watchlist/analyze")
def analyze_watchlist(tickers: str = Query(..., description="Danh sách mã, cách nhau bởi dấu phẩy. VD: FPT,TCB,HPG")):
    """Phân tích nhanh danh sách watchlist của người dùng."""
    ticker_list = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    if not ticker_list:
        raise HTTPException(status_code=400, detail="Danh sách tickers rỗng")
    if len(ticker_list) > 30:
        raise HTTPException(status_code=400, detail="Tối đa 30 mã mỗi lần")

    from scanner import analyze_watchlist_stocks
    try:
        result = analyze_watchlist_stocks(ticker_list)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.now().isoformat()}


@app.get("/app", response_class=HTMLResponse)
def serve_app():
    """Phục vụ Web App HTML trực tiếp từ server."""
    html_path = os.path.join(os.path.dirname(__file__), "vnstock-scanner.html")
    if not os.path.exists(html_path):
        raise HTTPException(status_code=404, detail="File HTML không tìm thấy — hãy upload vnstock-scanner.html vào repo")
    with open(html_path, "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())


# Chạy trực tiếp (fallback nếu không dùng uvicorn command)
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)

"""FastAPI 入口:波形交接台 HTTP API 与网页。"""
from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from . import core
from .db import Database

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


class PublishRequest(BaseModel):
    station: str
    window_start_ms: int


def create_app(db_path: str) -> FastAPI:
    db = Database(db_path)
    app = FastAPI(title="波形交接台", version="1.0.0")

    @app.exception_handler(core.BatchRejected)
    async def _batch_rejected(_request: Request, exc: core.BatchRejected):
        return JSONResponse({"detail": str(exc)}, status_code=400)

    @app.exception_handler(core.IncompleteWindow)
    async def _incomplete(_request: Request, exc: core.IncompleteWindow):
        return JSONResponse(
            {"detail": f"窗口有效样点不足 {core.WINDOW_SAMPLES},发布失败,"
                       f"旧窗状态保持不变", "filled": exc.filled},
            status_code=409)

    @app.exception_handler(core.WindowConflict)
    async def _conflict(_request: Request, exc: core.WindowConflict):
        return JSONResponse({"detail": f"内部一致性错误: {exc}"}, status_code=500)

    @app.get("/", include_in_schema=False)
    def index():
        return FileResponse(os.path.join(STATIC_DIR, "index.html"))

    @app.get("/api/health")
    def health():
        return {"ok": True}

    @app.post("/api/upload")
    async def upload(request: Request):
        """上传 CSV 片段(请求体即 CSV 文本)。失败整批拒收,不改变库中状态。"""
        text = (await request.body()).decode("utf-8-sig")
        return core.upload_csv(db, text)

    @app.get("/api/stations")
    def stations():
        return {"stations": core.list_stations(db)}

    @app.get("/api/windows")
    def windows(station: str, from_ms: str, to_ms: str):
        """按台站与时间范围列出 UTC 整 10 秒窗的缺口与状态。from/to 支持
        epoch 毫秒或 ISO-8601。"""
        lo = core.parse_time_ms(from_ms)
        hi = core.parse_time_ms(to_ms)
        if hi <= lo:
            raise HTTPException(400, "to_ms 必须大于 from_ms")
        lo = lo - lo % core.WINDOW_MS
        hi = hi if hi % core.WINDOW_MS == 0 else hi + (core.WINDOW_MS - hi % core.WINDOW_MS)
        if (hi - lo) // core.WINDOW_MS > core.MAX_WINDOWS_PER_QUERY:
            raise HTTPException(
                400, f"单次查询窗口数超过上限 {core.MAX_WINDOWS_PER_QUERY}")
        return {"station": station, "from_ms": lo, "to_ms": hi,
                "windows": core.list_windows(db, station, lo, hi)}

    @app.get("/api/windows/{station}/{window_start_ms}")
    def window_detail(station: str, window_start_ms: int):
        if window_start_ms % core.WINDOW_MS != 0:
            raise HTTPException(400, "窗口起点须按 UTC 整 10 秒对齐")
        return core.window_detail(db, station, window_start_ms)

    @app.post("/api/publish")
    def publish(req: PublishRequest):
        """发布窗口:1000 个有效样点齐全才成功并替换现役窗;失败保留旧窗。"""
        return core.publish(db, req.station, req.window_start_ms)

    @app.get("/api/picking/{station}/{window_start_ms}")
    def picking(station: str, window_start_ms: int):
        """震相拾取接口:只读现役窗;过期(stale)窗返回 409。"""
        res = core.get_picking(db, station, window_start_ms)
        if res is None:
            raise HTTPException(404, "窗口不存在或尚未发布")
        if res == "stale":
            raise HTTPException(409, "窗口已过期(补传改变了有效样点),需重新发布")
        return res

    return app


app = create_app(os.environ.get("WAVEFORM_DB", "./waveform.db"))

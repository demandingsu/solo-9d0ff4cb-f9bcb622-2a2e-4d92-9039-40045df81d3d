# 波形交接台

地震台站离线补传波形的交接服务:台网资料员上传 100 Hz 波形 CSV 片段,
系统按台站与 UTC 整 10 秒窗(1000 样点)汇总缺口,样点齐全后发布完整
波形(含每个样点的来源轮次),供震相拾取程序通过接口读取。

技术栈:FastAPI + SQLite + 单页网页(无构建步骤)。

## 快速启动

### Docker Compose(推荐)

```bash
docker compose up --build
# 打开 http://localhost:8000
```

SQLite 数据保存在命名卷 `waveform-data` 中。

### 本地开发

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
uvicorn app.main:app --reload --port 8000
pytest -q          # 运行自测
```

数据库路径由环境变量 `WAVEFORM_DB` 指定(默认 `./waveform.db`)。

## CSV 格式

每行一个片段,5 个字段,`#` 开头为注释:

```
台站,UTC起点,采集轮次,片段号,振幅序列
STA1,2026-11-17T00:00:00.000Z,1,0,12 15 11 9 ...
STA1,1769040001000,2,3,-3 0 4 ...
```

- UTC 起点:epoch 毫秒整数,或带时区的 ISO-8601;**必须按 10 毫秒对齐**。
- 振幅序列:整数(int32),以空格、分号或逗号分隔。
- 轮次、片段号:非负整数。轮次越大优先级越高。

## 入库与覆盖规则

- **幂等**:同一(台站,轮次,片段号)同内容重传不重复入库(计为跳过)。
- **整批拒收**(上传事务整体回滚,不改变库中片段与窗状态):
  - 同轮同号但内容不同(批内或与库中已有片段);
  - 同轮片段在重叠位置取值不同(批内或与库中)。
- **轮次覆盖**:有效样点按"最高轮次优先"逐样点合成——高轮次只覆盖
  其重叠位置;晚到的低轮次不得覆盖高轮次样点,只能填补空缺。
- **窗过期**:补传使现役窗的有效样点发生变化时,该窗立即标为
  `stale`(过期),拾取接口不再将其当作现役;重新发布成功才替换,
  发布失败保留过期旧窗。
- **一致性**:上传、发布、读取共用一把写锁与显式事务
  (`BEGIN IMMEDIATE`),交错执行时每个请求看到的都是同一已提交状态。

## HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/upload` | 上传 CSV(请求体即文本)。返回 `{inserted, skipped, stale_windows}`;冲突返回 400 |
| GET  | `/api/stations` | 台站列表 |
| GET  | `/api/windows?station=&from_ms=&to_ms=` | 列出整 10 秒窗的有效样点数、缺口、状态(单次≤720 窗) |
| GET  | `/api/windows/{station}/{window_start_ms}` | 窗口详情:缺口区间、当前有效样点与来源轮次 |
| POST | `/api/publish` | 发布窗口,JSON `{station, window_start_ms}`;样点不足 1000 返回 409 且旧窗不变 |
| GET  | `/api/picking/{station}/{window_start_ms}` | 拾取接口:仅现役窗可读(含 `samples` 与逐样点 `rounds`);过期窗返回 409,未发布返回 404 |

`from_ms`/`to_ms` 与 CSV 起点一样支持 epoch 毫秒或 ISO-8601。

## curl 示例

```bash
# 上传(轮次1铺满窗口 1700000000000..+10s)
python3 - <<'EOF' > /tmp/seg.csv
print("STA1,1700000000000,1,0," + " ".join(["7"]*1000))
EOF
curl -s -X POST --data-binary @/tmp/seg.csv http://localhost:8000/api/upload

# 查询缺口
curl -s "http://localhost:8000/api/windows?station=STA1&from_ms=1700000000000&to_ms=1700000010000"

# 发布
curl -s -X POST -H 'Content-Type: application/json' \
  -d '{"station":"STA1","window_start_ms":1700000000000}' \
  http://localhost:8000/api/publish

# 拾取读取
curl -s http://localhost:8000/api/picking/STA1/1700000000000
```

## 项目结构

```
app/
  core.py            # 解析、冲突校验、有效样点合成、上传/发布事务
  db.py              # SQLite 单连接 + 锁,建表
  main.py            # FastAPI 路由与网页挂载
  static/index.html  # 操作网页(上传/缺口浏览/发布/拾取视图)
tests/test_waveform.py  # 21 条自测:重叠、补传、幂等、原子性、交错一致性
Dockerfile  docker-compose.yml  requirements*.txt
```

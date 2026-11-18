"""波形交接台自测:重叠、补传、幂等、原子性、窗状态机、交错一致性。"""
import threading

import pytest
from fastapi.testclient import TestClient

from app.main import create_app

W0 = 1_700_000_000_000  # 整 10 秒对齐的窗起点(epoch 毫秒)
STA = "STA1"


@pytest.fixture()
def client(tmp_path):
    app = create_app(str(tmp_path / "test.db"))
    with TestClient(app) as c:
        yield c


def row(station, start_ms, rnd, no, samples):
    return f"{station},{start_ms},{rnd},{no},{' '.join(map(str, samples))}"


def upload(client, text):
    return client.post("/api/upload", content=text)


def publish(client, station=STA, wstart=W0):
    return client.post("/api/publish",
                       json={"station": station, "window_start_ms": wstart})


def picking(client, station=STA, wstart=W0):
    return client.get(f"/api/picking/{station}/{wstart}")


def windows(client, station=STA, lo=W0, hi=W0 + 10_000):
    r = client.get(f"/api/windows?station={station}&from_ms={lo}&to_ms={hi}")
    assert r.status_code == 200, r.text
    return r.json()["windows"]


def fill_window(client, rnd, value, wstart=W0, station=STA):
    """用一个整窗片段(1000 样点)铺满窗口并返回上传响应。"""
    return upload(client, row(station, wstart, rnd, 0, [value] * 1000))


# ---------------------------------------------------------------- 基础流程

def test_upload_publish_pick_flow(client):
    r = fill_window(client, rnd=1, value=7)
    assert r.status_code == 200, r.text
    assert r.json()["inserted"] == 1

    w = windows(client)[0]
    assert (w["filled"], w["gaps"], w["state"]) == (1000, 0, "none")

    p = publish(client)
    assert p.status_code == 200, p.text
    assert p.json()["version"] == 1

    pk = picking(client)
    assert pk.status_code == 200
    d = pk.json()
    assert d["samples"] == [7] * 1000
    assert d["rounds"] == [1] * 1000
    assert d["version"] == 1


def test_iso_timestamp_accepted_and_aligned(client):
    r = upload(client, f"{STA},2023-11-14T22:13:20.000Z,1,0,1 2 3")
    assert r.status_code == 200, r.text
    assert r.json()["inserted"] == 1


def test_start_must_align_10ms(client):
    r = upload(client, row(STA, W0 + 5, 1, 0, [1, 2, 3]))
    assert r.status_code == 400
    assert "10毫秒" in r.json()["detail"]
    r2 = upload(client, f"{STA},2023-11-14T22:13:20.005Z,1,0,1 2 3")
    assert r2.status_code == 400


# ---------------------------------------------------------------- 幂等与拒收

def test_idempotent_reupload_same_content(client):
    text = row(STA, W0, 1, 0, [1] * 1000)
    assert upload(client, text).json()["inserted"] == 1
    r = upload(client, text)
    assert r.status_code == 200
    assert r.json() == {"inserted": 0, "skipped": 1, "stale_windows": []}
    # 现役窗不因同内容重传而过期
    fill_window(client, 1, 1)  # 该键已存在,跳过
    publish(client)
    r = upload(client, text)
    assert r.json()["stale_windows"] == []
    assert picking(client).status_code == 200


def test_same_key_different_content_rejected_and_state_kept(client):
    fill_window(client, 1, 1)
    publish(client)
    before = picking(client).json()

    r = upload(client, row(STA, W0, 1, 0, [9] * 1000))  # 同轮同号异内容
    assert r.status_code == 400
    assert "同轮同号" in r.json()["detail"]
    # 上传失败不改变原有片段与窗状态
    assert picking(client).status_code == 200
    assert picking(client).json() == before


def test_same_round_overlap_conflict_within_batch(client):
    text = "\n".join([
        row(STA, W0, 1, 0, [1] * 100),
        row(STA, W0 + 500, 1, 1, [2] * 100),  # 与前一片重叠 50 样点且异值
    ])
    r = upload(client, text)
    assert r.status_code == 400
    assert "重叠异值" in r.json()["detail"]
    assert windows(client)[0]["filled"] == 0  # 整批拒收,一片未入


def test_same_round_overlap_conflict_against_db(client):
    assert upload(client, row(STA, W0, 1, 0, [1] * 100)).status_code == 200
    r = upload(client, row(STA, W0 + 500, 1, 1, [2] * 100))
    assert r.status_code == 400
    assert "重叠异值" in r.json()["detail"]
    assert windows(client)[0]["filled"] == 100  # 只有第一批


def test_same_round_overlap_same_value_allowed(client):
    assert upload(client, row(STA, W0, 1, 0, [5] * 100)).status_code == 200
    r = upload(client, row(STA, W0 + 500, 1, 1, [5] * 100))  # 重叠 50 样点同值
    assert r.status_code == 200
    assert windows(client)[0]["filled"] == 150


def test_upload_atomicity_on_parse_error(client):
    text = "\n".join([
        row(STA, W0, 1, 0, [1] * 100),
        row(STA, W0 + 1003, 1, 1, [2] * 100),  # 起点未按 10ms 对齐
    ])
    r = upload(client, text)
    assert r.status_code == 400
    assert windows(client)[0]["filled"] == 0  # 第一行也未入库


def test_failed_upload_keeps_segments_and_window(client):
    fill_window(client, 1, 1)
    publish(client)
    # 批内一行合法新片段 + 一行同键异内容 -> 整批拒收
    text = "\n".join([
        row(STA, W0 + 20_000, 2, 0, [3] * 100),
        row(STA, W0, 1, 0, [9] * 1000),
    ])
    assert upload(client, text).status_code == 400
    assert windows(client)[0]["state"] == "active"  # 窗未过期
    assert picking(client).json()["samples"] == [1] * 1000
    # 合法那行也未入库
    w2 = windows(client, lo=W0 + 20_000, hi=W0 + 30_000)[0]
    assert w2["filled"] == 0


# ---------------------------------------------------------------- 轮次覆盖规则

def test_higher_round_overwrites_overlap_and_marks_stale(client):
    fill_window(client, 1, 1)
    publish(client)
    assert picking(client).status_code == 200

    # 高轮次补传前 100 样点 -> 现役窗立即过期
    r = upload(client, row(STA, W0, 2, 0, [2] * 100))
    assert r.status_code == 200
    assert r.json()["stale_windows"] == [{"station": STA, "window_start_ms": W0}]
    assert windows(client)[0]["state"] == "stale"

    # 拾取接口不再把旧窗当现役
    pk = picking(client)
    assert pk.status_code == 409

    # 重新发布成功才替换
    p = publish(client)
    assert p.status_code == 200
    assert p.json()["version"] == 2
    d = picking(client).json()
    assert d["samples"] == [2] * 100 + [1] * 900
    assert d["rounds"] == [2] * 100 + [1] * 900


def test_late_lower_round_does_not_overwrite(client):
    fill_window(client, 5, 5)   # 高轮次先铺满
    publish(client)
    r = upload(client, row(STA, W0, 2, 0, [2] * 1000))  # 晚到的低轮次
    assert r.status_code == 200
    assert r.json()["stale_windows"] == []  # 有效样点未变,窗不过期
    d = picking(client).json()
    assert d["samples"] == [5] * 1000
    assert d["rounds"] == [5] * 1000


def test_lower_round_fills_only_gaps(client):
    # 轮次 5 覆盖样点 0..499
    assert upload(client, row(STA, W0, 5, 0, [5] * 500)).status_code == 200
    # 晚到的轮次 2 覆盖 250..749:250..499 不得覆盖高轮次,500..749 补缺
    assert upload(client, row(STA, W0 + 2500, 2, 0, [2] * 500)).status_code == 200
    # 轮次 1 覆盖 750..999
    assert upload(client, row(STA, W0 + 7500, 1, 0, [1] * 250)).status_code == 200

    w = windows(client)[0]
    assert (w["filled"], w["gaps"]) == (1000, 0)
    assert publish(client).status_code == 200
    d = picking(client).json()
    assert d["samples"] == [5] * 500 + [2] * 250 + [1] * 250
    assert d["rounds"] == [5] * 500 + [2] * 250 + [1] * 250


def test_higher_round_only_covers_its_own_positions(client):
    # 轮次 1 先铺满,轮次 3 只补传中间 100 点 -> 只覆盖那 100 点
    fill_window(client, 1, 1)
    upload(client, row(STA, W0 + 4000, 3, 0, [3] * 100))
    publish(client)
    d = picking(client).json()
    assert d["samples"] == [1] * 400 + [3] * 100 + [1] * 500
    assert d["rounds"] == [1] * 400 + [3] * 100 + [1] * 500


# ---------------------------------------------------------------- 发布与窗状态

def test_publish_incomplete_fails_and_creates_nothing(client):
    assert upload(client, row(STA, W0, 1, 0, [1] * 500)).status_code == 200
    p = publish(client)
    assert p.status_code == 409
    assert p.json()["filled"] == 500
    assert windows(client)[0]["state"] == "none"
    assert picking(client).status_code == 404


def test_picking_not_found_for_unknown_window(client):
    assert picking(client).status_code == 404


def test_stale_window_republish_restores_active(client):
    fill_window(client, 1, 1)
    publish(client)
    upload(client, row(STA, W0, 2, 0, [2] * 10))  # 改动 10 点 -> 过期
    assert windows(client)[0]["state"] == "stale"
    assert picking(client).status_code == 409
    assert publish(client).status_code == 200
    assert picking(client).status_code == 200
    assert picking(client).json()["version"] == 2


def test_backfill_outside_active_window_does_not_stale(client):
    fill_window(client, 1, 1)
    publish(client)
    # 补传到相邻窗,不影响现役窗
    r = upload(client, row(STA, W0 + 10_000, 1, 1, [9] * 100))
    assert r.json()["stale_windows"] == []
    assert windows(client)[0]["state"] == "active"


def test_window_detail_shows_gap_ranges(client):
    upload(client, row(STA, W0 + 2000, 1, 0, [1] * 100))  # 样点 200..299
    d = client.get(f"/api/windows/{STA}/{W0}").json()
    assert d["filled"] == 100
    assert d["gap_ranges"] == [[0, 199], [300, 999]]
    assert d["state"] == "none"


def test_multi_segment_assembly_across_windows(client):
    # 两段跨窗片段拼满两个窗
    text = "\n".join([
        row(STA, W0 + 5000, 1, 0, [4] * 1000),  # 覆盖 W0 后半与 W1 前半
        row(STA, W0, 1, 1, [3] * 500),          # W0 前半
        row(STA, W0 + 15_000, 1, 2, [5] * 500), # W1 后半
    ])
    assert upload(client, text).status_code == 200
    ws = windows(client, lo=W0, hi=W0 + 20_000)
    assert [w["filled"] for w in ws] == [1000, 1000]
    for w in (W0, W0 + 10_000):
        assert publish(client, wstart=w).status_code == 200
    d0 = picking(client, wstart=W0).json()
    d1 = picking(client, wstart=W0 + 10_000).json()
    assert d0["samples"] == [3] * 500 + [4] * 500
    assert d1["samples"] == [4] * 500 + [5] * 500


# ---------------------------------------------------------------- 交错一致性

def test_interleaved_upload_publish_read_consistency(client):
    """上传/发布/读取交错:读到的样点、轮次、窗状态须来自同一已提交状态。

    不变式:值 == 轮次(轮次1填1,轮次2填2)。若 samples 与 rounds 来自
    不同提交状态,会出现 samples[i] != rounds[i]。
    """
    fill_window(client, 1, 1)
    publish(client)
    errors = []
    stop = threading.Event()

    def writer():
        try:
            for _ in range(25):
                r = upload(client, row(STA, W0, 2, 0, [2] * 1000))
                assert r.status_code == 200
                p = publish(client)
                assert p.status_code == 200
        except Exception as e:  # noqa: BLE001
            errors.append(f"writer: {e}")
        finally:
            stop.set()

    def reader():
        try:
            while not stop.is_set():
                resp = picking(client)
                if resp.status_code == 200:
                    d = resp.json()
                    assert len(d["samples"]) == 1000
                    assert len(d["rounds"]) == 1000
                    assert all(s == r for s, r in zip(d["samples"], d["rounds"]))
                    assert d["state"] == "active"
                elif resp.status_code == 409:
                    pass  # 过期是合法中间态
                else:
                    errors.append(f"reader: unexpected {resp.status_code}")
        except Exception as e:  # noqa: BLE001
            errors.append(f"reader: {e}")

    threads = [threading.Thread(target=writer)] + \
              [threading.Thread(target=reader) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors
    # 收尾:轮次 2 已生效
    d = picking(client).json()
    assert d["samples"] == [2] * 1000
    assert d["rounds"] == [2] * 1000

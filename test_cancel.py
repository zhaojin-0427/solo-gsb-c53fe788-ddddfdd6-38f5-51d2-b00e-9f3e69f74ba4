"""事件撤销专项验证（非 Docker，便携 Postgres + 本地文件载荷后端）。

Part A：确定性数据库交错测试——撤销、认领、结算、死信重放、租约过期回收之间
        的锁顺序与状态检查（pending 原子 cancelled；撤销后无新租约/不可重放；
        认领先于撤销时已认领请求成功仍 succeeded、失败或租约过期转 cancelled；
        幂等、404、终态与历史保留；其他事件顺序不受影响）。
Part B：四进程 HTTP 端到端（api + receiver + 2 worker）：撤销 API、事件撤销
        时间与投递终态查询、重复撤销幂等、未知事件 404、同键重复提交不复活、
        撤销后死信重放 409、飞行中请求成功仍记 succeeded、其他事件照常投递。
"""
from __future__ import annotations

import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import asyncpg
import httpx
import pgserver

ROOT = Path(__file__).resolve().parent
VENV_PY = os.environ.get("VENV_PY", "/tmp/venv/bin/python")

COMMON_ENV = {
    "LEASE_SECONDS": "30",
    "HTTP_TIMEOUT_SECONDS": "10",
    "WORKER_POLL_SECONDS": "0.1",
    "MAX_ATTEMPTS": "6",
    "BACKOFF_BASE_SECONDS": "1",
    "BACKOFF_CAP_SECONDS": "8",
    "RECEIVER_SECRET": "dev-signing-secret",
    "RECEIVER_TIMESTAMP_TOLERANCE_SECONDS": "600",
    "WEBHOOK_ALLOWED_HOSTS": "127.0.0.1,localhost",
    "WEBHOOK_ALLOWED_CIDRS": "",
    "WEBHOOK_MAX_REDIRECTS": "5",
    "WEBHOOK_KEYRING_JSON": '{"active_key_id":"k1","keys":{"k1":"MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="}}',
    "PAYLOAD_STORE_MODE": "local",
    "PAYLOAD_LOCAL_DIR": str(ROOT / ".test-payloads-cancel"),
    "PYTHONUNBUFFERED": "1",
}

failures: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")
    if not ok:
        failures.append(name)


# ---------------------------------------------------------------------------
# Part A：确定性事务交错（直接使用 repository 原语，无需 HTTP/Worker）
# ---------------------------------------------------------------------------


async def _make_delivery(conn, status: str = "pending", *, source: str | None = None):
    seq = await conn.fetchval("SELECT nextval('delivery_seq_global')")
    source = source or f"cancelprobe-{seq}"
    target = f"http://127.0.0.1:9/{source}"
    sub = await conn.fetchrow(
        """
        INSERT INTO subscriptions (source, target_url, secret, current_revision)
        VALUES ($1, $2, '', 1)
        RETURNING id
        """,
        source, target,
    )
    sid = sub["id"]
    await conn.execute(
        """
        INSERT INTO subscription_versions
            (subscription_id, revision, target_url, secret, max_attempts,
             backoff_base_seconds, backoff_cap_seconds)
        VALUES ($1, 1, $2, '', 6, 1.0, 8.0)
        """,
        sid, target,
    )
    ev = await conn.fetchrow(
        """
        INSERT INTO events (source, event_id, payload)
        VALUES ($1, $2, '{}'::jsonb)
        RETURNING id
        """,
        source, f"evt-{sid}",
    )
    ver = await conn.fetchval(
        "SELECT id FROM subscription_versions WHERE subscription_id=$1 AND revision=1",
        sid,
    )
    dl = await conn.fetchrow(
        """
        INSERT INTO deliveries
            (event_id_fk, subscription_id, config_version_id, chain_id, chain_seq, status)
        VALUES ($1, $2, $3, nextval('delivery_chain_seq'), 1, $4)
        RETURNING id
        """,
        ev["id"], sid, ver, status,
    )
    return sid, ev["id"], dl["id"]


async def _db_interleaving_tests(dsn: str) -> None:
    from app import repository as repo
    from app.db import init_schema

    conn = await asyncpg.connect(dsn)
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )
    await init_schema(conn)

    # ---- 用例 1：撤销 pending 投递原子转 cancelled，之后无新租约 -----------
    sid, ev_pk, dl_id = await _make_delivery(conn, "pending")
    now = datetime.now(timezone.utc) + timedelta(seconds=1)

    # 撤销前可以认领到
    pre = await repo._claim_select_candidate(conn, now)
    check("用例1: 撤销前候选可被选中", pre is not None and pre["id"] == dl_id)

    row, changed = await repo.cancel_event(conn, ev_pk)
    check("用例1: 首次撤销 changed=True 且 cancelled_at 落库",
          changed and row["cancelled_at"] is not None)
    state = await conn.fetchrow("SELECT status FROM deliveries WHERE id=$1", dl_id)
    check("用例1: pending 投递原子转 cancelled", state["status"] == "cancelled")
    check("用例1: 撤销后认领不到任何租约",
          await repo.claim_delivery(conn, "probe-a") is None)

    # 重复撤销：幂等，cancelled_at 不刷新
    async with conn.transaction():
        locked = await conn.fetchrow(
            "SELECT cancelled_at FROM events WHERE id=$1 FOR UPDATE", ev_pk
        )
    row2, changed2 = await repo.cancel_event(conn, ev_pk)
    check("用例1: 重复撤销幂等 changed=False 且时刻不变",
          not changed2 and row2["cancelled_at"] == locked["cancelled_at"])

    # 未知事件
    none_row, changed_none = await repo.cancel_event(conn, 999_999_999)
    check("用例1: 未知事件返回 (None, False)", none_row is None and not changed_none)

    # ---- 用例 2：认领先持锁 —— 撤销在投递行上等待，认领先提交后撤销跳过 ----
    # 生产中认领事务语句背靠背执行，不会等待事件行锁，故无死锁环；这里用
    # 后台任务发起撤销（其 UPDATE 在服务端阻塞于投递行锁），认领继续完成并
    # 提交后撤销才解除阻塞（此时行已 in_flight，pending 更新条件不再命中）。
    sid2, ev2, dl2 = await _make_delivery(conn, "pending")
    now2 = datetime.now(timezone.utc) + timedelta(seconds=1)
    other = await asyncpg.connect(dsn)
    await other.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )
    claim_tx = conn.transaction()
    await claim_tx.start()
    cand = await repo._claim_select_candidate(conn, now2)
    check("用例2: 选中撤销前候选", cand is not None and cand["id"] == dl2)
    subr = await repo._claim_lock_subscription(conn, sid2)
    check("用例2: 订阅未暂停", subr["paused"] is False)
    cancel_task = asyncio.create_task(repo.cancel_event(other, ev2))
    await asyncio.sleep(0.5)  # 等撤销到达事件锁并阻塞在投递行上
    check("用例2: 撤销进行中时认领新鲜读仍为未撤销（读已提交快照）",
          await repo._claim_event_cancelled(conn, ev2) is False)
    lease2 = "probe-worker:2"
    await repo._claim_mark_in_flight(
        conn, delivery_id=dl2, now=now2,
        lease_expires=now2 + timedelta(seconds=30), lease_token=lease2,
    )
    await claim_tx.commit()  # 认领先提交：解除撤销的等待
    _r2, c2 = await cancel_task
    check("用例2: 认领先提交后撤销成功提交", c2)
    st2 = await conn.fetchrow(
        "SELECT status, cancelled_at IS NOT NULL AS cancelled FROM deliveries d JOIN events e ON e.id=d.event_id_fk WHERE d.id=$1",
        dl2,
    )
    check("用例2: 已认领投递保持 in_flight、事件已撤销",
          st2["status"] == "in_flight" and st2["cancelled"] is True,
          str(dict(st2)))
    await other.close()

    # 认领回滚（放弃）时：阻塞的撤销解除后把仍为 pending 的行终结
    sid2b, ev2b, dl2b = await _make_delivery(conn, "pending")
    other2b = await asyncpg.connect(dsn)
    await other2b.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )
    claim_txb = conn.transaction()
    await claim_txb.start()
    candb = await repo._claim_select_candidate(conn, now2)
    check("用例2b: 选中候选", candb is not None and candb["id"] == dl2b)
    cancel_taskb = asyncio.create_task(repo.cancel_event(other2b, ev2b))
    await asyncio.sleep(0.5)
    await claim_txb.rollback()  # 认领放弃：投递行回到 pending
    _rb, cb = await cancel_taskb
    check("用例2b: 认领回滚后撤销提交成功", cb)
    st2b = await conn.fetchrow("SELECT status FROM deliveries WHERE id=$1", dl2b)
    check("用例2b: 认领放弃的 pending 被撤销终结为 cancelled",
          st2b["status"] == "cancelled")
    check("用例2b: 撤销后无新租约",
          await repo.claim_delivery(conn, "probe-2b") is None)
    await other2b.close()

    # ---- 用例 2c：撤销先持锁未提交 —— 候选 SKIP LOCKED 越过该投递，撤销提交
    # 后认领彻底不可见该事件 ----
    sid2c, ev2c, dl2c = await _make_delivery(conn, "pending")
    other2c = await asyncpg.connect(dsn)
    await other2c.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )
    ctxt = other2c.transaction()
    await ctxt.start()
    evlock = await other2c.fetchrow(
        "SELECT id, cancelled_at FROM events WHERE id=$1 FOR UPDATE", ev2c
    )
    check("用例2c: 撤销事务已持事件行锁（未提交）", evlock is not None)
    await other2c.execute(
        """
        UPDATE deliveries
        SET status='cancelled', not_before=now(),
            leased_at=NULL, lease_expires_at=NULL, leased_by=NULL, updated_at=now()
        WHERE event_id_fk=$1 AND status='pending'
        """,
        ev2c,
    )
    # 撤销事务尚未提交：投递行已被其 FOR UPDATE 锁定，认领候选 SKIP LOCKED
    # 必须直接越过该行而不能阻塞。
    got = await repo._claim_select_candidate(conn, now2)
    check("用例2c: 撤销持锁期间候选查询不阻塞且跳过该投递",
          got is None or got["id"] != dl2c,
          str(got["id"] if got else None))
    await other2c.execute("UPDATE events SET cancelled_at=now() WHERE id=$1", ev2c)
    await ctxt.commit()
    await other2c.close()
    check("用例2c: 撤销提交后认领不到该事件",
          await repo.claim_delivery(conn, "probe-2c") is None)
    check("用例2c: 投递为 cancelled",
          await conn.fetchval("SELECT status FROM deliveries WHERE id=$1", dl2c)
          == "cancelled")

    # ---- 用例 3：认领先提交 —— in_flight 不被撤销触碰；成功/失败结算 --------
    sid3, ev3, dl3 = await _make_delivery(conn, "pending")
    now3 = datetime.now(timezone.utc) + timedelta(seconds=1)
    claim_tx3 = conn.transaction()
    await claim_tx3.start()
    cand3 = await repo._claim_select_candidate(conn, now3)
    await repo._claim_lock_subscription(conn, sid3)
    check("用例3: 认领时事件未撤销",
          await repo._claim_event_cancelled(conn, ev3) is False)
    lease_token = "probe-worker:3"
    await repo._claim_mark_in_flight(
        conn, delivery_id=dl3, now=now3,
        lease_expires=now3 + timedelta(seconds=30), lease_token=lease_token,
    )
    await claim_tx3.commit()  # 租约先于撤销落库

    _r3, c3 = await repo.cancel_event(conn, ev3)
    check("用例3: 认领提交后撤销成功", c3)
    st3 = await conn.fetchrow("SELECT status, leased_by FROM deliveries WHERE id=$1", dl3)
    check("用例3: 撤销不触碰在飞投递（仍 in_flight、租约保留）",
          st3["status"] == "in_flight" and st3["leased_by"] == lease_token)

    # 飞行中失败 -> cancelled（不重试、不进死信，哪怕未达 max_attempts）
    await repo.record_attempt(
        conn, delivery_id=dl3, lease_token=lease_token, worker_id="probe-worker:3",
        outcome="http_error", http_status=500, response_excerpt="err",
        error_message=None, leased_at=now3,
        lease_expires_at=now3 + timedelta(seconds=30),
    )
    nbf = datetime.now(timezone.utc) + timedelta(seconds=5)
    settled_fail = await repo.settle_delivery(
        conn, delivery_id=dl3, lease_token=lease_token, succeeded=False,
        not_before=nbf, max_attempts=6,
    )
    check("用例3: 撤销后飞行中失败结算为 cancelled", settled_fail == "cancelled")
    st3b = await conn.fetchrow(
        "SELECT status, attempts_made, leased_by FROM deliveries WHERE id=$1", dl3
    )
    check("用例3: cancelled 终态、尝试历史保留、租约字段清空",
          st3b["status"] == "cancelled" and st3b["attempts_made"] == 1
          and st3b["leased_by"] is None)
    attempts = await repo.list_attempts(conn, dl3)
    check("用例3: 尝试历史保留 1 条 http_error",
          len(attempts) == 1 and attempts[0]["outcome"] == "http_error")
    check("用例3: 终态后不再被认领",
          await repo.claim_delivery(conn, "probe-b") is None)

    # 认领先提交 + 撤销后飞行中成功 -> succeeded（撤销不抹成功）
    sid3s, ev3s, dl3s = await _make_delivery(conn, "pending")
    now3s = datetime.now(timezone.utc) + timedelta(seconds=1)
    async with conn.transaction():
        c3s = await repo._claim_select_candidate(conn, now3s)
        await repo._claim_lock_subscription(conn, sid3s)
        tok3s = "probe-worker:3s"
        await repo._claim_mark_in_flight(
            conn, delivery_id=dl3s, now=now3s,
            lease_expires=now3s + timedelta(seconds=30), lease_token=tok3s,
        )
    await repo.cancel_event(conn, ev3s)
    await repo.record_attempt(
        conn, delivery_id=dl3s, lease_token=tok3s, worker_id="probe-worker:3s",
        outcome="success", http_status=200, response_excerpt="ok",
        error_message=None, leased_at=now3s,
        lease_expires_at=now3s + timedelta(seconds=30),
    )
    settled_ok = await repo.settle_delivery(
        conn, delivery_id=dl3s, lease_token=tok3s, succeeded=True,
        not_before=None, max_attempts=6,
    )
    check("用例3: 撤销后飞行中成功仍记 succeeded", settled_ok == "succeeded")

    # ---- 用例 4：租约过期的 in_flight 不被接管，sweep 终结为 cancelled ------
    sid4, ev4, dl4 = await _make_delivery(conn, "in_flight")
    past = datetime.now(timezone.utc) - timedelta(seconds=30)
    await conn.execute(
        """
        UPDATE deliveries
        SET leased_at=$2, lease_expires_at=$3, leased_by=$4
        WHERE id=$1
        """,
        dl4, past, past + timedelta(seconds=30), "dead-worker:4",
    )
    # 未撤销：租约过期仍可被接管（既有语义）
    now4 = datetime.now(timezone.utc) + timedelta(seconds=1)
    takeover = await repo._claim_select_candidate(conn, now4)
    check("用例4: 未撤销事件的过期租约仍可接管",
          takeover is not None and takeover["id"] == dl4)
    # 撤销后：候选排除（不接管），sweep 将其终结
    await repo.cancel_event(conn, ev4)
    check("用例4: 撤销后过期租约不被接管",
          await repo._claim_select_candidate(conn, now4) is None)
    # 租约未到期的在飞投递不被 sweep 触碰（构造另一条）
    sid4b, ev4b, dl4b = await _make_delivery(conn, "in_flight")
    future = datetime.now(timezone.utc) + timedelta(seconds=60)
    await conn.execute(
        "UPDATE deliveries SET leased_at=now(), lease_expires_at=$2, leased_by=$3 WHERE id=$1",
        dl4b, future, "alive-worker:4b",
    )
    await repo.cancel_event(conn, ev4b)
    n = await repo.sweep_cancelled_deliveries(conn, now4)
    check("用例4: sweep 终结过期的在飞投递（至少 dl4，且不动未到期 dl4b）",
          n >= 1 and await conn.fetchval(
              "SELECT status FROM deliveries WHERE id=$1", dl4) == "cancelled"
          and await conn.fetchval(
              "SELECT status FROM deliveries WHERE id=$1", dl4b) == "in_flight")
    # 再 sweep 幂等
    n2 = await repo.sweep_cancelled_deliveries(conn, now4)
    check("用例4: sweep 幂等（dl4 已终结，不计入）",
          await conn.fetchval(
              "SELECT status FROM deliveries WHERE id=$1", dl4) == "cancelled")
    check("用例4: 未到期者租约字段未被 sweep 清空",
          await conn.fetchval(
              "SELECT leased_by FROM deliveries WHERE id=$1", dl4b) == "alive-worker:4b")

    # ---- 用例 5：撤销后死信重放被拒；已有死信记录保留 ------------------------
    sid5, ev5, dl5 = await _make_delivery(conn, "dead_lettered")
    # 撤销前重放可行（其他死信路径已有测试覆盖，这里只建链验证撤销拒绝）
    await repo.cancel_event(conn, ev5)
    try:
        await repo.replay_dead_letter(conn, dl5)
        check("用例5: 撤销后死信重放被拒绝", False)
    except repo.ReplayError as exc:
        check("用例5: 撤销后死信重放抛 event_cancelled", exc.code == "event_cancelled")
    st5 = await conn.fetchrow("SELECT status FROM deliveries WHERE id=$1", dl5)
    check("用例5: 原死信记录原样保留", st5["status"] == "dead_lettered")
    try:
        await repo.replay_dead_letter(conn, dl5, use_current_version=True)
        check("用例5: use_current 重放同样被拒", False)
    except repo.ReplayError as exc:
        check("用例5: use_current_version=True 重放也被拒",
              exc.code == "event_cancelled")

    # ---- 用例 6：重放先于撤销提交：新 pending 随撤销被终结 -------------------
    sid6, ev6, dl6 = await _make_delivery(conn, "dead_lettered")
    new_row, _rev = await repo.replay_dead_letter(conn, dl6)
    check("用例6: 撤销前重放成功建 pending 链记录",
          new_row["status"] == "pending" and new_row["chain_seq"] == 2)
    await repo.cancel_event(conn, ev6)
    st6a = await conn.fetchrow("SELECT status FROM deliveries WHERE id=$1", dl6)
    st6b = await conn.fetchrow("SELECT status FROM deliveries WHERE id=$1", new_row["id"])
    check("用例6: 撤销同时终结原死信旁的重放 pending（旧死信保留）",
          st6a["status"] == "dead_lettered" and st6b["status"] == "cancelled")

    # ---- 用例 7：其他事件仍严格按序投递；终态/成功记录/载荷引用保留 ----------
    sA, eA, dA = await _make_delivery(conn, "pending", source="ord-keep")
    sB, eB, dB = await _make_delivery(conn, "pending", source="ord-keep-2")
    # 同 source 不现实（种子唯一 source），改用同订阅连续两事件验证顺序
    evA2 = await conn.fetchval(
        """
        INSERT INTO events (source, event_id, payload)
        VALUES ('ord-keep', 'evt-second', '{}'::jsonb) RETURNING id
        """
    )
    verA = await conn.fetchval(
        "SELECT config_version_id FROM deliveries WHERE id=$1", dA
    )
    dA2 = await conn.fetchval(
        """
        INSERT INTO deliveries
            (event_id_fk, subscription_id, config_version_id, chain_id, chain_seq, status)
        VALUES ($1, $2, $3, nextval('delivery_chain_seq'), 1, 'pending')
        RETURNING id
        """,
        evA2, sA, verA,
    )
    # 撤销第二条事件（dA2），第一条 dA 不受影响、可被认领；dA2 为 cancelled 终态
    await repo.cancel_event(conn, evA2)
    now7 = datetime.now(timezone.utc) + timedelta(seconds=1)
    claim7 = await repo.claim_delivery(conn, "probe-7")
    check("用例7: 其他事件照常被认领（严格顺序：cancelled 视为终态不挡队首）",
          claim7 is not None and claim7["delivery"]["id"] == dA,
          str(claim7["delivery"]["id"] if claim7 else None))
    check("用例7: 被撤销的后续事件未被选中",
          await conn.fetchval("SELECT status FROM deliveries WHERE id=$1", dA2)
          == "cancelled")
    # 完全无关订阅 sB 的投递也仍可认领（在 dA 结算后；先把 dA 终结）
    async with conn.transaction():
        locked = await conn.fetchrow(
            "SELECT leased_by FROM deliveries WHERE id=$1 FOR UPDATE", dA
        )
        await repo.settle_delivery(
            conn, delivery_id=dA, lease_token=locked["leased_by"],
            succeeded=True, not_before=None, max_attempts=6,
        )
    claimB = await repo.claim_delivery(conn, "probe-7b")
    check("用例7: 无关订阅事件不受撤销影响",
          claimB is not None and claimB["delivery"]["id"] == dB)
    async with conn.transaction():
        locked = await conn.fetchrow(
            "SELECT leased_by FROM deliveries WHERE id=$1 FOR UPDATE", dB
        )
        await repo.settle_delivery(
            conn, delivery_id=dB, lease_token=locked["leased_by"],
            succeeded=True, not_before=None, max_attempts=6,
        )

    # 载荷引用仍在（事件行与 payload 保留；撤销不触发 GC 删除）
    ev_row = await conn.fetchrow(
        "SELECT id, cancelled_at, payload IS NOT NULL AS has_payload FROM events WHERE id=$1",
        evA2,
    )
    check("用例7: 撤销事件行与载荷保留",
          ev_row is not None and ev_row["cancelled_at"] is not None
          and ev_row["has_payload"])

    await conn.close()


# ---------------------------------------------------------------------------
# Part B：HTTP 端到端
# ---------------------------------------------------------------------------


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_http(url: str, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            if httpx.get(url, timeout=1).status_code == 200:
                return
        except Exception as exc:  # noqa: BLE001
            last = exc
        time.sleep(0.3)
    raise RuntimeError(f"服务未就绪: {url} ({last})")


def wait_for(condition, desc: str, timeout: float = 30.0, interval: float = 0.1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = condition()
        if result:
            return result
        time.sleep(interval)
    raise AssertionError(f"等待超时: {desc}")


class Harness:
    def __init__(self) -> None:
        self.procs: list[subprocess.Popen] = []

    def spawn(self, module_cmd: list[str], name: str):
        env = os.environ.copy()
        env.update(COMMON_ENV)
        env["DATABASE_URL"] = self.dsn
        log = open(ROOT / f"test-cancel-{name}.log", "w")
        proc = subprocess.Popen(
            [VENV_PY, "-m", *module_cmd],
            cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
        )
        self.procs.append(proc)

    def start(self):
        self.pg = pgserver.get_server(
            str(ROOT / ".test-pgdata-cancel"), cleanup_mode="stop"
        )
        self.dsn = self.pg.get_uri()
        api_port = free_port()
        rcv_port = free_port()
        self.api = f"http://127.0.0.1:{api_port}"
        self.rcv = f"http://127.0.0.1:{rcv_port}"
        self.spawn(
            ["uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(api_port)],
            "api",
        )
        self.spawn(
            ["uvicorn", "app.receiver:app", "--host", "127.0.0.1", "--port", str(rcv_port)],
            "receiver",
        )
        self.spawn(["app.worker"], "worker1")
        self.spawn(["app.worker"], "worker2")
        wait_http(f"{self.api}/health")
        wait_http(f"{self.rcv}/health")

    def stop(self):
        for p in self.procs:
            p.send_signal(signal.SIGTERM)
        for p in self.procs:
            try:
                p.wait(timeout=8)
            except subprocess.TimeoutExpired:
                p.kill()
        try:
            self.pg.cleanup()
        except Exception:  # noqa: BLE001
            pass


def run_http_tests(h: Harness) -> None:
    api = httpx.Client(base_url=h.api, timeout=15)
    rcv = httpx.Client(base_url=h.rcv, timeout=15)
    rcv.post("/received/reset")
    rcv.post("/control/fail", params={"enabled": False}).raise_for_status()

    # 未知事件撤销 -> 404
    r404 = api.post("/api/v1/events/999999/cancel")
    check("未知事件撤销 404", r404.status_code == 404, r404.text)

    # 订阅（先暂停，使投递稳定停留在 pending 便于观察撤销）
    sub = api.post("/api/v1/subscriptions", json={
        "source": "cx",
        "target_url": f"{h.rcv}/receive",
        "secret": "dev-signing-secret",
        "backoff_base_seconds": 1,
        "backoff_cap_seconds": 4,
    }).json()
    sid = sub["id"]
    api.post(f"/api/v1/subscriptions/{sid}/pause").raise_for_status()

    ev = api.post("/api/v1/events", json={
        "source": "cx", "event_id": "c1", "payload": {"k": 1},
    }).json()
    ev_pk = ev["id"]
    d1 = wait_for(
        lambda: next((x for x in api.get("/api/v1/deliveries",
                     params={"source": "cx"}).json()
                     if x["source_event_id"] == "c1"), None),
        "c1 投递出现",
    )
    time.sleep(0.5)
    check("暂停期间 c1 保持 pending",
          api.get(f"/api/v1/deliveries/{d1['id']}").json()["status"] == "pending")

    # 撤销
    cr = api.post(f"/api/v1/events/{ev_pk}/cancel")
    check("撤销 200 且事件查询显示 cancelled_at",
          cr.status_code == 200 and cr.json()["cancelled_at"] is not None, cr.text)
    cancelled_at = cr.json()["cancelled_at"]
    got = api.get(f"/api/v1/events/{ev_pk}").json()
    check("GET 事件显示同一 cancelled_at", got["cancelled_at"] == cancelled_at)
    dl = wait_for(
        lambda: (lambda d: d if d["status"] == "cancelled" else None)(
            api.get(f"/api/v1/deliveries/{d1['id']}").json()),
        "投递查询显示 cancelled 终态",
    )
    check("cancelled 投递无租约字段",
          dl["leased_at"] is None and dl["leased_by"] is None
          and dl["lease_expires_at"] is None, json.dumps(dl, ensure_ascii=False))
    evdels = api.get(f"/api/v1/events/{ev_pk}/deliveries").json()
    check("事件投递列表均为终态 cancelled",
          all(x["status"] == "cancelled" for x in evdels) and len(evdels) == 1)

    # 恢复订阅后投递不得复活
    api.post(f"/api/v1/subscriptions/{sid}/resume").raise_for_status()
    time.sleep(2.0)
    check("恢复订阅后撤销投递仍 cancelled（不复活）",
          api.get(f"/api/v1/deliveries/{d1['id']}").json()["status"] == "cancelled")
    check("撤销投递零尝试",
          api.get(f"/api/v1/deliveries/{d1['id']}").json()["attempts_made"] == 0)
    recv = rcv.get("/received", params={"limit": 200}).json()["items"]
    check("接收器从未收到 c1", not any(x["event_id"] == "c1" for x in recv))

    # 同 source/event_id 重复提交：200 duplicate，cancelled_at 保留，不新增投递
    dup = api.post("/api/v1/events", json={
        "source": "cx", "event_id": "c1", "payload": {"k": 1},
    })
    check("同键重复提交 200 duplicate 且撤销态保留",
          dup.status_code == 200 and dup.json()["duplicate"] is True
          and dup.json()["cancelled_at"] == cancelled_at
          and dup.json()["id"] == ev_pk, dup.text)
    time.sleep(1.0)
    ndel = len(api.get(f"/api/v1/events/{ev_pk}/deliveries").json())
    check("重复提交不新增/复活投递", ndel == 1)

    # 重复撤销幂等：时刻不变
    cr2 = api.post(f"/api/v1/events/{ev_pk}/cancel")
    check("重复撤销 200 且 cancelled_at 不刷新",
          cr2.status_code == 200 and cr2.json()["cancelled_at"] == cancelled_at)

    # status=cancelled 过滤查询可用
    lst = api.get("/api/v1/deliveries", params={"status": "cancelled"}).json()
    check("按 status=cancelled 可查询",
          any(x["id"] == d1["id"] for x in lst))
    bad = api.get("/api/v1/deliveries", params={"status": "bogus"})
    check("非法 status 仍 400", bad.status_code == 400)

    # ---- 撤销前已认领（in_flight）的请求成功仍记 succeeded（/slow 5s）-------
    slow_sub = api.post("/api/v1/subscriptions", json={
        "source": "cxslow",
        "target_url": f"{h.rcv}/slow?seconds=5",
        "secret": "dev-signing-secret",
    }).json()
    ssid = slow_sub["id"]
    api.post(f"/api/v1/subscriptions/{ssid}/pause").raise_for_status()
    sev = api.post("/api/v1/events", json={
        "source": "cxslow", "event_id": "s1", "payload": {},
    }).json()
    sd = wait_for(
        lambda: next((x for x in api.get("/api/v1/deliveries",
                     params={"source": "cxslow"}).json()
                     if x["source_event_id"] == "s1"), None),
        "s1 投递出现",
    )
    api.post(f"/api/v1/subscriptions/{ssid}/resume").raise_for_status()
    wait_for(
        lambda: api.get(f"/api/v1/deliveries/{sd['id']}").json()["status"] == "in_flight",
        "s1 进入 in_flight（请求飞行中）",
        timeout=10,
    )
    scr = api.post(f"/api/v1/events/{sev['id']}/cancel")
    check("飞行中撤销 200", scr.status_code == 200 and scr.json()["cancelled_at"])
    final = wait_for(
        lambda: (lambda d: d if d["status"] in {"succeeded", "cancelled"} else None)(
            api.get(f"/api/v1/deliveries/{sd['id']}").json()),
        "s1 结算",
        timeout=15,
    )
    check("飞行中请求成功仍记 succeeded（尝试历史保留 1 条）",
          final["status"] == "succeeded" and final["attempts_made"] == 1,
          json.dumps(final, ensure_ascii=False))
    got2 = api.get(f"/api/v1/events/{sev['id']}").json()
    check("成功后事件仍保持撤销态", got2["cancelled_at"] is not None)

    # ---- 死信后撤销：死信记录保留，重放 409 -------------------------------
    rcv.post("/control/fail", params={"enabled": True}).raise_for_status()
    dlsub = api.post("/api/v1/subscriptions", json={
        "source": "cxdl",
        "target_url": f"{h.rcv}/fail",
        "secret": "dev-signing-secret",
        "max_attempts": 2,
        "backoff_base_seconds": 1,
        "backoff_cap_seconds": 1,
    }).json()
    dlsid = dlsub["id"]
    dlev = api.post("/api/v1/events", json={
        "source": "cxdl", "event_id": "d1", "payload": {},
    }).json()
    dlrow = wait_for(
        lambda: next((x for x in api.get("/api/v1/deliveries",
                     params={"source": "cxdl"}).json()
                     if x["source_event_id"] == "d1"), None),
        "d1 投递出现",
    )
    wait_for(
        lambda: api.get(f"/api/v1/deliveries/{dlrow['id']}").json()["status"] == "dead_lettered",
        "d1 进入死信",
        timeout=40,
    )
    # 撤销前重放可以成功（建链），撤销后再拒绝
    api.post(f"/api/v1/events/{dlev['id']}/cancel").raise_for_status()
    rj = api.post(f"/api/v1/dead-letters/{dlrow['id']}/replay", json={})
    check("撤销后死信重放 409", rj.status_code == 409, rj.text)
    check("原死信记录保留且仍在死信列表",
          api.get(f"/api/v1/deliveries/{dlrow['id']}").json()["status"] == "dead_lettered"
          and any(x["id"] == dlrow["id"]
                  for x in api.get("/api/v1/dead-letters",
                                   params={"source": "cxdl"}).json()))
    attempts = api.get(f"/api/v1/deliveries/{dlrow['id']}/attempts").json()
    check("撤销不删除尝试历史（2 条失败）",
          len(attempts) == 2 and all(a["outcome"] == "http_error" for a in attempts))
    rcv.post("/control/fail", params={"enabled": False}).raise_for_status()

    # ---- 其他事件仍按原顺序投递（同订阅 c1 已 cancelled 不挡后续）----------
    # 用一个新订阅验证连续两事件严格顺序送达
    osub = api.post("/api/v1/subscriptions", json={
        "source": "cxord",
        "target_url": f"{h.rcv}/receive",
        "secret": "dev-signing-secret",
    }).json()
    osid = osub["id"]
    api.post("/api/v1/events", json={"source": "cxord", "event_id": "o1", "payload": {}})
    api.post("/api/v1/events", json={"source": "cxord", "event_id": "o2", "payload": {}})
    ods = wait_for(
        lambda: (lambda ds: ds if len(ds) == 2 else None)([
            x for x in api.get("/api/v1/deliveries", params={"source": "cxord"}).json()
            if x["source_event_id"] in {"o1", "o2"}
        ]),
        "o1/o2 投递均创建",
    )
    for x in ods:
        wait_for(
            lambda x=x: api.get(f"/api/v1/deliveries/{x['id']}").json()["status"] == "succeeded",
            f"{x['source_event_id']} 成功",
        )
    # 撤销 o2：已成功的投递不被修改
    o2 = next(x for x in ods if x["source_event_id"] == "o2")
    o2_pk = o2["event_id_fk"]
    api.post(f"/api/v1/events/{o2_pk}/cancel").raise_for_status()
    check("撤销不修改已成功投递（仍 succeeded）",
          api.get(f"/api/v1/deliveries/{o2['id']}").json()["status"] == "succeeded")
    check("已成功事件显示撤销时间",
          api.get(f"/api/v1/events/{o2_pk}").json()["cancelled_at"] is not None)

    items = rcv.get("/received", params={"limit": 200}).json()["items"]
    i1 = max(i for i, x in enumerate(items) if x["event_id"] == "o1")
    i2 = max(i for i, x in enumerate(items) if x["event_id"] == "o2")
    check("其他事件严格顺序送达（o1 先于 o2）", i1 < i2, f"{i1} vs {i2}")
    check("被撤销的 c1 从未送达",
          not any(x["event_id"] == "c1" for x in items))


def main() -> int:
    # Part A
    pg = pgserver.get_server(str(ROOT / ".test-pgdata-cancel-db"), cleanup_mode="stop")
    dsn = pg.get_uri()
    try:
        asyncio.run(_db_interleaving_tests(dsn))
    except Exception:  # noqa: BLE001
        import traceback
        traceback.print_exc()
        failures.append("Part A 异常")
    finally:
        try:
            pg.cleanup()
        except Exception:  # noqa: BLE001
            pass

    # Part B
    h = Harness()
    h.start()
    try:
        run_http_tests(h)
    except Exception:  # noqa: BLE001
        import traceback
        traceback.print_exc()
        failures.append("Part B 异常")
    finally:
        h.stop()

    print()
    if failures:
        print(f"失败 {len(failures)} 项: {failures}")
        return 1
    print("事件撤销全部检查通过 ✅")
    return 0


if __name__ == "__main__":
    sys.exit(main())

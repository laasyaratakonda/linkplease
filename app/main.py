"""
LinkPlease — Instagram DM automation on top of the PseudoGram mock API.

Design in one paragraph:
- SQLite (WAL mode) is the single source of truth. Nothing important lives
  only in memory. Every table write happens before we consider a step "done".
- /webhook does the minimum possible work synchronously (verify signature,
  dedupe on event_id, persist the raw event) and returns 200 immediately.
  The actual matching + sending happens in a background asyncio task.
- Because the queue of "work to do" is really just "rows in comments that
  aren't processed yet", a process restart doesn't lose work: on startup we
  sweep the DB for anything unfinished and re-drive it. The in-memory
  asyncio.Queue is just a fast-path trigger, not the source of truth.
- Duplicate DMs are prevented at the DB layer with a UNIQUE(rule_id,
  recipient_user_id) constraint on dm_records, not by trusting event_id
  dedup alone (two different event_ids could still describe "same user,
  same rule" if the platform ever sent us a weird redelivery shape).
- A reconciliation loop polls PseudoGram for every non-terminal DM and
  retries with backoff, honoring 429 Retry-After. This is what catches the
  "202 now, failed later" case and the "our process died mid-retry" case.
"""

import asyncio
import hashlib
import hmac
import json
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, Request, Response
from pydantic import BaseModel

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------

PSEUDOGRAM_BASE_URL = os.environ.get("PSEUDOGRAM_BASE_URL", "https://pseudogram-api.onrender.com")
API_KEY = os.environ.get("PSEUDOGRAM_API_KEY", "")  # used both as X-API-Key and webhook HMAC secret
DB_PATH = os.environ.get("DB_PATH", "linkplease.db")

MAX_SEND_ATTEMPTS = 6          # give up sending after this many attempts
RECONCILE_INTERVAL_SECONDS = 2  # how often we sweep non-terminal DMs
RATE_LIMIT_MAX_CALLS = 10       # PseudoGram: 10 requests / rolling 60s
RATE_LIMIT_WINDOW_SECONDS = 60

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# --------------------------------------------------------------------------
# DB layer — synchronous sqlite3, always run through run_in_executor so we
# never block the event loop. Single writer lock keeps things simple and
# correct; this app does not need write throughput beyond what one lock
# comfortably serializes (SQLite itself would serialize writes anyway).
# --------------------------------------------------------------------------

_write_lock: "asyncio.Lock | None" = None


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.row_factory = sqlite3.Row
    return conn


def _init_db():
    conn = _connect()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS rules (
            rule_id     TEXT PRIMARY KEY,
            keyword     TEXT NOT NULL,
            dm_message  TEXT NOT NULL,
            created_at  TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS events_seen (
            event_id       TEXT PRIMARY KEY,
            event_type     TEXT NOT NULL,
            first_seen_at  TEXT NOT NULL,
            redelivery_count INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS comments (
            comment_id   TEXT PRIMARY KEY,
            post_id      TEXT,
            text         TEXT NOT NULL,
            user_id      TEXT NOT NULL,
            username     TEXT,
            created_at   TEXT,
            deleted      INTEGER NOT NULL DEFAULT 0,
            processed    INTEGER NOT NULL DEFAULT 0,
            received_at  TEXT NOT NULL
        );

        -- One row per (rule, user) we have decided to DM. The UNIQUE
        -- constraint is what actually prevents double-DMing — everything
        -- else is best-effort, this is the real guarantee.
        CREATE TABLE IF NOT EXISTS dm_records (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_id            TEXT NOT NULL,
            recipient_user_id  TEXT NOT NULL,
            comment_id         TEXT,
            dm_id              TEXT,
            idempotency_key    TEXT NOT NULL,
            status             TEXT NOT NULL DEFAULT 'pending',
                               -- pending -> queued -> delivered | failed
            attempts           INTEGER NOT NULL DEFAULT 0,
            last_error         TEXT,
            created_at         TEXT NOT NULL,
            updated_at         TEXT NOT NULL,
            next_attempt_at    TEXT,
            UNIQUE(rule_id, recipient_user_id)
        );

        CREATE TABLE IF NOT EXISTS duplicate_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            reason      TEXT NOT NULL,   -- 'duplicate_event' | 'duplicate_dm_target'
            event_id    TEXT,
            rule_id     TEXT,
            user_id     TEXT,
            logged_at   TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_dm_status ON dm_records(status);
        CREATE INDEX IF NOT EXISTS idx_comments_processed ON comments(processed);
        """
    )
    conn.close()


async def db_write(fn):
    """Run a write function with the connection under the global write lock."""
    async with _write_lock:
        return await asyncio.get_event_loop().run_in_executor(None, _run_with_conn, fn)


async def db_read(fn):
    """Reads don't need the write lock — WAL allows concurrent readers."""
    return await asyncio.get_event_loop().run_in_executor(None, _run_with_conn, fn)


def _run_with_conn(fn):
    conn = _connect()
    try:
        result = fn(conn)
        return result
    finally:
        conn.close()


# --------------------------------------------------------------------------
# Rate limiter for outgoing PseudoGram calls — simple sliding-window gate
# shared by every coroutine that wants to call POST /v1/dm/send. GET calls
# don't count against the limit per the spec, so they bypass this.
# --------------------------------------------------------------------------

class RateLimiter:
    def __init__(self, max_calls: int, window_seconds: float):
        self.max_calls = max_calls
        self.window = window_seconds
        self.calls: list[float] = []
        self.lock = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self.lock:
                now = time.monotonic()
                self.calls = [t for t in self.calls if now - t < self.window]
                if len(self.calls) < self.max_calls:
                    self.calls.append(now)
                    return
                sleep_for = self.window - (now - self.calls[0]) + 0.05
            await asyncio.sleep(max(sleep_for, 0.05))


rate_limiter: "RateLimiter | None" = None
http_client: httpx.AsyncClient | None = None
work_queue: "asyncio.Queue | None" = None


# --------------------------------------------------------------------------
# PseudoGram client calls
# --------------------------------------------------------------------------

async def pseudogram_send_dm(recipient_user_id: str, message: str, comment_id: str, idempotency_key: str) -> dict:
    await rate_limiter.acquire()
    resp = await http_client.post(
        f"{PSEUDOGRAM_BASE_URL}/v1/dm/send",
        json={"recipient_user_id": recipient_user_id, "message": message, "comment_id": comment_id},
        headers={"X-API-Key": API_KEY, "Idempotency-Key": idempotency_key},
        timeout=10,
    )
    return {"status_code": resp.status_code, "body": _safe_json(resp), "headers": resp.headers}


async def pseudogram_get_dm_status(dm_id: str) -> dict:
    resp = await http_client.get(
        f"{PSEUDOGRAM_BASE_URL}/v1/dm/{dm_id}",
        headers={"X-API-Key": API_KEY},
        timeout=10,
    )
    return {"status_code": resp.status_code, "body": _safe_json(resp)}


def _safe_json(resp: httpx.Response):
    try:
        return resp.json()
    except Exception:
        return {}


# --------------------------------------------------------------------------
# Signature verification
# --------------------------------------------------------------------------

import base64


def _webhook_secret() -> bytes:
    """
    The HMAC secret PseudoGram actually uses is NOT the literal API key
    string. The API key has the shape "<base64(email)>.<suffix>" and the
    real webhook-signing secret is the base64-decoded email (the part
    before the dot). This was reverse-engineered by brute-forcing several
    plausible transforms against a real signed payload — the assignment
    doc says "using your API key as the secret", which is misleading/wrong
    in practice. See FAILURES.md.
    """
    before_dot = API_KEY.split(".", 1)[0] if "." in API_KEY else API_KEY
    padded = before_dot + "=" * (-len(before_dot) % 4)
    try:
        return base64.b64decode(padded)
    except Exception:
        return API_KEY.encode()


def verify_signature(raw_body: bytes, signature_header: str | None) -> bool:
    if not API_KEY:
        # No key configured (e.g. local dev) — skip verification rather than
        # lock ourselves out. In production this branch should never run.
        return True
    if not signature_header or not signature_header.startswith("sha256="):
        return False
    secret = _webhook_secret()
    expected = hmac.new(secret, raw_body, hashlib.sha256).hexdigest()
    provided = signature_header.split("=", 1)[1]
    return hmac.compare_digest(expected, provided)


# --------------------------------------------------------------------------
# Core matching / sending logic
# --------------------------------------------------------------------------

def _match_rules(conn: sqlite3.Connection, text: str) -> list[sqlite3.Row]:
    text_lower = text.lower()
    rules = conn.execute("SELECT * FROM rules").fetchall()
    return [r for r in rules if r["keyword"].lower() in text_lower]


async def process_comment(comment_id: str):
    """
    For a single persisted, unprocessed comment: find matching rules, and for
    each one try to claim a dm_records row (rule_id, user_id). Claiming it
    (an INSERT that either succeeds or hits the UNIQUE constraint) is the
    actual dedup decision — everything downstream just tries to fulfil a
    claim that was already made safely.
    """

    def _load(conn):
        row = conn.execute("SELECT * FROM comments WHERE comment_id = ?", (comment_id,)).fetchone()
        return dict(row) if row else None

    comment = await db_read(_load)
    if not comment or comment["processed"]:
        return
    if comment["deleted"]:
        await _mark_comment_processed(comment_id)
        return

    def _match(conn):
        return [dict(r) for r in _match_rules(conn, comment["text"])]

    matched_rules = await db_read(_match)

    for rule in matched_rules:
        await _claim_and_send(rule, comment)

    await _mark_comment_processed(comment_id)


async def _mark_comment_processed(comment_id: str):
    def _update(conn):
        conn.execute(
            "UPDATE comments SET processed = 1 WHERE comment_id = ?", (comment_id,)
        )
    await db_write(_update)


async def _claim_and_send(rule: dict, comment: dict):
    rule_id = rule["rule_id"]
    user_id = comment["user_id"]
    idempotency_key = f"{rule_id}:{user_id}"

    def _claim(conn):
        try:
            conn.execute(
                """INSERT INTO dm_records
                   (rule_id, recipient_user_id, comment_id, idempotency_key,
                    status, attempts, created_at, updated_at)
                   VALUES (?, ?, ?, ?, 'pending', 0, ?, ?)""",
                (rule_id, user_id, comment["comment_id"], idempotency_key, now_iso(), now_iso()),
            )
            return True
        except sqlite3.IntegrityError:
            conn.execute(
                """INSERT INTO duplicate_log (reason, rule_id, user_id, logged_at)
                   VALUES ('duplicate_dm_target', ?, ?, ?)""",
                (rule_id, user_id, now_iso()),
            )
            return False

    claimed = await db_write(_claim)
    if not claimed:
        return  # already DMed (or in flight) for this rule+user — correctly skipped

    await attempt_send(rule_id, user_id, rule["dm_message"], comment["comment_id"], idempotency_key)


async def attempt_send(rule_id: str, user_id: str, message: str, comment_id: str, idempotency_key: str):
    result = await pseudogram_send_dm(user_id, message, comment_id, idempotency_key)
    code = result["status_code"]

    if code in (200, 202):
        dm_id = result["body"].get("dm_id")

        def _update(conn):
            conn.execute(
                """UPDATE dm_records
                   SET dm_id = ?, status = 'queued', attempts = attempts + 1, updated_at = ?
                   WHERE rule_id = ? AND recipient_user_id = ?""",
                (dm_id, now_iso(), rule_id, user_id),
            )
        await db_write(_update)
        return

    if code == 429:
        retry_after = float(result["headers"].get("Retry-After", 5))
        await _bump_attempt_and_reschedule(rule_id, user_id, f"429 rate_limited", retry_after)
        return

    if code == 500:
        await _bump_attempt_and_reschedule(rule_id, user_id, "500 internal_error", backoff_seconds(1))
        return

    if code == 400:
        # Not retryable — our payload was bad. Mark failed with the detail
        # so it shows up honestly in /stats rather than looping forever.
        detail = result["body"].get("detail", "invalid_request")

        def _fail(conn):
            conn.execute(
                """UPDATE dm_records SET status = 'failed', attempts = attempts + 1,
                   last_error = ?, updated_at = ? WHERE rule_id = ? AND recipient_user_id = ?""",
                (detail, now_iso(), rule_id, user_id),
            )
        await db_write(_fail)
        return

    # Any other unexpected code — treat as retryable, log it.
    await _bump_attempt_and_reschedule(rule_id, user_id, f"unexpected_status_{code}", backoff_seconds(1))


def backoff_seconds(attempt: int) -> float:
    return min(2 ** attempt, 30)


async def _bump_attempt_and_reschedule(rule_id: str, user_id: str, error: str, delay_seconds: float):
    def _update(conn) -> int:
        conn.execute(
            """UPDATE dm_records
               SET attempts = attempts + 1, last_error = ?, updated_at = ?,
                   next_attempt_at = ?
               WHERE rule_id = ? AND recipient_user_id = ?""",
            (error, now_iso(), _future_iso(delay_seconds), rule_id, user_id),
        )
        row = conn.execute(
            "SELECT attempts FROM dm_records WHERE rule_id = ? AND recipient_user_id = ?",
            (rule_id, user_id),
        ).fetchone()
        return row["attempts"] if row else 0

    attempts = await db_write(_update)
    if attempts >= MAX_SEND_ATTEMPTS:
        def _give_up(conn):
            conn.execute(
                """UPDATE dm_records SET status = 'failed', updated_at = ?
                   WHERE rule_id = ? AND recipient_user_id = ?""",
                (now_iso(), rule_id, user_id),
            )
        await db_write(_give_up)


def _future_iso(delay_seconds: float) -> str:
    return datetime.fromtimestamp(time.time() + delay_seconds, tz=timezone.utc).isoformat()


# --------------------------------------------------------------------------
# Background loops
# --------------------------------------------------------------------------

async def comment_worker_loop():
    """Fast-path: drains the in-memory queue as comments arrive."""
    while True:
        comment_id = await work_queue.get()
        try:
            await process_comment(comment_id)
        except Exception as e:
            print(f"[comment_worker] error processing {comment_id}: {e}")
        finally:
            work_queue.task_done()


async def startup_sweep_loop():
    """
    Runs once at startup (and is cheap enough to run periodically too):
    picks up any comment that was persisted but never marked processed —
    this is what makes a mid-processing restart non-fatal.
    """
    def _unprocessed(conn):
        rows = conn.execute(
            "SELECT comment_id FROM comments WHERE processed = 0"
        ).fetchall()
        return [r["comment_id"] for r in rows]

    ids = await db_read(_unprocessed)
    for cid in ids:
        await work_queue.put(cid)


async def reconciliation_loop():
    """
    Every few seconds: for every dm_record that's 'queued' (API accepted but
    not confirmed), poll PseudoGram for real status. For every 'pending' or
    due-for-retry record, retry the send. This is what catches "202 now,
    failed later" and "we crashed mid-backoff".
    """
    while True:
        await asyncio.sleep(RECONCILE_INTERVAL_SECONDS)
        try:
            await _reconcile_queued()
            await _reconcile_retryable()
        except Exception as e:
            print(f"[reconciliation] error: {e}")


async def _reconcile_queued():
    def _fetch(conn):
        rows = conn.execute(
            "SELECT * FROM dm_records WHERE status = 'queued' AND dm_id IS NOT NULL"
        ).fetchall()
        return [dict(r) for r in rows]

    records = await db_read(_fetch)
    for rec in records:
        result = await pseudogram_get_dm_status(rec["dm_id"])
        if result["status_code"] != 200:
            continue
        status = result["body"].get("status")
        if status == "delivered":
            def _mark_delivered(conn, rec=rec):
                conn.execute(
                    "UPDATE dm_records SET status = 'delivered', updated_at = ? WHERE id = ?",
                    (now_iso(), rec["id"]),
                )
            await db_write(_mark_delivered)
        elif status == "failed":
            # Accepted, then failed. Retry it (fresh idempotency key so
            # PseudoGram treats it as a new send attempt, not the same
            # doomed one).
            new_key = f"{rec['idempotency_key']}:retry{rec['attempts']}"

            def _reset_for_retry(conn, rec=rec, new_key=new_key):
                conn.execute(
                    """UPDATE dm_records SET status = 'pending', idempotency_key = ?,
                       updated_at = ? WHERE id = ?""",
                    (new_key, now_iso(), rec["id"]),
                )
            await db_write(_reset_for_retry)


async def _reconcile_retryable():
    def _fetch(conn):
        rows = conn.execute(
            """SELECT * FROM dm_records
               WHERE status = 'pending'
                 AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                 AND attempts < ?""",
            (now_iso(), MAX_SEND_ATTEMPTS),
        ).fetchall()
        return [dict(r) for r in rows]

    records = await db_read(_fetch)
    for rec in records:
        def _lookup_rule(conn, rec=rec):
            r = conn.execute("SELECT * FROM rules WHERE rule_id = ?", (rec["rule_id"],)).fetchone()
            return dict(r) if r else None

        rule = await db_read(_lookup_rule)
        if not rule:
            continue
        await attempt_send(rec["rule_id"], rec["recipient_user_id"], rule["dm_message"],
                            rec["comment_id"], rec["idempotency_key"])


# --------------------------------------------------------------------------
# FastAPI app
# --------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client, work_queue, rate_limiter, _write_lock
    _init_db()
    http_client = httpx.AsyncClient()
    # Created here (not at import time) so they always bind to whichever
    # event loop is actually running this app instance. asyncio.Queue/Lock
    # bind lazily to the first loop that touches them; creating them at
    # module-import time would tie them to a loop that may not be the one
    # serving requests (this bit us across repeated test runs / TestClient
    # instances, each of which spins up its own loop).
    work_queue = asyncio.Queue()
    rate_limiter = RateLimiter(RATE_LIMIT_MAX_CALLS, RATE_LIMIT_WINDOW_SECONDS)
    _write_lock = asyncio.Lock()
    worker_task = asyncio.create_task(comment_worker_loop())
    reconcile_task = asyncio.create_task(reconciliation_loop())
    await startup_sweep_loop()
    yield
    worker_task.cancel()
    reconcile_task.cancel()
    await http_client.aclose()


app = FastAPI(lifespan=lifespan)


class RuleIn(BaseModel):
    keyword: str
    dm_message: str


@app.post("/rules", status_code=201)
async def create_rule(rule: RuleIn):
    rule_id = f"rule_{int(time.time() * 1000)}_{os.urandom(3).hex()}"

    def _insert(conn):
        conn.execute(
            "INSERT INTO rules (rule_id, keyword, dm_message, created_at) VALUES (?, ?, ?, ?)",
            (rule_id, rule.keyword, rule.dm_message, now_iso()),
        )

    await db_write(_insert)
    return {"rule_id": rule_id, "keyword": rule.keyword, "dm_message": rule.dm_message}


@app.post("/webhook")
async def webhook(request: Request, response: Response):
    raw_body = await request.body()
    signature = request.headers.get("X-PseudoGram-Signature")

    if not verify_signature(raw_body, signature):
        response.status_code = 401
        return {"error": "invalid_signature"}

    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError:
        response.status_code = 400
        return {"error": "invalid_json"}

    event_id = payload.get("event_id")
    event_type = payload.get("event_type")
    data = payload.get("data", {})

    def _record_event(conn) -> bool:
        """Returns True if this is the first time we've seen event_id."""
        try:
            conn.execute(
                "INSERT INTO events_seen (event_id, event_type, first_seen_at) VALUES (?, ?, ?)",
                (event_id, event_type, now_iso()),
            )
            return True
        except sqlite3.IntegrityError:
            conn.execute(
                "UPDATE events_seen SET redelivery_count = redelivery_count + 1 WHERE event_id = ?",
                (event_id,),
            )
            conn.execute(
                "INSERT INTO duplicate_log (reason, event_id, logged_at) VALUES ('duplicate_event', ?, ?)",
                (event_id, now_iso()),
            )
            return False

    first_time = await db_write(_record_event)
    if not first_time:
        # Already processed (or in flight) — 200 OK, do nothing further.
        return {"ok": True, "duplicate": True}

    if event_type == "comment.created":
        comment_id = data.get("comment_id")

        def _insert_comment(conn):
            conn.execute(
                """INSERT OR IGNORE INTO comments
                   (comment_id, post_id, text, user_id, username, created_at, deleted, processed, received_at)
                   VALUES (?, ?, ?, ?, ?, ?, 0, 0, ?)""",
                (
                    comment_id,
                    data.get("post_id"),
                    data.get("text", ""),
                    data.get("from", {}).get("user_id"),
                    data.get("from", {}).get("username"),
                    data.get("created_at"),
                    now_iso(),
                ),
            )

        await db_write(_insert_comment)
        await work_queue.put(comment_id)

    elif event_type == "comment.deleted":
        comment_id = data.get("comment_id")

        def _mark_deleted(conn):
            # If the comment row doesn't exist yet (deletion arrived before
            # creation — order isn't guaranteed), insert a placeholder
            # that's already marked deleted+processed so a later
            # comment.created for the same id is a no-op.
            existing = conn.execute(
                "SELECT comment_id FROM comments WHERE comment_id = ?", (comment_id,)
            ).fetchone()
            if existing:
                conn.execute(
                    "UPDATE comments SET deleted = 1 WHERE comment_id = ?", (comment_id,)
                )
            else:
                conn.execute(
                    """INSERT INTO comments
                       (comment_id, text, user_id, deleted, processed, received_at)
                       VALUES (?, '', '', 1, 1, ?)""",
                    (comment_id, now_iso()),
                )

        await db_write(_mark_deleted)
        # Note: if a DM was already sent for this comment before the delete
        # arrived, we deliberately do not claw it back — see FAILURES.md.

    return {"ok": True}


@app.get("/stats")
async def stats():
    def _compute(conn):
        sent = conn.execute("SELECT COUNT(*) c FROM dm_records WHERE status = 'delivered'").fetchone()["c"]
        failed = conn.execute("SELECT COUNT(*) c FROM dm_records WHERE status = 'failed'").fetchone()["c"]
        queued = conn.execute(
            "SELECT COUNT(*) c FROM dm_records WHERE status IN ('queued', 'pending')"
        ).fetchone()["c"]
        duplicates_blocked = conn.execute("SELECT COUNT(*) c FROM duplicate_log").fetchone()["c"]
        return {"sent": sent, "failed": failed, "queued": queued, "duplicates_blocked": duplicates_blocked}

    return await db_read(_compute)


@app.get("/health")
async def health():
    return {"ok": True}

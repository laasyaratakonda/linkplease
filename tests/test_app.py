"""
Runs entirely offline: monkeypatches the two functions that talk to
PseudoGram (pseudogram_send_dm, pseudogram_get_dm_status) so you can verify
the dedup/matching/retry logic without spending real API calls.

Run with:  pytest tests/test_app.py -v
(from the repo root, after `pip install -r requirements.txt pytest pytest-asyncio`)
"""

import asyncio
import hashlib
import hmac
import json
import os

import pytest
from fastapi.testclient import TestClient

os.environ["PSEUDOGRAM_API_KEY"] = "test-secret-key"
os.environ["DB_PATH"] = "test_linkplease.db"

from app import main  # noqa: E402


def sign(body: bytes) -> str:
    mac = hmac.new(b"test-secret-key", body, hashlib.sha256).hexdigest()
    return f"sha256={mac}"


@pytest.fixture(autouse=True)
def fresh_db():
    for suffix in ("", "-wal", "-shm"):
        path = f"test_linkplease.db{suffix}"
        if os.path.exists(path):
            os.remove(path)
    main._init_db()
    yield
    for suffix in ("", "-wal", "-shm"):
        path = f"test_linkplease.db{suffix}"
        if os.path.exists(path):
            os.remove(path)


@pytest.fixture
def client(monkeypatch):
    # Fake PseudoGram: always accepts immediately, "delivers" on first poll.
    async def fake_send(recipient_user_id, message, comment_id, idempotency_key):
        return {"status_code": 202, "body": {"dm_id": f"dm_{idempotency_key}", "status": "queued"}, "headers": {}}

    async def fake_get_status(dm_id):
        return {"status_code": 200, "body": {"dm_id": dm_id, "status": "delivered"}}

    monkeypatch.setattr(main, "pseudogram_send_dm", fake_send)
    monkeypatch.setattr(main, "pseudogram_get_dm_status", fake_get_status)

    with TestClient(main.app) as c:
        yield c


def make_event(event_id, text, user_id="usr_1", comment_id="cmt_1", event_type="comment.created"):
    return {
        "event_id": event_id,
        "event_type": event_type,
        "sent_at": "2026-08-19T10:00:00.000Z",
        "data": {
            "comment_id": comment_id,
            "post_id": "post_1",
            "text": text,
            "created_at": "2026-08-19T10:00:00.000Z",
            "from": {"user_id": user_id, "username": "someuser"},
        },
    }


def post_webhook(client, event):
    body = json.dumps(event).encode()
    return client.post("/webhook", content=body, headers={
        "X-PseudoGram-Signature": sign(body),
        "Content-Type": "application/json",
    })


def test_rule_creation(client):
    r = client.post("/rules", json={"keyword": "PRICE", "dm_message": "here's the price list"})
    assert r.status_code == 201
    body = r.json()
    assert body["keyword"] == "PRICE"
    assert "rule_id" in body


def test_bad_signature_rejected(client):
    event = make_event("evt_1", "PRICE please")
    body = json.dumps(event).encode()
    r = client.post("/webhook", content=body, headers={
        "X-PseudoGram-Signature": "sha256=deadbeef",
        "Content-Type": "application/json",
    })
    assert r.status_code == 401


def test_matching_and_send(client):
    client.post("/rules", json={"keyword": "PRICE", "dm_message": "here's the price list"})
    r = post_webhook(client, make_event("evt_1", "PRICE please 🙏"))
    assert r.status_code == 200

    import time
    time.sleep(2.5)  # let the background worker + reconciliation loop run

    stats = client.get("/stats").json()
    assert stats["sent"] == 1, stats


def test_duplicate_event_id_blocked(client):
    client.post("/rules", json={"keyword": "PRICE", "dm_message": "..."})
    post_webhook(client, make_event("evt_1", "PRICE please"))
    post_webhook(client, make_event("evt_1", "PRICE please"))  # redelivery, same event_id

    import time
    time.sleep(2.5)

    stats = client.get("/stats").json()
    assert stats["sent"] == 1
    assert stats["duplicates_blocked"] >= 1


def test_same_user_two_comments_same_rule_only_one_dm(client):
    client.post("/rules", json={"keyword": "PRICE", "dm_message": "..."})
    post_webhook(client, make_event("evt_1", "PRICE please", user_id="usr_1", comment_id="cmt_1"))
    post_webhook(client, make_event("evt_2", "price again", user_id="usr_1", comment_id="cmt_2"))

    import time
    time.sleep(2.5)

    stats = client.get("/stats").json()
    assert stats["sent"] == 1
    assert stats["duplicates_blocked"] >= 1


def test_comment_deleted_before_processed_skips_dm(client):
    client.post("/rules", json={"keyword": "PRICE", "dm_message": "..."})
    # created and deleted for the same comment_id, deleted arrives "first"
    # in the sense that we post it before the worker has a chance to run
    post_webhook(client, {
        "event_id": "evt_del",
        "event_type": "comment.deleted",
        "sent_at": "2026-08-19T10:00:00.000Z",
        "data": {"comment_id": "cmt_x"},
    })
    post_webhook(client, make_event("evt_created", "PRICE please", comment_id="cmt_x"))

    import time
    time.sleep(2.5)

    stats = client.get("/stats").json()
    assert stats["sent"] == 0

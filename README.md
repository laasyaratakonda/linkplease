# LinkPlease

Instagram comment → DM automation on top of the PseudoGram mock API.

## Stack
Python, FastAPI, SQLite (WAL mode), httpx. No external services required —
everything (dedup, retries, rate limiting, reconciliation) is in-process.

## How it works (short version)
- `POST /webhook` verifies the signature, dedupes on `event_id`, persists the
  raw event, and returns `200` immediately. Matching + sending happens in a
  background task.
- Duplicate DMs are prevented by a `UNIQUE(rule_id, recipient_user_id)`
  constraint in SQLite — that's the actual guarantee, not just event-id dedup.
- A reconciliation loop polls PseudoGram every few seconds for any DM that's
  `queued` (accepted but not confirmed) or due for retry, with exponential
  backoff and `429 Retry-After` handling.
- On startup, any comment that was persisted but never marked `processed` is
  re-queued — so a restart mid-processing doesn't lose it.

Full breakdown of what's NOT covered is in `FAILURES.md` — read that before
you assume this is bulletproof. It isn't, and the assignment doesn't want it
to pretend to be.

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Set your API key (get it via `/v1/apply` then `/v1/keygen` per the assignment doc):

```bash
export PSEUDOGRAM_API_KEY="your-key-here"
```

Run locally:

```bash
uvicorn app.main:app --reload --port 8000
```

## Testing offline (no network, no API key needed)

```bash
pip install pytest pytest-asyncio
pytest tests/test_app.py -v
```

This mocks the PseudoGram calls entirely, so it verifies the dedup/matching/
retry *logic* without spending real requests. It does NOT prove your
deployment handles the real API's actual failure modes — for that, use the
simulator (next section).

## Testing against the real mock API

Once deployed (or running locally with a tunnel like `ngrok`/`cloudflared`
if you want to test before deploying):

```bash
curl -X POST https://pseudogram-api.onrender.com/v1/simulate/start \
  -H "X-API-Key: $PSEUDOGRAM_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"webhook_url": "https://your-app.example.com/webhook", "count": 500, "duration_seconds": 10}'
```

Then compare `GET /stats` on your app against:

```bash
curl https://pseudogram-api.onrender.com/v1/simulate/{run_id}/truth \
  -H "X-API-Key: $PSEUDOGRAM_API_KEY"
```

Run this several times before submitting. If your numbers don't match, that's
exactly what `FAILURES.md` is for — figure out why, and either fix it or
write it down honestly.

## Deploying to Render

This repo includes a `Dockerfile` and `render.yaml`.

1. Push this repo to GitHub.
2. On Render: New → Blueprint → point at the repo. It'll read `render.yaml`.
3. Set the `PSEUDOGRAM_API_KEY` env var in the Render dashboard (marked
   `sync: false` in the blueprint so it's not committed to git).
4. **Important:** `render.yaml` mounts a persistent disk at `/app/data` for
   the SQLite file. If you're on a plan where persistent disks aren't
   available, the DB resets on every restart/redeploy — see `FAILURES.md`,
   this is a real gap, not a hypothetical one, if that's your situation.
5. Once deployed, run the simulator against your live `working_url` before
   you submit.

## Known gaps

See `FAILURES.md`. Read it. It's graded.

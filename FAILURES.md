# Failure Modes

This document describes known failure modes and the tradeoffs in the LinkPlease implementation.

Process crash during a retry: DM attempts and retry state are persisted in SQLite. A restart can recover persisted pending work, but an unexpected crash between an external API call and the corresponding database update can leave a record requiring reconciliation. Idempotency keys reduce the risk of sending the same DM twice.

Duplicate or concurrent comment events: Incoming events are deduplicated using the persistent event_id table. DM delivery is additionally protected by a uniqueness constraint on (rule_id, recipient_user_id), so repeated comments or redeliveries do not cause the same user to receive the same rule's DM twice.

Repeated API failures: HTTP 429 and 500 responses are retried with backoff. The 429 Retry-After value is honored, and attempts are capped at six. If the maximum number of attempts is reached, the DM is marked as failed rather than being retried forever.

Accepted DM that later fails: PseudoGram can accept a DM before ultimately reporting delivery failure. The application stores the returned dm_id and periodically reconciles queued deliveries using the DM-status endpoint. A later failure is converted back to a pending retry with a fresh idempotency key.

Rate limiting: Outgoing DM requests pass through a shared sliding-window limiter configured for 10 requests per 60 seconds. This intentionally trades throughput for compliance with the API rate limit.

Comment deletion: If a deletion arrives after a DM has already been sent, the implementation does not attempt to retract the DM because the mock API does not provide a retraction operation. If deletion arrives before comment creation, a deleted placeholder prevents the later creation event from triggering processing.

Statistics during active processing: /stats reports the persisted current state of DM records. During an active load, counts can change between successive requests as background workers process retries and reconcile delivery status.

Historical test records: The local SQLite database can contain failures from earlier test runs or earlier versions of the implementation. Final evaluation should use a clean/reproducible deployed environment rather than treating historical local records as proof of current behavior.

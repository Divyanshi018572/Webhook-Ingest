# Solution Document: Webhook Ingestion Service

## 1. What Was Broken & How I Fixed It

### Defect 1: Duplicate Calls & Double-Counted Stats
- **Problem**: When two identical webhooks arrived at the same time, both checked if the event existed before either finished saving it. Both thought it was new, so both inserted the event and incremented stats twice.
- **How I Fixed It**:
  1. Added a `UNIQUE (event_id)` constraint on the `events` table in `migrations/002_idempotency.sql`.
  2. Combined event insertion, call updating, and stats increment into a single atomic PostgreSQL transaction (`IngestEventTx`).
  3. Used `ON CONFLICT (event_id) DO NOTHING` — if the event was already stored, the database immediately ignores the duplicate and skips stats increment.

### Defect 2: Recordings Never Marked Processed & Silent Errors
- **Problem**: The background goroutine was given the HTTP request context. As soon as the API sent `200 OK` back to the provider, Go canceled that context, causing the database update to fail. The error was also ignored with `// TODO: handle`.
- **How I Fixed It**:
  1. Used `context.WithoutCancel(ctx)` so the background worker continues running even after the HTTP response is sent.
  2. Added proper error logging with `s.log.Error(...)` instead of ignoring failures.

### Defect 3: In-Flight Jobs Lost on Deploy
- **Problem**: When the server received a shutdown signal (`SIGTERM`), it stopped the HTTP server and exited immediately, killing background recording workers mid-way.
- **How I Fixed It**:
  1. Added a `sync.WaitGroup` to track running background workers (`wg.Add(1)` / `wg.Done()`).
  2. Added a `Close()` method that waits for active workers to finish before the application shuts down.

### Defect 4: Stats Reset to Zero After Server Restart
- **Problem**: The in-memory cache started empty on restart. When someone requested stats, it returned 0 because it never checked PostgreSQL. Concurrent writes also lacked thread-safety locks.
- **How I Fixed It**:
  1. Added mutex locks (`c.mu.Lock()`) around cache updates and added a `Set()` method.
  2. Updated `Service.Stats()` with read-through fallback: if the cache is empty, fetch the durable counts from PostgreSQL and update the cache.

---

## 2. Deduplication Strategy: Why PostgreSQL Over Alternatives

- **Why Not In-Memory**: In-memory sets are lost whenever the server restarts and cannot be shared if we run multiple server instances.
- **Why Not Redis (`SETNX`)**: Using Redis locks alongside PostgreSQL adds a second system that can fail independently (network timeouts, lock expiration before database write, or keys lost under memory pressure).
- **Why PostgreSQL `UNIQUE` Constraint**: It handles deduplication directly in the database using ACID transactions. Inserting the event and updating stats happen together atomically with zero extra tools needed.

---

## 3. Scaling to 10,000 Webhooks / Second

At 10,000 requests/second, doing a synchronous database write on every HTTP request will overload PostgreSQL connections and disk. To handle this scale:

1. **Queue Webhooks First (Message Queue / Redis Stream / Kafka)**:
   - The API server only validates the incoming JSON, pushes it to a queue, and immediately returns `202 Accepted` to the provider in under 5ms.
2. **Batch Database Inserts**:
   - Background workers read events from the queue and write them to PostgreSQL in batches (e.g. 500 records per query) instead of one query per webhook. This cuts database load by over 90%.
3. **Use Redis for Fast Stats Counters**:
   - Increment per-account counters directly in Redis for fast reads (`INCRBY`), and periodically sync summary totals to PostgreSQL in the background.
4. **Separate Worker Pool for Slow Tasks**:
   - Run audio downloads and transcoding on a separate background worker queue with automatic retries so slow tasks don't block event ingestion.

package ingest_test

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// TestFlaw_ConcurrentDuplicateDeliveries demonstrates Defect #1:
// When duplicate webhooks with the same event_id arrive simultaneously,
// the check-then-act race condition causes duplicate event insertions and
// inflates the account call_count beyond 1.
func TestFlaw_ConcurrentDuplicateDeliveries(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	concurrency := 30

	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			<-start
			_ = post(t, srv.URL+"/webhooks/calls", body)
		}()
	}
	close(start)
	wg.Wait()

	var eventCount int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&eventCount); err != nil {
		t.Fatalf("scan events: %v", err)
	}
	if eventCount != 1 {
		t.Errorf("[FLAW DETECTED] events table stored %d rows for event %s, want 1", eventCount, eventID)
	}

	stats, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("get account stats: %v", err)
	}
	if stats.CallCount != 1 {
		t.Errorf("[FLAW DETECTED] account_stats.call_count is %d, want 1 (stats drifted due to concurrent duplicates)", stats.CallCount)
	}
}

// TestFlaw_RecordingNeverMarkedProcessed demonstrates Defect #2:
// When a webhook has a recording_url, the background goroutine uses the HTTP
// request context. As soon as the HTTP handler returns 200 OK, the context is
// canceled, causing processRecording to fail silently and never update
// recording_processed to true.
func TestFlaw_RecordingNeverMarkedProcessed(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	resp := post(t, srv.URL+"/webhooks/calls", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	time.Sleep(120 * time.Millisecond)

	var processed bool
	row := st.Pool().QueryRow(ctx, `SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan calls: %v", err)
	}

	if !processed {
		t.Errorf("[FLAW DETECTED] call %s has recording_processed = false, expected true after background processing", callID)
	}
}

// TestFlaw_GracefulShutdownWaitsForRecordingWorkers demonstrates Defect #3:
// On deploy (SIGTERM), the server must drain in-flight background recording
// goroutines before exiting. Without sync.WaitGroup tracking and Service.Close(),
// Close returns immediately and recording_processed stays false.
func TestFlaw_GracefulShutdownWaitsForRecordingWorkers(t *testing.T) {
	svc, st := testutil.NewService(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	evt := ingest.Event{
		EventID:      eventID,
		CallID:       callID,
		AccountID:    accountID,
		Status:       "completed",
		DurationSec:  143,
		RecordingURL: "https://recordings.example.com/" + callID + ".wav",
		OccurredAt:   time.Date(2026, 8, 13, 9, 12, 0, 0, time.UTC),
	}

	if err := svc.Ingest(ctx, evt); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	// Simulate deploy: shut down while the recording worker is still running
	// (recordingWork is 50ms; we do not sleep here).
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := svc.Close(shutdownCtx); err != nil {
		t.Fatalf("close: %v", err)
	}

	var processed bool
	row := st.Pool().QueryRow(ctx, `SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan calls: %v", err)
	}
	if !processed {
		t.Errorf("[FLAW DETECTED] recording_processed is false after Close(); graceful shutdown did not drain in-flight recording workers")
	}
}

// TestFlaw_CacheDriftAfterServerRestart demonstrates Defect #4:
// When the server restarts with existing data in PostgreSQL, the in-memory cache
// starts empty and returns 0 calls for accounts that have durable stats in PostgreSQL.
func TestFlaw_CacheDriftAfterServerRestart(t *testing.T) {
	srv, st := testutil.NewServer(t)
	_, _, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if err := st.IncrementAccountStats(ctx, accountID, 100); err != nil {
			t.Fatalf("seed stats: %v", err)
		}
	}

	resp, err := http.Get(srv.URL + "/accounts/" + accountID + "/stats")
	if err != nil {
		t.Fatalf("get stats: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var result struct {
		AccountID        string `json:"account_id"`
		CallCount        int64  `json:"call_count"`
		TotalDurationSec int64  `json:"total_duration_sec"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode stats: %v", err)
	}

	if result.CallCount != 5 {
		t.Errorf("[FLAW DETECTED] GET /accounts/%s/stats returned call_count = %d, want 5 (cache is out of sync with database)", accountID, result.CallCount)
	}
}

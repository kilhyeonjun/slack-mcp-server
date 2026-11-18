package handler

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/slack-go/slack"
)

func TestSchedulingSDKRoundTrip(t *testing.T) {
	const channel, parent, body = "D12345", "1999999999.000001", "Synthetic reservation"
	at := time.Now().Unix() + 1800
	var pending atomic.Bool
	var sends, cancels atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		switch r.URL.Path {
		case "/auth.test":
			fmt.Fprint(w, `{"ok":true,"user_id":"U12345","team_id":"T12345"}`)
		case "/chat.scheduledMessages.list":
			if r.Form.Get("channel") != channel {
				t.Error("missing scheduled list channel")
			}
			if pending.Load() {
				fmt.Fprintf(w, `{"ok":true,"scheduled_messages":[{"id":"QTEST","channel_id":%q,"post_at":%d,"text":%q}]}`, channel, at, body)
			} else {
				fmt.Fprint(w, `{"ok":true,"scheduled_messages":[]}`)
			}
		case "/chat.scheduleMessage":
			if r.Form.Get("channel") != channel || r.Form.Get("thread_ts") != parent || r.Form.Get("text") != body || r.Form.Get("post_at") != fmt.Sprint(at) {
				t.Error("reservation payload did not bind destination, thread, time and text")
			}
			sends.Add(1)
			pending.Store(true)
			fmt.Fprintf(w, `{"ok":true,"channel":%q,"scheduled_message_id":"QTEST"}`, channel)
		case "/chat.deleteScheduledMessage":
			if r.Form.Get("channel") != channel || r.Form.Get("scheduled_message_id") != "QTEST" {
				t.Error("cancellation payload did not bind reservation and channel")
			}
			cancels.Add(1)
			pending.Store(false)
			fmt.Fprint(w, `{"ok":true}`)
		default:
			t.Errorf("unexpected Slack API path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	client := slack.New("xoxp-synthetic", slack.OptionAPIURL(srv.URL+"/"))
	ctx := context.Background()
	args := map[string]any{"expected_user_id": "U99999", "expected_team_id": "T12345", "post_at": float64(at), "text": body, "thread_ts": parent}
	var request mcp.CallToolRequest
	request.Params.Arguments = args
	if _, err := scheduleMessage(ctx, client, channel, request); err == nil || sends.Load() != 0 {
		t.Fatal("wrong actor could create a reservation")
	}
	args["expected_user_id"] = "U12345"
	if _, err := scheduleMessage(ctx, client, channel, request); err != nil {
		t.Fatal(err)
	}
	if _, err := scheduleMessage(ctx, client, channel, request); err == nil || sends.Load() != 1 {
		t.Fatal("second write escaped channel/time duplicate guard")
	}
	args["scheduled_message_id"] = "QTEST"
	args["text_sha256"] = strings.Repeat("a", 64)
	if _, err := cancelScheduledMessage(ctx, client, channel, request); err == nil || cancels.Load() != 0 {
		t.Fatal("wrong text hash could cancel a reservation")
	}
	args["text_sha256"] = fmt.Sprintf("%x", sha256.Sum256([]byte(body)))
	if _, err := cancelScheduledMessage(ctx, client, channel, request); err != nil || cancels.Load() != 1 {
		t.Fatalf("exact reservation did not cancel: %v", err)
	}
	if got, err := pendingReservations(ctx, client, channel); err != nil || len(got) != 0 {
		t.Fatalf("cancellation was not reflected in queue: %v, %v", got, err)
	}
}

func TestSchedulingAllowsOnlyConfiguredLiteralChannels(t *testing.T) {
	if !scheduleAllowed("D12345", "D12345, C67890") {
		t.Fatal("explicit DM not recognized")
	}
	for _, cfg := range []string{"", "true", "!D67890", "D12345,C!", "D67890"} {
		if scheduleAllowed("D12345", cfg) {
			t.Fatalf("out-of-policy channel allowed with config %q", cfg)
		}
	}
}

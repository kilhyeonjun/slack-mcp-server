package handler

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/slack-go/slack"
)

var (
	scheduleChannel = regexp.MustCompile(`^[CDG][A-Z0-9]{2,}$`)
	scheduleUser    = regexp.MustCompile(`^[UW][A-Z0-9]{2,}$`)
	scheduleTeam    = regexp.MustCompile(`^T[A-Z0-9]{2,}$`)
	scheduleID      = regexp.MustCompile(`^(Q[A-Z0-9]{2,}|[0-9]{1,24})$`)
	scheduleHash    = regexp.MustCompile(`^[a-f0-9]{64}$`)
	scheduleMu      sync.Mutex
)

type slackScheduler interface {
	AuthTestContext(context.Context) (*slack.AuthTestResponse, error)
	GetScheduledMessagesContext(context.Context, *slack.GetScheduledMessagesParameters) ([]slack.ScheduledMessage, string, error)
	ScheduleMessageContext(context.Context, string, string, ...slack.MsgOption) (string, string, error)
	DeleteScheduledMessageContext(context.Context, *slack.DeleteScheduledMessageParameters) (bool, error)
}

type pendingReservation struct {
	ID       string `json:"scheduled_message_id"`
	PostAt   int64  `json:"post_at"`
	BodyHash string `json:"text_sha256"`
}

func scheduleResult(value any) (*mcp.CallToolResult, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return mcp.NewToolResultText(string(encoded)), nil
}

// Scheduling is opt-in even when explicitly selected, and requires exact allowed channel IDs.
func scheduleAllowed(channel, config string) bool {
	if !scheduleChannel.MatchString(channel) || config == "" {
		return false
	}
	allowed := false
	for _, id := range strings.Split(config, ",") {
		id = strings.TrimSpace(id)
		if !scheduleChannel.MatchString(id) {
			return false
		}
		allowed = allowed || id == channel
	}
	return allowed
}

func (ch *ConversationsHandler) scheduler(request mcp.CallToolRequest) (slackScheduler, string, error) {
	channel := request.GetString("channel_id", "")
	if len(ch.enabledTools) != 0 && !slices.Contains(ch.enabledTools, request.Params.Name) ||
		!scheduleAllowed(channel, os.Getenv("SLACK_MCP_SCHEDULING_CHANNELS")) {
		return nil, "", fmt.Errorf("Slack scheduling is not enabled for this channel")
	}
	provider, ok := ch.apiProvider.(interface{ SchedulingClient() *slack.Client })
	if !ok {
		return nil, "", fmt.Errorf("Slack scheduling requires a user OAuth connection")
	}
	client := provider.SchedulingClient()
	if client == nil {
		return nil, "", fmt.Errorf("Slack scheduling requires a user OAuth connection")
	}
	return client, channel, nil
}

func scheduleIdentity(ctx context.Context, client slackScheduler, request mcp.CallToolRequest) error {
	user, team := request.GetString("expected_user_id", ""), request.GetString("expected_team_id", "")
	if !scheduleUser.MatchString(user) || !scheduleTeam.MatchString(team) {
		return fmt.Errorf("Scheduling requires exact Slack user and workspace IDs")
	}
	identity, err := client.AuthTestContext(ctx)
	if err != nil || identity == nil || identity.UserID != user || identity.TeamID != team || identity.BotID != "" {
		return fmt.Errorf("Slack scheduling identity could not be confirmed")
	}
	return nil
}

func pendingReservations(ctx context.Context, client slackScheduler, channel string) ([]pendingReservation, error) {
	rows := make([]pendingReservation, 0)
	ids, cursors := map[string]bool{}, map[string]bool{}
	params := slack.GetScheduledMessagesParameters{Channel: channel, Limit: 100}
	for range 10 {
		page, next, err := client.GetScheduledMessagesContext(ctx, &params)
		if err != nil {
			return nil, fmt.Errorf("Slack scheduled-message list unavailable")
		}
		for _, row := range page {
			if row.Channel != channel || !scheduleID.MatchString(row.ID) || ids[row.ID] || row.PostAt <= 0 || row.Text == "" {
				return nil, fmt.Errorf("Conflicting Slack scheduled-message inventory")
			}
			ids[row.ID] = true
			rows = append(rows, pendingReservation{row.ID, int64(row.PostAt), fmt.Sprintf("%x", sha256.Sum256([]byte(row.Text)))})
		}
		if next == "" {
			return rows, nil
		}
		if cursors[next] || len(next) > 512 {
			return nil, fmt.Errorf("Invalid Slack scheduled-message pagination")
		}
		cursors[next], params.Cursor = true, next
	}
	return nil, fmt.Errorf("Slack scheduled-message inventory exceeds page limit")
}

func scheduleUnix(request mcp.CallToolRequest) int64 {
	switch v := request.GetArguments()["post_at"].(type) {
	case float64:
		if v > 0 && v < 1e12 && float64(int64(v)) == v {
			return int64(v)
		}
	case int:
		if v > 0 && int64(v) < 1e12 {
			return int64(v)
		}
	}
	return 0
}

func (ch *ConversationsHandler) ListScheduledMessages(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	client, channel, err := ch.scheduler(request)
	if err != nil {
		return nil, err
	}
	if err := scheduleIdentity(ctx, client, request); err != nil {
		return nil, err
	}
	rows, err := pendingReservations(ctx, client, channel)
	if err != nil {
		return nil, err
	}
	return scheduleResult(map[string]any{"channel_id": channel, "scheduled_messages": rows})
}

func (ch *ConversationsHandler) ScheduleMessage(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	client, channel, err := ch.scheduler(request)
	if err != nil {
		return nil, err
	}
	return scheduleMessage(ctx, client, channel, request)
}

func scheduleMessage(ctx context.Context, client slackScheduler, channel string, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	at, now := scheduleUnix(request), time.Now().Unix()
	text, thread := request.GetString("text", ""), request.GetString("thread_ts", "")
	if at <= now+60 || at > now+120*86400 || !utf8.ValidString(text) || strings.TrimSpace(text) == "" ||
		utf8.RuneCountInString(text) > 4000 || thread != "" && !messageTimestampPattern.MatchString(thread) {
		return nil, fmt.Errorf("Invalid Slack reservation text, thread or future delivery time")
	}
	scheduleMu.Lock()
	defer scheduleMu.Unlock()
	if err := scheduleIdentity(ctx, client, request); err != nil {
		return nil, err
	}
	rows, err := pendingReservations(ctx, client, channel)
	if err != nil {
		return nil, err
	}
	// Pending inventory does not attest a thread; refuse the whole channel/time slot on conflict.
	if slices.ContainsFunc(rows, func(row pendingReservation) bool { return row.PostAt == at }) {
		return nil, fmt.Errorf("Another Slack reservation already occupies this channel and time")
	}
	if at <= time.Now().Unix()+60 {
		return nil, fmt.Errorf("Too close to scheduled delivery; nothing scheduled")
	}
	options := []slack.MsgOption{slack.MsgOptionText(text, false), slack.MsgOptionDisableLinkUnfurl(), slack.MsgOptionDisableMediaUnfurl()}
	if thread != "" {
		options = append(options, slack.MsgOptionTS(thread))
	}
	actualChannel, id, err := client.ScheduleMessageContext(ctx, channel, strconv.FormatInt(at, 10), options...)
	if err != nil || actualChannel != channel || !scheduleID.MatchString(id) {
		return nil, fmt.Errorf("Slack scheduling outcome uncertain; check pending queue and destination before another write")
	}
	return scheduleResult(map[string]any{"channel_id": channel, "scheduled_message_id": id, "post_at": at})
}

func (ch *ConversationsHandler) CancelScheduledMessage(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	client, channel, err := ch.scheduler(request)
	if err != nil {
		return nil, err
	}
	return cancelScheduledMessage(ctx, client, channel, request)
}

func cancelScheduledMessage(ctx context.Context, client slackScheduler, channel string, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	id, digest, at := request.GetString("scheduled_message_id", ""), request.GetString("text_sha256", ""), scheduleUnix(request)
	if !scheduleID.MatchString(id) || !scheduleHash.MatchString(digest) || at == 0 {
		return nil, fmt.Errorf("Invalid Slack cancellation binding")
	}
	scheduleMu.Lock()
	defer scheduleMu.Unlock()
	if err := scheduleIdentity(ctx, client, request); err != nil {
		return nil, err
	}
	rows, err := pendingReservations(ctx, client, channel)
	if err != nil {
		return nil, err
	}
	if !slices.Contains(rows, pendingReservation{id, at, digest}) || at <= time.Now().Unix()+60 {
		return nil, fmt.Errorf("No matching cancellable Slack reservation; nothing cancelled")
	}
	ok, err := client.DeleteScheduledMessageContext(ctx, &slack.DeleteScheduledMessageParameters{
		Channel: channel, ScheduledMessageID: id, AsUser: true,
	})
	if err != nil || !ok {
		return nil, fmt.Errorf("Slack cancellation outcome uncertain; check the queue and destination before another write")
	}
	return scheduleResult(map[string]any{"channel_id": channel, "scheduled_message_id": id, "cancelled": true})
}

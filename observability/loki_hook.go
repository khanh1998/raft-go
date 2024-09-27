package observability

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// LokiHook struct for sending logs to Loki
type LokiHook struct {
	client LokiClient
	nodeId int
}

// LokiClient struct handles pushing logs to Loki
type LokiClient struct {
	url string
}

func NewLokiClient(url string) LokiClient {
	return LokiClient{url}
}

func NewLokiHook(client LokiClient, nodeId int) *LokiHook {
	return &LokiHook{client, nodeId}
}

// Push sends log data to Loki with structured metadata in the log line
func (c *LokiClient) Push(labels map[string]string, timestamp time.Time, message string, metadata map[string]string) error {
	// Prepare the payload for Loki
	payload := map[string]interface{}{
		"streams": []map[string]interface{}{
			{
				"stream": labels,
				"values": [][]interface{}{
					{strconv.FormatInt(timestamp.UnixNano(), 10), message, metadata},
				},
			},
		},
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := http.Post(c.url, "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("failed to send log to Loki: %s", resp.Status)
	}

	return nil
}

// Write sends log data to Loki with metadata (level, request_id, etc.)
func (h *LokiHook) Write(p []byte) (n int, err error) {
	var logEntry map[string]interface{}
	if err := json.Unmarshal(p, &logEntry); err != nil {
		return 0, err
	}

	level, ok := logEntry["level"].(string)
	if !ok {
		return 0, fmt.Errorf("invalid log entry format: level")
	}
	delete(logEntry, "level")

	message, ok := logEntry["message"].(string)
	if !ok {
		return 0, fmt.Errorf("invalid log entry format: message")
	}
	delete(logEntry, "message")

	timestampStr, ok := logEntry["time"].(string)
	if !ok {
		return 0, fmt.Errorf("timestamp not found in log entry: time")
	}
	delete(logEntry, "time")

	timestamp, err := time.Parse(time.RFC3339Nano, timestampStr)
	if err != nil {
		return 0, fmt.Errorf("failed to parse timestamp: %v", err)
	}

	metadata := make(map[string]string)
	for key, value := range logEntry {
		metadata[key] = fmt.Sprintf("%v", value)
	}

	labels := map[string]string{
		"service_name": fmt.Sprintf("raft-node-%d", h.nodeId),
		"level":        level,
	}

	return len(p), h.client.Push(labels, timestamp, message, metadata)
}

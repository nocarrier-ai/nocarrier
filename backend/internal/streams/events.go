package streams

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

var ErrConflict = errors.New("subject moved past expected sequence")

func Last(ctx context.Context, js jetstream.JetStream, stream, subject string, v any) (uint64, error) {
	s, err := js.Stream(ctx, stream)
	if err != nil {
		return 0, err
	}
	raw, err := s.GetLastMsgForSubject(ctx, subject)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if err := json.Unmarshal(raw.Data, v); err != nil {
		return 0, fmt.Errorf("decode %s@%d: %w", subject, raw.Sequence, err)
	}
	return raw.Sequence, nil
}

func Append(ctx context.Context, js jetstream.JetStream, subject string, v any, expected uint64) (uint64, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}
	ack, err := js.Publish(ctx, subject, data, jetstream.WithExpectLastSequencePerSubject(expected))
	if err != nil {
		var apiErr *jetstream.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence {
			return 0, ErrConflict
		}
		return 0, err
	}
	return ack.Sequence, nil
}

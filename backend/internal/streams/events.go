package streams

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

var (
	ErrConflict  = errors.New("subject moved past expected sequence")
	ErrDuplicate = errors.New("message id already appended")
)

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

func Append(ctx context.Context, js jetstream.JetStream, subject string, v any, expected uint64, opts ...jetstream.PublishOpt) (uint64, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}
	opts = append([]jetstream.PublishOpt{jetstream.WithExpectLastSequencePerSubject(expected)}, opts...)
	ack, err := js.Publish(ctx, subject, data, opts...)
	if err != nil {
		var apiErr *jetstream.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence {
			return 0, ErrConflict
		}
		return 0, err
	}
	if ack.Duplicate {
		return ack.Sequence, ErrDuplicate
	}
	return ack.Sequence, nil
}

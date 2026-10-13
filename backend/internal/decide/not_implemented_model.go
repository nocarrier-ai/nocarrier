package decide

import (
	"context"
	"encoding/json"
	"errors"
)

// NotImplementedModel is the seam awaiting the real decision model.
type NotImplementedModel struct{}

func (NotImplementedModel) Decide(context.Context, string, int64) (json.RawMessage, json.RawMessage, error) {
	return nil, nil, errors.New("decision model not implemented")
}

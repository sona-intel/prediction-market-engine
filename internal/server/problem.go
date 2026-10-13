package server

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"net/http"
	"strconv"

	"pm/internal/store"
)

const (
	codeInvalidID    = "invalid_id"
	codeInvalidQuery = "invalid_query"
	codeNotFound     = "not_found"
	codeInternal     = "internal"
	codeUnavailable  = "upstream_unavailable"
	codeDeadline     = "deadline_exceeded"
)

var problemKinds = map[string]struct {
	status int
	title  string
}{
	codeInvalidID:    {http.StatusBadRequest, "Invalid identifier"},
	codeInvalidQuery: {http.StatusBadRequest, "Invalid query"},
	codeNotFound:     {http.StatusNotFound, "Not found"},
	codeInternal:     {http.StatusInternalServerError, "Internal error"},
	codeUnavailable:  {http.StatusServiceUnavailable, "Data source unavailable"},
	codeDeadline:     {http.StatusGatewayTimeout, "Deadline exceeded"},
}

// problem has the wire shape of services/internal/contract, so sona-api reads it with ReadProblem
type problem struct {
	Type        string `json:"type"`
	Title       string `json:"title"`
	Status      int    `json:"status"`
	Detail      string `json:"detail,omitzero"`
	Instance    string `json:"instance,omitzero"`
	Code        string `json:"code"`
	RetryAfterS int    `json:"retry_after_s,omitzero"`
}

func newProblem(code, detail, instance string) problem {
	kind := problemKinds[code]
	p := problem{
		Type:     "urn:sona:problem:" + code,
		Title:    kind.title,
		Status:   kind.status,
		Detail:   detail,
		Instance: instance,
		Code:     code,
	}
	if code == codeUnavailable {
		p.RetryAfterS = 1
	}
	return p
}

func writeProblem(w http.ResponseWriter, p problem) {
	// AllowInvalidUTF8 leaves no encode error to handle
	body, _ := json.Marshal(p, jsontext.AllowInvalidUTF8(true))
	h := w.Header()
	h.Set("Content-Type", "application/problem+json")
	h.Set("Cache-Control", "no-store")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	if p.RetryAfterS > 0 {
		h.Set("Retry-After", strconv.Itoa(p.RetryAfterS))
	}
	w.WriteHeader(p.Status)
	_, _ = w.Write(body)
}

type requestError struct{ code, detail string }

func (e *requestError) Error() string { return e.code + ": " + e.detail }

func problemFor(err error, instance string) problem {
	var bad *requestError
	switch {
	case errors.As(err, &bad):
		return newProblem(bad.code, bad.detail, instance)
	case errors.Is(err, errNotPolled):
		return newProblem(codeUnavailable, "The first Polymarket poll has not finished.", instance)
	case errors.Is(err, store.ErrNotBuilt):
		return newProblem(codeUnavailable, "The database is not ready.", instance)
	case errors.Is(err, store.ErrNotFound):
		return newProblem(codeNotFound, "Nothing was found for this identifier.", instance)
	case errors.Is(err, context.DeadlineExceeded):
		return newProblem(codeDeadline, "The deadline passed before the data was read.", instance)
	}
	return newProblem(codeInternal, "The request failed.", instance)
}

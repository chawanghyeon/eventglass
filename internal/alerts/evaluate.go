package alerts

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/query"
)

type Kind string

const (
	KindIssue     Kind = "issue"
	KindThreshold Kind = "threshold"
)

type IssueRule struct {
	Events []string `json:"events"`
}

type ThresholdRule struct {
	Expression    *string         `json:"expression,omitempty"`
	Filter        json.RawMessage `json:"filter,omitempty"`
	Kinds         []model.Kind    `json:"kinds"`
	WindowSeconds int             `json:"window_seconds"`
	Metric        string          `json:"metric"`
	Operator      string          `json:"operator"`
	Threshold     string          `json:"threshold"`
}

type Rule struct {
	Kind      Kind
	Bytes     []byte
	SHA256    string
	Issue     *IssueRule
	Threshold *ThresholdRule
	Filter    *query.Node
}

// ValidateRule is the shared control/API boundary for persisted alert rules.
// It returns stable JSON bytes so revisions and hashes cannot disagree about
// semantically identical set ordering.
func ValidateRule(kind Kind, raw []byte) (Rule, error) {
	if len(raw) < 2 || len(raw) > 32<<10 {
		return Rule{}, errors.New("alert rule size is invalid")
	}
	switch kind {
	case KindIssue:
		var value IssueRule
		if err := decodeStrict(raw, &value); err != nil {
			return Rule{}, err
		}
		if len(value.Events) < 1 || len(value.Events) > 2 {
			return Rule{}, errors.New("issue alert requires one or two events")
		}
		slices.Sort(value.Events)
		for index, event := range value.Events {
			if event != "created" && event != "regressed" || index > 0 && value.Events[index-1] == event {
				return Rule{}, errors.New("invalid issue alert event")
			}
		}
		canonical, _ := json.Marshal(value)
		return finishRule(Rule{Kind: kind, Issue: &value}, canonical), nil
	case KindThreshold:
		var value ThresholdRule
		if err := decodeStrict(raw, &value); err != nil {
			return Rule{}, err
		}
		if (value.Expression == nil) == (len(value.Filter) == 0) {
			return Rule{}, errors.New("threshold alert requires exactly one expression or filter")
		}
		if len(value.Kinds) < 1 || len(value.Kinds) > 3 || value.Metric != "count" || !slices.Contains([]int{60, 300, 900, 3600}, value.WindowSeconds) || !slices.Contains([]string{"gt", "ge", "lt", "le"}, value.Operator) {
			return Rule{}, errors.New("invalid threshold alert")
		}
		slices.Sort(value.Kinds)
		for index, recordKind := range value.Kinds {
			if recordKind != model.KindError && recordKind != model.KindLog && recordKind != model.KindTransaction || index > 0 && value.Kinds[index-1] == recordKind {
				return Rule{}, errors.New("invalid threshold alert kind")
			}
		}
		threshold, err := strconv.ParseInt(value.Threshold, 10, 64)
		if err != nil || threshold < 0 || strconv.FormatInt(threshold, 10) != value.Threshold {
			return Rule{}, errors.New("threshold must be a canonical nonnegative int64")
		}
		var filter *query.Node
		if value.Expression != nil {
			if strings.TrimSpace(*value.Expression) != *value.Expression || *value.Expression == "" {
				return Rule{}, errors.New("invalid threshold expression")
			}
			filter, err = query.ParseCEL(*value.Expression)
		} else {
			filter, err = query.DecodeJSON(value.Filter)
		}
		if err != nil {
			return Rule{}, fmt.Errorf("invalid threshold filter: %w", err)
		}
		canonical, _ := json.Marshal(value)
		result := finishRule(Rule{Kind: kind, Threshold: &value, Filter: filter}, canonical)
		return result, nil
	default:
		return Rule{}, errors.New("invalid alert kind")
	}
}

func decodeStrict(raw []byte, destination any) error {
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return errors.New("invalid alert rule")
	}
	var trailing any
	if decoder.Decode(&trailing) == nil {
		return errors.New("trailing alert rule JSON")
	}
	return nil
}

func rejectDuplicateJSONKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	var parse func() error
	parse = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delimiter {
		case '{':
			seen := map[string]bool{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok || seen[key] {
					return errors.New("duplicate alert rule field")
				}
				seen[key] = true
				if err := parse(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		case '[':
			for decoder.More() {
				if err := parse(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		}
		return nil
	}
	if err := parse(); err != nil {
		return err
	}
	var trailing any
	if decoder.Decode(&trailing) == nil {
		return errors.New("trailing alert rule JSON")
	}
	return nil
}

func finishRule(rule Rule, canonical []byte) Rule {
	digest := sha256.Sum256(canonical)
	rule.Bytes = canonical
	rule.SHA256 = hex.EncodeToString(digest[:])
	return rule
}

func FirstWindowEnd(createdUS int64, windowSeconds int) (int64, error) {
	if createdUS < 0 || !slices.Contains([]int{60, 300, 900, 3600}, windowSeconds) {
		return 0, errors.New("invalid first alert window")
	}
	const minuteUS int64 = 60 * 1_000_000
	next := (createdUS/minuteUS + 1) * minuteUS
	return next + int64(windowSeconds)*1_000_000, nil
}

func CompareCount(observed int64, operator, threshold string) (bool, error) {
	if observed < 0 {
		return false, errors.New("observed count is negative")
	}
	want, err := strconv.ParseInt(threshold, 10, 64)
	if err != nil || want < 0 {
		return false, errors.New("invalid threshold")
	}
	switch operator {
	case "gt":
		return observed > want, nil
	case "ge":
		return observed >= want, nil
	case "lt":
		return observed < want, nil
	case "le":
		return observed <= want, nil
	default:
		return false, errors.New("invalid threshold operator")
	}
}

func CooldownElapsed(previousEnd *int64, currentEnd int64, cooldownSeconds int) bool {
	return previousEnd == nil || currentEnd-*previousEnd >= int64(cooldownSeconds)*1_000_000
}

type ThresholdBody struct {
	WindowStartUS string `json:"window_start_us"`
	WindowEndUS   string `json:"window_end_us"`
	Observed      string `json:"observed"`
	Operator      string `json:"operator"`
	Threshold     string `json:"threshold"`
}

type IssueBody struct {
	IssueID    string `json:"issue_id"`
	Transition string `json:"transition"`
	Revision   string `json:"revision"`
	Title      string `json:"title"`
}

type WebhookBody struct {
	Version       int            `json:"version"`
	DeliveryID    string         `json:"delivery_id"`
	AlertID       string         `json:"alert_id"`
	AlertRevision string         `json:"alert_revision"`
	ProjectID     string         `json:"project_id"`
	Type          Kind           `json:"type"`
	OccurredAt    time.Time      `json:"occurred_at"`
	Issue         *IssueBody     `json:"issue,omitempty"`
	Threshold     *ThresholdBody `json:"threshold,omitempty"`
	Link          string         `json:"link"`
}

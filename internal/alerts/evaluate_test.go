package alerts

import (
	"testing"
)

func TestThresholdRuleWindowAndCooldown(t *testing.T) {
	rule, err := ValidateRule(KindThreshold, []byte(`{"expression":"level == \"error\"","kinds":["log","error"],"window_seconds":60,"metric":"count","operator":"ge","threshold":"2"}`))
	if err != nil || rule.SHA256 == "" || rule.Threshold == nil {
		t.Fatalf("validate threshold: %+v %v", rule, err)
	}
	end, err := FirstWindowEnd(61_000_000, 60)
	if err != nil || end != 180_000_000 {
		t.Fatalf("first complete window = %d, %v", end, err)
	}
	if fired, _ := CompareCount(2, "ge", "2"); !fired {
		t.Fatal("inclusive threshold did not fire")
	}
	previous := int64(120_000_000)
	if CooldownElapsed(&previous, 179_999_999, 60) || !CooldownElapsed(&previous, 180_000_000, 60) {
		t.Fatal("cooldown must use aligned window end")
	}
}

func TestRuleValidationRejectsAmbiguousAndInvalidRules(t *testing.T) {
	bad := []struct {
		kind Kind
		raw  string
	}{
		{KindIssue, `{"events":["created","created"]}`},
		{KindThreshold, `{"expression":"true","filter":{"op":"constant","value":true},"kinds":["error"],"window_seconds":60,"metric":"count","operator":"gt","threshold":"0"}`},
		{KindThreshold, `{"expression":"true","kinds":["error"],"window_seconds":60,"metric":"count","operator":"gt","threshold":"01"}`},
		{KindIssue, `{"events":["created"],"events":["regressed"]}`},
	}
	for _, test := range bad {
		if _, err := ValidateRule(test.kind, []byte(test.raw)); err == nil {
			t.Fatalf("accepted invalid rule %s", test.raw)
		}
	}
}

package schedule

import (
	"fmt"
	"time"
)

func Due(scheduleType, expression, timezone, policy string, firstDue, now time.Time, maxCatchUp int) ([]time.Time, time.Time, error) {
	if firstDue.After(now) {
		return nil, firstDue, nil
	}
	if scheduleType == "fixed_delay" {
		if now.Sub(firstDue) > 2*time.Second {
			switch policy {
			case "skip":
				next, err := Next(scheduleType, expression, timezone, now)
				return nil, next, err
			case "fire_once", "catch_up":
				return []time.Time{now}, time.Time{}, nil
			default:
				return nil, time.Time{}, fmt.Errorf("unsupported misfire policy %q", policy)
			}
		}
		return []time.Time{firstDue}, time.Time{}, nil
	}
	due := []time.Time{firstDue}
	misfired := now.Sub(firstDue) > 2*time.Second
	switch {
	case misfired && policy == "skip":
		due = nil
	case misfired && policy == "fire_once":
		due = []time.Time{now}
	case policy == "catch_up":
		if maxCatchUp < 1 {
			maxCatchUp = 10
		}
	default:
		if policy != "skip" && policy != "fire_once" {
			return nil, time.Time{}, fmt.Errorf("unsupported misfire policy %q", policy)
		}
	}
	next := firstDue
	for !next.IsZero() && !next.After(now) {
		candidate, err := Next(scheduleType, expression, timezone, next)
		if err != nil {
			return nil, time.Time{}, err
		}
		next = candidate
		if policy == "catch_up" && !next.IsZero() && !next.After(now) && len(due) < maxCatchUp {
			due = append(due, next)
		}
	}
	return due, next, nil
}

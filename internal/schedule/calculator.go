package schedule

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

var parser = cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

func Preview(scheduleType, expression, timezone string, after time.Time, count int) ([]time.Time, error) {
	if count < 1 || count > 100 {
		return nil, fmt.Errorf("preview count must be between 1 and 100")
	}
	result := make([]time.Time, 0, count)
	cursor := after
	for range count {
		next, err := Next(scheduleType, expression, timezone, cursor)
		if err != nil {
			return nil, err
		}
		if next.IsZero() {
			break
		}
		result = append(result, next)
		cursor = next
	}
	return result, nil
}

func Next(scheduleType, expression, timezone string, after time.Time) (time.Time, error) {
	location, err := time.LoadLocation(timezone)
	if err != nil {
		return time.Time{}, fmt.Errorf("load timezone: %w", err)
	}
	switch scheduleType {
	case "cron":
		if err := validateCronExpression(expression); err != nil {
			return time.Time{}, err
		}
		if rule, ok, err := parseQuartzCalendarRule(expression); err != nil {
			return time.Time{}, err
		} else if ok {
			return nextQuartzCalendar(expression, rule, location, after)
		}
		fields := strings.Fields(expression)
		fields[5], err = normalizeQuartzDayOfWeek(fields[5])
		if err != nil {
			return time.Time{}, err
		}
		expression = strings.Join(fields, " ")
		s, err := parser.Parse(expression)
		if err != nil {
			return time.Time{}, fmt.Errorf("parse cron: %w", err)
		}
		return s.Next(after.In(location)).UTC(), nil
	case "once":
		t, err := time.ParseInLocation(time.RFC3339, expression, location)
		if err != nil {
			return time.Time{}, fmt.Errorf("parse once schedule: %w", err)
		}
		if !t.After(after) {
			return time.Time{}, nil
		}
		return t.UTC(), nil
	case "fixed_interval", "fixed_rate", "fixed_delay":
		seconds, err := strconv.ParseInt(expression, 10, 64)
		if err != nil || seconds < 1 {
			return time.Time{}, fmt.Errorf("fixed schedule must be positive seconds")
		}
		return after.Add(time.Duration(seconds) * time.Second).UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("unsupported schedule type %q", scheduleType)
	}
}

func validateCronExpression(expression string) error {
	fields := strings.Fields(expression)
	if len(fields) != 6 {
		return fmt.Errorf("cron expression must contain exactly 6 fields including seconds")
	}
	questionMarks := 0
	for index, field := range fields {
		if !strings.Contains(field, "?") {
			continue
		}
		if (index != 3 && index != 5) || field != "?" {
			return fmt.Errorf("cron ? is only supported as the complete day-of-month or day-of-week field")
		}
		questionMarks++
	}
	if questionMarks > 1 {
		return fmt.Errorf("cron ? may appear in only one day field")
	}
	_, calendarRule, err := parseQuartzCalendarRule(expression)
	if err != nil {
		return err
	}
	if !calendarRule {
		if _, err = normalizeQuartzDayOfWeek(fields[5]); err != nil {
			return err
		}
	}
	return nil
}

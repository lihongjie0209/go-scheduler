package schedule

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type quartzCalendarKind uint8

const (
	quartzLastDay quartzCalendarKind = iota + 1
	quartzNearestWeekday
	quartzLastWeekdayOfMonth
	quartzLastDayOfWeek
	quartzNthDayOfWeek
)

type quartzCalendarRule struct {
	kind   quartzCalendarKind
	day    int
	offset int
	week   int
}

var (
	lastDayPattern = regexp.MustCompile(`^L(?:-([0-9]{1,2}))?(W)?$`)
	weekdayPattern = regexp.MustCompile(`^([0-9]{1,2})W$`)
	lastDOWPattern = regexp.MustCompile(`^([1-7]|SUN|MON|TUE|WED|THU|FRI|SAT)L$`)
	nthDOWPattern  = regexp.MustCompile(`^([1-7]|SUN|MON|TUE|WED|THU|FRI|SAT)#([1-5])$`)
)

func parseQuartzCalendarRule(expression string) (quartzCalendarRule, bool, error) {
	fields := strings.Fields(strings.ToUpper(expression))
	if len(fields) != 6 {
		return quartzCalendarRule{}, false, nil
	}
	dom, dow := fields[3], fields[5]
	if matches := lastDayPattern.FindStringSubmatch(dom); matches != nil {
		if dow != "?" {
			return quartzCalendarRule{}, false, fmt.Errorf("cron calendar day-of-month rules require ? in day-of-week")
		}
		offset := 0
		if matches[1] != "" {
			offset, _ = strconv.Atoi(matches[1])
			if offset > 30 {
				return quartzCalendarRule{}, false, fmt.Errorf("cron L offset must be between 0 and 30")
			}
		}
		kind := quartzLastDay
		if matches[2] == "W" {
			kind = quartzLastWeekdayOfMonth
		}
		return quartzCalendarRule{kind: kind, offset: offset}, true, nil
	}
	if matches := weekdayPattern.FindStringSubmatch(dom); matches != nil {
		if dow != "?" {
			return quartzCalendarRule{}, false, fmt.Errorf("cron W requires ? in day-of-week")
		}
		day, _ := strconv.Atoi(matches[1])
		if day < 1 || day > 31 {
			return quartzCalendarRule{}, false, fmt.Errorf("cron W day must be between 1 and 31")
		}
		return quartzCalendarRule{kind: quartzNearestWeekday, day: day}, true, nil
	}
	if matches := lastDOWPattern.FindStringSubmatch(dow); matches != nil {
		if dom != "?" {
			return quartzCalendarRule{}, false, fmt.Errorf("cron L day-of-week rules require ? in day-of-month")
		}
		day, err := quartzDayOfWeek(matches[1])
		if err != nil {
			return quartzCalendarRule{}, false, err
		}
		return quartzCalendarRule{kind: quartzLastDayOfWeek, day: day}, true, nil
	}
	if matches := nthDOWPattern.FindStringSubmatch(dow); matches != nil {
		if dom != "?" {
			return quartzCalendarRule{}, false, fmt.Errorf("cron # requires ? in day-of-month")
		}
		day, err := quartzDayOfWeek(matches[1])
		if err != nil {
			return quartzCalendarRule{}, false, err
		}
		week, _ := strconv.Atoi(matches[2])
		return quartzCalendarRule{kind: quartzNthDayOfWeek, day: day, week: week}, true, nil
	}
	if strings.ContainsAny(dom, "LW#") || strings.ContainsAny(dow, "W#") || strings.Contains(dow, "L") && dow != "L" {
		return quartzCalendarRule{}, false, fmt.Errorf("invalid Quartz calendar day expression")
	}
	return quartzCalendarRule{}, false, nil
}

func quartzDayOfWeek(value string) (int, error) {
	names := map[string]int{"SUN": 1, "MON": 2, "TUE": 3, "WED": 4, "THU": 5, "FRI": 6, "SAT": 7}
	if day, ok := names[value]; ok {
		return day, nil
	}
	day, err := strconv.Atoi(value)
	if err != nil || day < 1 || day > 7 {
		return 0, fmt.Errorf("Quartz day-of-week must be between 1 and 7 or SUN-SAT")
	}
	return day, nil
}

func normalizeQuartzDayOfWeek(field string) (string, error) {
	field = strings.ToUpper(field)
	if field == "?" || field == "*" {
		return "*", nil
	}
	if field == "L" {
		return "6", nil
	}
	selected := make(map[int]struct{}, 7)
	for _, item := range strings.Split(field, ",") {
		parts := strings.Split(item, "/")
		if len(parts) > 2 || parts[0] == "" {
			return "", fmt.Errorf("invalid Quartz day-of-week %q", field)
		}
		step := 1
		if len(parts) == 2 {
			var err error
			step, err = strconv.Atoi(parts[1])
			if err != nil || step < 1 || step > 7 {
				return "", fmt.Errorf("Quartz day-of-week step must be between 1 and 7")
			}
		}
		start, end := 0, 0
		switch {
		case parts[0] == "*":
			start, end = 1, 7
		case strings.Contains(parts[0], "-"):
			rangeParts := strings.Split(parts[0], "-")
			if len(rangeParts) != 2 {
				return "", fmt.Errorf("invalid Quartz day-of-week range %q", parts[0])
			}
			var err error
			start, err = quartzDayOfWeek(rangeParts[0])
			if err != nil {
				return "", err
			}
			end, err = quartzDayOfWeek(rangeParts[1])
			if err != nil {
				return "", err
			}
		default:
			var err error
			start, err = quartzDayOfWeek(parts[0])
			if err != nil {
				return "", err
			}
			end = start
			if len(parts) == 2 {
				end = 7
			}
		}
		values := quartzDayRange(start, end)
		for index := 0; index < len(values); index += step {
			selected[values[index]-1] = struct{}{}
		}
	}
	values := make([]int, 0, len(selected))
	for value := range selected {
		values = append(values, value)
	}
	sort.Ints(values)
	parts := make([]string, len(values))
	for index, value := range values {
		parts[index] = strconv.Itoa(value)
	}
	return strings.Join(parts, ","), nil
}

func quartzDayRange(start, end int) []int {
	values := []int{start}
	for current := start; current != end; {
		current++
		if current == 8 {
			current = 1
		}
		values = append(values, current)
	}
	return values
}

func nextQuartzCalendar(expression string, rule quartzCalendarRule, location *time.Location, after time.Time) (time.Time, error) {
	fields := strings.Fields(strings.ToUpper(expression))
	localAfter := after.In(location)
	year, month, _ := localAfter.Date()
	searchAfter := localAfter
	for checked := 0; checked < 1200; checked++ {
		day := rule.dayInMonth(year, month, location)
		if day != 0 {
			candidateFields := append([]string(nil), fields...)
			candidateFields[3] = strconv.Itoa(day)
			candidateFields[5] = "*"
			schedule, err := parser.Parse(strings.Join(candidateFields, " "))
			if err != nil {
				return time.Time{}, fmt.Errorf("parse cron: %w", err)
			}
			candidate := schedule.Next(searchAfter)
			candidateYear, candidateMonth, candidateDay := candidate.In(location).Date()
			if candidateYear == year && candidateMonth == month && candidateDay == day {
				return candidate.UTC(), nil
			}
		}
		nextMonth := time.Date(year, month+1, 1, 12, 0, 0, 0, location)
		year, month, _ = nextMonth.Date()
		searchAfter = time.Date(year, month, 1, 0, 0, 0, 0, location).Add(-time.Nanosecond)
	}
	return time.Time{}, fmt.Errorf("cron has no matching calendar date within 100 years")
}

func (rule quartzCalendarRule) dayInMonth(year int, month time.Month, location *time.Location) int {
	last := time.Date(year, month+1, 0, 12, 0, 0, 0, location).Day()
	switch rule.kind {
	case quartzLastDay:
		return last - rule.offset
	case quartzNearestWeekday:
		if rule.day > last {
			return 0
		}
		return nearestWeekday(year, month, rule.day, last, location)
	case quartzLastWeekdayOfMonth:
		day := last - rule.offset
		if day < 1 {
			return 0
		}
		return nearestWeekday(year, month, day, last, location)
	case quartzLastDayOfWeek:
		wanted := time.Weekday(rule.day - 1)
		current := time.Date(year, month, last, 12, 0, 0, 0, location).Weekday()
		return last - (int(current-wanted)+7)%7
	case quartzNthDayOfWeek:
		wanted := time.Weekday(rule.day - 1)
		firstWeekday := time.Date(year, month, 1, 12, 0, 0, 0, location).Weekday()
		day := 1 + (int(wanted-firstWeekday)+7)%7 + (rule.week-1)*7
		if day > last {
			return 0
		}
		return day
	default:
		return 0
	}
}

func nearestWeekday(year int, month time.Month, day, last int, location *time.Location) int {
	switch time.Date(year, month, day, 12, 0, 0, 0, location).Weekday() {
	case time.Saturday:
		if day == 1 {
			return day + 2
		}
		return day - 1
	case time.Sunday:
		if day == last {
			return day - 2
		}
		return day + 1
	default:
		return day
	}
}

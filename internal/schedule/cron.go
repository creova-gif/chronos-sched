package schedule

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const maxCronSearchYears = 8

// Spec represents a parsed recurring schedule specification, either a standard
// 5-field UTC cron expression or a fixed-interval @every duration.
type Spec struct {
	expr string

	interval time.Duration // non-zero for @every specs

	minute  uint64 // bits 0..59
	hour    uint64 // bits 0..23
	dom     uint64 // bits 1..31
	month   uint64 // bits 1..12
	dow     uint64 // bits 0..6 (0 = Sunday)
	domWild bool
	dowWild bool
}

// String returns the original expression used to construct the Spec.
func (s *Spec) String() string {
	if s == nil {
		return ""
	}
	return s.expr
}

// ParseSpec parses a 5-field cron expression ("min hour dom mon dow") or a
// supported shorthand ("@hourly", "@daily", "@midnight", "@every <duration>").
func ParseSpec(expr string) (*Spec, error) {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return nil, errors.New("schedule expression must not be empty")
	}

	if strings.HasPrefix(trimmed, "@") {
		return parseShorthand(trimmed)
	}

	fields := strings.Fields(trimmed)
	if len(fields) != 5 {
		return nil, fmt.Errorf("cron expression %q must have exactly 5 fields, got %d", expr, len(fields))
	}

	minute, _, err := parseField(fields[0], 0, 59, false)
	if err != nil {
		return nil, fmt.Errorf("cron minute field %q: %w", fields[0], err)
	}
	hour, _, err := parseField(fields[1], 0, 23, false)
	if err != nil {
		return nil, fmt.Errorf("cron hour field %q: %w", fields[1], err)
	}
	dom, domWild, err := parseField(fields[2], 1, 31, false)
	if err != nil {
		return nil, fmt.Errorf("cron day-of-month field %q: %w", fields[2], err)
	}
	month, _, err := parseField(fields[3], 1, 12, false)
	if err != nil {
		return nil, fmt.Errorf("cron month field %q: %w", fields[3], err)
	}
	dow, dowWild, err := parseField(fields[4], 0, 6, true)
	if err != nil {
		return nil, fmt.Errorf("cron day-of-week field %q: %w", fields[4], err)
	}

	return &Spec{
		expr:    trimmed,
		minute:  minute,
		hour:    hour,
		dom:     dom,
		month:   month,
		dow:     dow,
		domWild: domWild,
		dowWild: dowWild,
	}, nil
}

func parseShorthand(expr string) (*Spec, error) {
	switch expr {
	case "@hourly":
		spec, err := ParseSpec("0 * * * *")
		if err != nil {
			return nil, err
		}
		spec.expr = expr
		return spec, nil
	case "@daily", "@midnight":
		spec, err := ParseSpec("0 0 * * *")
		if err != nil {
			return nil, err
		}
		spec.expr = expr
		return spec, nil
	}

	if strings.HasPrefix(expr, "@every") {
		rest := strings.TrimSpace(strings.TrimPrefix(expr, "@every"))
		if rest == "" {
			return nil, fmt.Errorf("invalid @every expression %q: missing duration", expr)
		}
		d, err := time.ParseDuration(rest)
		if err != nil {
			return nil, fmt.Errorf("invalid @every duration %q: %w", rest, err)
		}
		if d < time.Second {
			return nil, fmt.Errorf("invalid @every duration %q: minimum interval is 1s", rest)
		}
		return &Spec{
			expr:     expr,
			interval: d,
		}, nil
	}

	return nil, fmt.Errorf("unsupported schedule shorthand %q", expr)
}

func parseField(field string, minVal, maxVal int, isDOW bool) (uint64, bool, error) {
	if field == "*" {
		return fullMask(minVal, maxVal), true, nil
	}

	var mask uint64
	parts := strings.Split(field, ",")
	for _, part := range parts {
		if part == "" {
			return 0, false, errors.New("empty list item")
		}
		partMask, err := parseFieldPart(part, minVal, maxVal, isDOW)
		if err != nil {
			return 0, false, err
		}
		mask |= partMask
	}
	if mask == 0 {
		return 0, false, errors.New("field matches no values")
	}
	return mask, false, nil
}

func parseFieldPart(part string, minVal, maxVal int, isDOW bool) (uint64, error) {
	step := 1
	rangeExpr := part
	if strings.Contains(part, "/") {
		pieces := strings.Split(part, "/")
		if len(pieces) != 2 || pieces[0] == "" || pieces[1] == "" {
			return 0, fmt.Errorf("invalid step expression %q", part)
		}
		parsedStep, err := strconv.Atoi(pieces[1])
		if err != nil || parsedStep <= 0 {
			return 0, fmt.Errorf("invalid step value %q", pieces[1])
		}
		step = parsedStep
		rangeExpr = pieces[0]
	}

	parseMax := maxVal
	if isDOW {
		// Accept 7 as an alias for Sunday (0).
		parseMax = 7
	}

	var start, end int
	switch {
	case rangeExpr == "*":
		start, end = minVal, maxVal
	case strings.Contains(rangeExpr, "-"):
		bounds := strings.Split(rangeExpr, "-")
		if len(bounds) != 2 || bounds[0] == "" || bounds[1] == "" {
			return 0, fmt.Errorf("invalid range %q", rangeExpr)
		}
		var err error
		start, err = strconv.Atoi(bounds[0])
		if err != nil {
			return 0, fmt.Errorf("invalid range start %q", bounds[0])
		}
		end, err = strconv.Atoi(bounds[1])
		if err != nil {
			return 0, fmt.Errorf("invalid range end %q", bounds[1])
		}
		if start < minVal || start > parseMax || end < minVal || end > parseMax {
			return 0, fmt.Errorf("range %d-%d out of bounds [%d,%d]", start, end, minVal, maxVal)
		}
		if start > end {
			return 0, fmt.Errorf("range start %d exceeds end %d", start, end)
		}
	default:
		if strings.Contains(part, "/") {
			return 0, fmt.Errorf("step requires wildcard or range in %q", part)
		}
		val, err := strconv.Atoi(rangeExpr)
		if err != nil {
			return 0, fmt.Errorf("invalid value %q", rangeExpr)
		}
		if val < minVal || val > parseMax {
			return 0, fmt.Errorf("value %d out of bounds [%d,%d]", val, minVal, maxVal)
		}
		start, end = val, val
	}

	var mask uint64
	for v := start; v <= end; v += step {
		bit := v
		if isDOW && bit == 7 {
			bit = 0
		}
		mask |= 1 << uint(bit)
	}
	return mask, nil
}

func fullMask(minVal, maxVal int) uint64 {
	var mask uint64
	for v := minVal; v <= maxVal; v++ {
		mask |= 1 << uint(v)
	}
	return mask
}

// Next returns the next firing time strictly after the given time, always in UTC.
// Returns the zero time if no firing time exists within the search horizon.
func (s *Spec) Next(after time.Time) time.Time {
	afterUTC := after.UTC()
	if s.interval > 0 {
		return afterUTC.Add(s.interval)
	}

	// Standard cron granularity is 1 minute; start at the next whole minute strictly after afterUTC.
	t := afterUTC.Truncate(time.Minute).Add(time.Minute)
	yearLimit := afterUTC.Year() + maxCronSearchYears

	for t.Year() <= yearLimit {
		if s.month&(1<<uint(t.Month())) == 0 {
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, time.UTC)
			continue
		}
		if !s.dayMatches(t) {
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, time.UTC)
			continue
		}
		if s.hour&(1<<uint(t.Hour())) == 0 {
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, time.UTC)
			continue
		}
		if s.minute&(1<<uint(t.Minute())) == 0 {
			t = t.Add(time.Minute)
			continue
		}
		return t
	}

	return time.Time{}
}

func (s *Spec) dayMatches(t time.Time) bool {
	domMatch := s.dom&(1<<uint(t.Day())) != 0
	dowMatch := s.dow&(1<<uint(t.Weekday())) != 0
	if !s.domWild && !s.dowWild {
		return domMatch || dowMatch
	}
	return domMatch && dowMatch
}

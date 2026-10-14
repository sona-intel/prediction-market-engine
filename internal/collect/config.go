package collect

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"pm/internal/venue"
)

const (
	defaultVenues   = venue.Hyperliquid + "," + venue.Kalshi + "," + venue.PolymarketUS + "," + venue.Levr
	defaultKeepDays = 60
)

var ErrConfig = errors.New("invalid config")

type Config struct {
	PGURL    string
	Venues   []string
	KeepDays int
}

// ConfigFrom reads the settings from an environment lookup
func ConfigFrom(get func(string) string) (Config, error) {
	c := Config{PGURL: get("PM_PG_URL"), KeepDays: defaultKeepDays}
	if c.PGURL == "" {
		return c, fmt.Errorf("%w: PM_PG_URL is not set", ErrConfig)
	}
	for v := range strings.SplitSeq(cmp.Or(get("PM_COLLECT_VENUES"), defaultVenues), ",") {
		v = strings.TrimSpace(v)
		if _, ok := venue.Catalog[v]; !ok {
			return c, fmt.Errorf("%w: PM_COLLECT_VENUES names unknown venue %q", ErrConfig, v)
		}
		if !slices.Contains(c.Venues, v) {
			c.Venues = append(c.Venues, v)
		}
	}
	if s := get("PM_SNAP_KEEP_DAYS"); s != "" {
		d, err := strconv.Atoi(s)
		if err != nil || d < 1 {
			return c, fmt.Errorf("%w: PM_SNAP_KEEP_DAYS must be a positive number of days", ErrConfig)
		}
		c.KeepDays = d
	}
	return c, nil
}

// Duration reads a duration setting and falls back to def when it is unset
func Duration(get func(string) string, key string, def time.Duration) (time.Duration, error) {
	s := get(key)
	if s == "" {
		return def, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%w: %s must be a positive duration", ErrConfig, key)
	}
	return d, nil
}

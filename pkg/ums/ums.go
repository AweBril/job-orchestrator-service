package ums

import (
	"fmt"
	"time"
)

// UnixMillis provides a convenient way to handle time within the MAF ecosystem. UnixMillis are the easiest form of
// a date that can be passed, so this allows for easy transformation of that format to usable Time or Duration forms.
type UnixMillis int64

// FromTime converts a time to UnixMillis
func FromTime(t time.Time) UnixMillis {
	return UnixMillis(t.UnixMilli())
}

// Now returns the current time as a UnixMillis
func Now() UnixMillis {
	return FromTime(time.Now())
}

// FromDuration converts a duration to a UnixMillis
func FromDuration(d time.Duration) UnixMillis {
	return UnixMillis(d / time.Millisecond)
}

// Time converts the unix milliseconds into a Time object.
func (ums UnixMillis) Time() time.Time {
	return time.Unix(0, ums.Int64()*int64(time.Millisecond))
}

// Duration converts the unix milliseconds into a Duration type.
func (ums UnixMillis) Duration() time.Duration {
	return time.Duration(ums) * time.Millisecond
}

// Int64 is a convenience function for type casting unix millis to int64.
func (ums UnixMillis) Int64() int64 {
	return int64(ums)
}

// Add operates the same way time.Time.Add does, for convenience.
func (ums UnixMillis) Add(d time.Duration) UnixMillis {
	return ums + FromDuration(d)
}

// Sub operates the same way time.Time.Sub does, for convenience.
func (ums UnixMillis) Sub(t time.Time) UnixMillis {
	return ums - FromTime(t)
}

// String gets the string form of the base unix milliseconds- not a formatted time.
func (ums UnixMillis) String() string {
	return fmt.Sprintf("%d", ums)
}

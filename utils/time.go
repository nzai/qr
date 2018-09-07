package utils

import "time"

// TodayZero truncate time to today zero clock
func TodayZero(now time.Time) time.Time {
	_, offset := now.Zone()
	return now.Truncate(time.Hour * 24).Add(-time.Second * time.Duration(offset))
}

// TomorrowZero round time to tomorrow zero clock
func TomorrowZero(now time.Time) time.Time {
	return TodayZero(now).AddDate(0, 0, 1)
}

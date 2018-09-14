package utils

import "time"

// TodayZero truncate time to today zero clock
func TodayZero(now time.Time) time.Time {
	_, offset := now.Zone()
	duration := time.Second * time.Duration(offset)
	return now.Add(duration).Truncate(time.Hour * 24).Add(-duration)
}

// TomorrowZero round time to tomorrow zero clock
func TomorrowZero(now time.Time) time.Time {
	return TodayZero(now).AddDate(0, 0, 1)
}

// YesterdayZero truncate time to yesterday zero clock
func YesterdayZero(now time.Time) time.Time {
	return TodayZero(now).AddDate(0, 0, -1)
}

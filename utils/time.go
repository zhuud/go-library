package utils

import (
    "fmt"
    "math"
    "time"
)

// ParseTimeFromString 解析时间
func ParseTimeFromString(tm string) time.Time {
    s := []string{
        "2006-01-02 15:04:05",
        "2006/01/02 15:04:05",
        "2006-01-02",
        "2006/01/02",
        "15:04:05",
        // ...
    }
    for _, v := range s {
        t, err := time.ParseInLocation(v, tm, time.Local)
        if nil == err && !t.IsZero() {
            return t
        }
    }
    return time.Time{}
}

// FormatRelativeTime 转换为相对时间
func FormatRelativeTime(showDate time.Time) string {
    currentTime := time.Now().Local().Unix()
    showTime := showDate.Unix()
    duration := currentTime - showTime

    if duration < 60 {
        if duration == 0 {
            duration = 1
        }
        return fmt.Sprintf("%d秒前", duration)
    }
    if duration < 3600 {
        return fmt.Sprintf("%d分钟前", int(math.Floor(float64(duration/60))))
    }
    if duration < 86400 {
        return fmt.Sprintf("%d小时前", int(math.Floor(float64(duration/3600))))
    }
    if duration < 2592000 {
        return fmt.Sprintf("%d天前", int(math.Floor(float64(duration/86400))))
    }
    if duration < 31104000 {
        return fmt.Sprintf("%d个月前", int(math.Floor(float64(duration/2592000))))
    }

    return showDate.Format("2006-01-02 15:04:05")
}

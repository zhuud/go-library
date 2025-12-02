package utils

import (
	"fmt"
	"time"
)

const (
	// 常用格式变体
	TimeFormatDateTimeSlash   = "2006/01/02 15:04:05"  // 日期时间（斜杠分隔）
	TimeFormatDateSlash       = "2006/01/02"           // 日期（斜杠分隔）
	TimeFormatDateCompact     = "20060102"             // 日期（紧凑格式）
	TimeFormatDateTimeCompact = "20060102150405"       // 日期时间（紧凑格式）
	TimeFormatRFC3339NoTZ     = "2006-01-02T15:04:05"  // RFC3339 不带时区
	TimeFormatRFC3339UTC      = "2006-01-02T15:04:05Z" // RFC3339 UTC 时间
	TimeFormatUSDate          = "01/02/2006"           // 美式日期格式 (MM/DD/YYYY)
	TimeFormatEuropeanDate    = "02/01/2006"           // 欧式日期格式 (DD/MM/YYYY)
	TimeFormatDateDot         = "02.01.2006"           // 日期（点分隔）
	TimeFormatDateDash        = "02-01-2006"           // 日期（短横线分隔）
	TimeFormatDateTimeDot     = "02.01.2006 15:04:05"  // 日期时间（点分隔）
	TimeFormatDateTimeDash    = "02-01-2006 15:04:05"  // 日期时间（短横线分隔）
	TimeFormatDateTimeMin     = "2006-01-02 15:04"     // 日期时间（到分钟）
	TimeFormatTime12hSpace    = "03:04:05 PM"          // 12小时制（带空格）
)

// Use the long enough past time as start time, in case timex.Now() - lastTime equals 0.
var initTime = time.Now().AddDate(-1, -1, -1)

// Now returns a relative time duration since initTime, which is not important.
// The caller only needs to care about the relative value.
func Now() time.Duration {
	return time.Since(initTime)
}

// Since returns a diff since given d.
func Since(d time.Duration) time.Duration {
	return time.Since(initTime) - d
}

// ParseTimeFromString 解析时间，支持多种时间格式
// 格式按优先级排序：最常用、最精确的格式优先
func ParseTimeFromString(tm string) time.Time {
	formats := []string{
		// 标准格式（最常用，优先匹配）
		time.RFC3339Nano, // 2006-01-02T15:04:05.999999999Z07:00（最精确）
		time.RFC3339,     // 2006-01-02T15:04:05Z07:00（ISO 8601 标准）
		time.DateTime,    // 2006-01-02 15:04:05（Go 1.20+）

		// RFC3339 变体
		TimeFormatRFC3339NoTZ, // 2006-01-02T15:04:05（不带时区）
		TimeFormatRFC3339UTC,  // 2006-01-02T15:04:05Z（UTC 时间）

		// 自定义格式
		TimeFormatDateTimeSlash,   // 2006/01/02 15:04:05
		TimeFormatDateTimeCompact, // 20060102150405
		TimeFormatDateTimeMin,     // 2006-01-02 15:04（到分钟）
		TimeFormatDateSlash,       // 2006/01/02
		TimeFormatDateCompact,     // 20060102

		// 地区格式变体
		TimeFormatUSDate,       // 01/02/2006（美式：MM/DD/YYYY）
		TimeFormatEuropeanDate, // 02/01/2006（欧式：DD/MM/YYYY）
		TimeFormatDateDot,      // 02.01.2006（点分隔）
		TimeFormatDateDash,     // 02-01-2006（短横线分隔）
		TimeFormatDateTimeDot,  // 02.01.2006 15:04:05
		TimeFormatDateTimeDash, // 02-01-2006 15:04:05

		// RFC 标准格式
		time.RFC1123Z, // Mon, 02 Jan 2006 15:04:05 -0700
		time.RFC1123,  // Mon, 02 Jan 2006 15:04:05 MST
		time.RFC822Z,  // 02 Jan 06 15:04 -0700
		time.RFC850,   // Monday, 02-Jan-06 15:04:05 MST
		time.RFC822,   // 02 Jan 06 15:04 MST（注意：不支持时区偏移，优先使用 RFC822Z）

		// Unix 相关格式
		time.UnixDate, // Mon Jan _2 15:04:05 MST 2006
		time.ANSIC,    // Mon Jan _2 15:04:05 2006
		time.RubyDate, // Mon Jan 02 15:04:05 -0700 2006

		// 部分信息格式（只包含日期或时间，解析后会有默认值填充）
		time.DateOnly, // 2006-01-02（只包含日期）
		time.TimeOnly, // 15:04:05（只包含时间）

		// 时间戳格式（只包含时间，不包含日期）
		time.StampNano,         // Jan _2 15:04:05.000000000
		time.StampMicro,        // Jan _2 15:04:05.000000
		time.StampMilli,        // Jan _2 15:04:05.000
		time.Stamp,             // Jan _2 15:04:05
		time.Kitchen,           // 3:04PM（12小时制）
		TimeFormatTime12hSpace, // 03:04:05 PM（12小时制，带空格）

		// Go 参考时间格式（很少在实际数据中使用）
		time.Layout, // 01/02 03:04:05PM '06 -0700
	}

	for _, format := range formats {
		t, err := time.ParseInLocation(format, tm, time.Local)
		if err == nil && !t.IsZero() {
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
		return fmt.Sprintf("%d分钟前", duration/60)
	}
	if duration < 86400 {
		return fmt.Sprintf("%d小时前", duration/3600)
	}
	if duration < 2592000 {
		return fmt.Sprintf("%d天前", duration/86400)
	}
	if duration < 31104000 {
		return fmt.Sprintf("%d个月前", duration/2592000)
	}

	return showDate.Format(time.DateTime)
}

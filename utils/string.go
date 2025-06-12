package utils

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

// 	AddSlashes(str string) string ：escapes special characters in the given string.
//	StripSlashes(str string) string ：removes escaped characters from the given string.
//	Substr(str string, start int, length int) string ：returns a substring of the given string starting at the specified position and with the specified length.
//	GenRandomStr(n int) string ：generates a random alphanumeric string of the specified length.
//	GenRandomNumStr(n int) string ：generates a random numeric string of the specified length.
//	UniqId(prefix string) string：generates a unique ID prefixed with a specified string.
// 	CastSliceStrToSliceInt(strs []string) []int ：converts a slice of strings to a slice of integers.

// AddSlashes 转义
func AddSlashes(str string) string {
	var tmpRune []rune
	strRune := []rune(str)
	for _, ch := range strRune {
		switch ch {
		case []rune{'\\'}[0], []rune{'"'}[0], []rune{'\''}[0]:
			tmpRune = append(tmpRune, []rune{'\\'}[0])
			tmpRune = append(tmpRune, ch)
		default:
			tmpRune = append(tmpRune, ch)
		}
	}
	return string(tmpRune)
}

// StripSlashes 去除转义
func StripSlashes(str string) string {
	var dstRune []rune
	strRune := []rune(str)
	strLength := len(strRune)
	for i := 0; i < strLength; i++ {
		if strRune[i] == []rune{'\\'}[0] {
			i++
		}
		if i < strLength {
			dstRune = append(dstRune, strRune[i])
		}
	}
	return string(dstRune)
}

// Substr 截取字符
func Substr(str string, start int, length int) string {

	runes := []rune(str)
	strLen := utf8.RuneCountInString(str)

	if start < 0 {
		start = strLen + start
	}

	if start >= strLen {
		return ""
	}

	if length <= 0 {
		return ""
	}

	if length > strLen-start {
		length = strLen - start
	}

	end := start + length
	return string(runes[start:end])
}

// GenRandomStr 生成随机字符串
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

func GenRandomStr(n int) string {
	src := rand.NewSource(time.Now().UnixNano())
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

// GenRandomNumStr 生成随机数字字符串
func GenRandomNumStr(n int) string {
	sn := `1` + strings.Repeat(`0`, n)
	in, _ := strconv.Atoi(sn)
	src := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(in)
	return fmt.Sprintf("%0"+strconv.Itoa(n)+"v", src)
}

// GenUniqId 生成唯一字符串
func GenUniqId() string {
	return uuid.NewString()
}

// CastSliceStrToSliceInt 将字符串切片转换为整数切片
func CastSliceStrToSliceInt(strs []string) []int {
	ret := make([]int, 0, len(strs))
	for _, str := range strs {
		if str == "" {
			ret = append(ret, 0)
			continue
		}
		n := 0
		// 只保留数字前缀，忽略非数字部分
		for i := 0; i < len(str); i++ {
			if str[i] < '0' || str[i] > '9' {
				break
			}
			// 字符数字的 ASCII 减去 '0' 的 ASCII，得到数字
			n = n*10 + int(str[i]-'0')
		}
		ret = append(ret, n)
	}
	return ret
}

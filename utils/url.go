package utils

import (
    "fmt"
    "strings"
)

//	MapToUrlQueryString(data map[string]string) string ：converts a map to a URL query string.

// MapToUrlQueryString 将map格式数据拼接成url Get方式参数【注：拼接后的参数顺序是随机的】
func MapToUrlQueryString[V comparable](data map[string]V) string {
    var res string
    for k, v := range data {
        tmp := fmt.Sprintf("%s=%v&", k, v)
        res = res + tmp
    }
    return strings.Trim(res, "&")
}

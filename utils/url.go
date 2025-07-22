package utils

import (
	"fmt"
	"sort"
	"strings"
)

//	MapToUrlQueryString(data map[string]string) string ：converts a map to a URL query string.

// MapToUrlQueryString 将map格式数据拼接成url Get方式参数，参数顺序为 key 字典序
func MapToUrlQueryString[V comparable](data map[string]V) string {
	if len(data) == 0 {
		return ""
	}
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var res strings.Builder
	for _, k := range keys {
		res.WriteString(fmt.Sprintf("%s=%v&", k, data[k]))
	}
	out := res.String()
	return strings.TrimSuffix(out, "&")
}

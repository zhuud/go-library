package utils

import (
    "bytes"
    "regexp"
    "strings"

    "golang.org/x/net/html"
)

// StripTags 去除html标签
func StripTags(data string, allowedTags []string) string {

    data = html.UnescapeString(data)

    // 将允许的标签列表转换为一个查找表。
    allowed := make(map[string]struct{})
    for _, tag := range allowedTags {
        allowed[tag] = struct{}{}
    }

    var buf bytes.Buffer
    tokenizer := html.NewTokenizer(strings.NewReader(data))

    for {
        tokenType := tokenizer.Next()

        switch tokenType {
        case html.ErrorToken:
            // 读取结束。
            end := buf.String()
            return html.UnescapeString(end)
        case html.TextToken:
            // 当前标记是文本，则直接写入输出缓冲区。
            buf.Write(tokenizer.Text())
        case html.StartTagToken, html.EndTagToken, html.SelfClosingTagToken:
            // 如果当前标记是开始标记、结束标记或自闭合标记，则检查是否是允许的标签。
            token := tokenizer.Token()
            if _, ok := allowed[token.Data]; ok {
                // 允许的标记，则重构HTML标记并写入输出。
                buf.WriteString(token.String())
            }
        default:
        }
    }
}

// ExtractUrlsFromHtml 富文本获取url
var urlPattern = regexp.MustCompile(`(?:(?:http|https)(?:\:\/\/))?(?:(?:[\d|a-z]+)(?:\.))+(?:cn|com|org|net|tv|live|xyz|pub|xin|top|tech|ink)`)

func ExtractUrlsFromHtml(content string) []string {
    urlList := make([]string, 0)
    matches := urlPattern.FindAllStringSubmatch(content, -1)

    for _, match := range matches {
        if len(match) > 0 {
            urlList = append(urlList, match[0])
        }
    }

    return urlList
}

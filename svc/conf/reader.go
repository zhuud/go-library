package conf

import "github.com/zhuud/go-library/svc/conf/internal"

// Reader 读取器接口，外部可以实现此接口来自定义读取器
// 这是 internal.Reader 的别名，保持 API 兼容性
type Reader = internal.Reader

package alarm

import "github.com/zhuud/go-library/svc/alarm/internal"

// Sender 发送器接口，外部可以实现此接口来自定义发送器
// 这是 internal.Sender 的别名，保持 API 兼容性
type Sender = internal.Sender

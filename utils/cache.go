package utils

import (
    "fmt"
    "math/rand"
    "time"
)

// GetCacheKey 格式化key
func GetCacheKey(key string, args ...any) string {
    return fmt.Sprintf(key, args...)
}

// GetCacheExpWithRand 返回 t的时间 +  [0- t的时间一半的时间)
func GetCacheExpWithRand(t time.Duration) time.Duration {

    r := rand.New(rand.NewSource(time.Now().UnixNano()))

    randomOffset := time.Duration(r.Int63n(int64(t / 2)))

    return t + randomOffset
}

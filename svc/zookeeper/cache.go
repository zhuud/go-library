package zookeeper

import (
	"fmt"
	"sync"

	"github.com/cornelk/hashmap"
)

var (
	zkCache         sync.Map
	zkChildKeyCache sync.Map
	zkChildCache    = hashmap.New[string, any]()
)

func (z *Client) GetC(key string) (string, error) {

	if cacheData, ok := zkCache.Load(key); ok {
		return cacheData.(string), nil
	}

	data, _, events, err := z.GetW(key)
	if err != nil {
		return "", fmt.Errorf("zookeeper.GetC.GetW key %s  error %w ", key, err)
	}

	zkCache.Store(key, string(data))
	go func() {
		for {
			<-events
			data, _, events, err = z.GetW(key)
			// 重试
			if err != nil {
				data, _, events, err = z.GetW(key)
			}
			// 重试后还失败 删除此key 结束watch 等待下次重新获取
			if err != nil {
				zkCache.Delete(key)
				return
			}
			zkCache.Store(key, string(data))
		}
	}()

	return string(data), nil
}

func (z *Client) GetChildC(key string) (map[string]string, error) {
	cacheData, ok := zkChildCache.Get(key)
	if ok {
		return cacheData.(map[string]string), nil
	}

	data := make(map[string]string)
	keys, _, events, err := z.ChildrenW(key)
	if err != nil {
		return data, fmt.Errorf("zookeeper.GetChildC.ChildrenW key %s  error %w ", key, err)
	}

	for _, childKey := range keys {
		childData, _, err := z.Get(key + `/` + childKey)
		if err != nil {
			return data, fmt.Errorf("zookeeper.GetChildC.Get key %s/%s  error %w ", key, childKey, err)
		}
		data[childKey] = string(childData)
	}
	zkChildCache.Set(key, data)
	go func() {
		for {
			<-events
			keys, _, events, err = z.ChildrenW(key)
			data = make(map[string]string, len(keys))
			if err == nil {
				for _, childKey := range keys {
					childData, _, err := z.Get(key + `/` + childKey)
					// 重试
					if err != nil {
						childData, _, err = z.Get(key + `/` + childKey)
					}
					// 重试后还失败 删除此key 结束watch 等待下次重新获取
					if err != nil {
						zkChildCache.Del(key)
						return
					}
					data[childKey] = string(childData)
				}
				zkChildCache.Set(key, data)
			}
		}
	}()

	return data, err
}

func (z *Client) GetChildKeyC(key string) ([]string, error) {
	if cacheKeys, ok := zkChildKeyCache.Load(key); ok {
		return cacheKeys.([]string), nil
	}

	keys := make([]string, 0)

	//通过watch的方式获取并观察节点的变化
	data, _, events, err := z.ChildrenW(key)
	if err != nil {
		return keys, fmt.Errorf("zookeeper.GetChildKeyC.ChildrenW key %s  error %w ", key, err)
	}

	zkChildKeyCache.Store(key, data)
	go func() {
		for {
			<-events
			data, _, events, err = z.ChildrenW(key)

			// 重试
			if err != nil {
				data, _, events, err = z.ChildrenW(key)
			}
			// 重试后还失败 删除此key 结束watch 等待下次重新获取
			if err != nil {
				zkChildKeyCache.Delete(key)
				return
			}
			zkChildKeyCache.Store(key, data)
		}
	}()

	return data, nil
}

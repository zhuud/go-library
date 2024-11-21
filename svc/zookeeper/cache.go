package zookeeper

import (
	"sync"

	"github.com/cornelk/hashmap"
	"github.com/pkg/errors"
)

var (
	zkCache         sync.Map
	zkChildKeyCache sync.Map
	zkChildCache    = hashmap.New[string, any]()
)

func (z *Client) GetC(k string) (string, error) {

	if cacheData, ok := zkCache.Load(k); ok {
		return cacheData.(string), nil
	}

	data, _, events, err := z.GetW(k)
	if err != nil {
		return "", errors.Wrap(err, "GetC.GetW error, key:"+k)
	}

	zkCache.Store(k, string(data))
	go func() {
		for {
			<-events
			data, _, events, err = z.GetW(k)
			// 重试
			if err != nil {
				data, _, events, err = z.GetW(k)
			}
			// 重试后还失败 删除此key 结束watch 等待下次重新获取
			if err != nil {
				zkCache.Delete(k)
				return
			}
			zkCache.Store(k, string(data))
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
		return data, errors.Wrap(err, "GetChildC.ChildrenW error, key:"+key)
	}

	for _, childKey := range keys {
		childData, _, err := z.Get(key + `/` + childKey)
		if err != nil {
			return data, errors.Wrap(err, "GetChildC.Get error, key:"+key+`/`+childKey)
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
		return keys, errors.Wrap(err, "GetChildKeyC.ChildrenW error, key:"+key)
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

package utils

import "reflect"

// ArrayIn[V comparable](needle V, haystack []V) bool ：Checks if a value exists within a slice.
// ArraySearch[V comparable](needle V, haystack []V) int ：Searches for a value in a slice and returns its index.
// ArrayDiff[V comparable](array1 []V, arrayOthers ...[]V) []V ：Returns a new slice containing elements that are in the first slice but not in any of the others.
// ArrayIntersect[V comparable](array1 []V, arrayOthers ...[]V) []V ：Returns a new slice containing elements that are common to all the input slices.
// ArrayColumn[M ~[]map[K]V, K comparable, V any](m M, key K) []V ：Retrieves values from a slice of maps by a specified key.
// ArrayUnique[V comparable](idList []V) []V ：Returns a slice with unique elements from the original slice.
// ArrayKey[M ~map[K]V, K comparable, V any](m M) []K ：Returns the keys from a map as a slice.
// ArrayValue[M ~map[K]V, K comparable, V any](m M, sort ...K) []V ：Returns the values from a map as a slice. Optionally, it can return the values sorted by the provided keys.
// ArrayUnset[V comparable](s []V, elems ...V) []V ：Removes specified elements from a slice.

func ArrayIn[V comparable](needle V, haystack []V) bool {
    if len(haystack) <= 8 {
        for _, v := range haystack {
            if needle == v {
                return true
            }
        }
        return false
    }

    cache := make(map[V]bool, len(haystack))
    for _, v := range haystack {
        if _, ok := cache[v]; !ok {
            cache[v] = true
        }
    }
    return cache[needle]
}

func ArraySearch[V comparable](needle V, haystack []V) int {
    for i, v := range haystack {
        if v == needle {
            return i
        }
    }
    return -1
}

func ArrayDiff[V comparable](array1 []V, arrayOthers ...[]V) []V {

    m := make(map[V]bool)
    r := make([]V, 0)

    for _, val := range array1 {
        m[val] = true
    }

    for _, array := range arrayOthers {
        for _, val := range array {
            if _, hasKey := m[val]; hasKey {
                m[val] = false
            }
        }
    }

    for k, v := range m {
        if v {
            r = append(r, k)
        }
    }
    return r
}

func ArrayIntersect[V comparable](array1 []V, arrayOthers ...[]V) []V {
    m := make(map[V]bool)
    r := make([]V, 0)

    for _, val := range array1 {
        m[val] = false
    }

    for _, array := range arrayOthers {
        tm := make(map[V]bool)
        for _, val := range array {
            if _, hasKey := m[val]; hasKey {
                tm[val] = true
            }
        }
        m = tm
    }

    for k, v := range m {
        if v {
            r = append(r, k)
        }
    }
    return r
}

func ArrayColumn[M ~[]map[K]V, K comparable, V any](m M, key K) []V {
    r := make([]V, 0, len(m))
    for _, val := range m {
        if v, ok := val[key]; ok {
            r = append(r, v)
        }
    }
    return r
}

func ArrayUnique[V comparable](idList []V) []V {
    uiqIdList := make([]V, 0, len(idList))
    tmp := make(map[V]bool)
    for _, id := range idList {
        rf := reflect.ValueOf(id)
        if _, ok := tmp[id]; !ok && !rf.IsZero() {
            tmp[id] = true
            uiqIdList = append(uiqIdList, id)
        }
    }
    return uiqIdList
}

func ArrayKey[M ~map[K]V, K comparable, V any](m M) []K {
    r := make([]K, 0, len(m))
    for k := range m {
        r = append(r, k)
    }
    return r
}

func ArrayValue[M ~map[K]V, K comparable, V any](m M, sort ...K) []V {
    r := make([]V, 0, len(m))
    if len(sort) != 0 {
        for _, id := range sort {
            if v, ok := m[id]; ok {
                r = append(r, v)
            }
        }
    } else {
        for _, v := range m {
            r = append(r, v)
        }
    }
    return r
}

func ArrayUnset[V comparable](s []V, elems ...V) []V {
    if len(s) == 0 {
        return s
    }
    r := make([]V, 0, len(s))

    for _, v := range s {
        flag := true
        for _, elem := range elems {
            if v == elem {
                flag = false
            }
        }
        if flag {
            r = append(r, v)
        }
    }

    return r
}

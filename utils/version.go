package utils

import (
	"strings"
)

// CompareVersions returns true if the first field and the third field are equal, otherwise false.
func CompareVersions(v1, op, v2 string) bool {
	result := compare(v1, v2)
	switch op {
	case "=", "==":
		return result == 0
	case "<":
		return result == -1
	case ">":
		return result == 1
	case "<=":
		return result == -1 || result == 0
	case ">=":
		return result == 0 || result == 1
	}

	return false
}

// return -1 if v1<v2, 0 if they are equal, and 1 if v1>v2
func compare(v1, v2 string) int {
	v1 = sanitizeVersionString(v1)
	v2 = sanitizeVersionString(v2)

	fields1, fields2 := strings.Split(v1, "."), strings.Split(v2, ".")
	ver1, ver2 := CastSliceStrToSliceInt(fields1), CastSliceStrToSliceInt(fields2)
	ver1len, ver2len := len(ver1), len(ver2)
	shorter := ver1len
	if ver2len < ver1len {
		shorter = ver2len
	}

	for i := 0; i < shorter; i++ {
		if ver1[i] == ver2[i] {
			continue
		} else if ver1[i] < ver2[i] {
			return -1
		} else {
			return 1
		}
	}

	if ver1len < ver2len {
		return -1
	} else if ver1len == ver2len {
		return 0
	} else {
		return 1
	}
}

func sanitizeVersionString(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch r {
		case 'V', 'v':
			continue
		case '-':
			b.WriteRune('.')
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

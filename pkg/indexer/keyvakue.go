package indexer

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

func combineKey(prefix string, suffix interface{}) string {
	if prefix != "" {
		return fmt.Sprintf("%s.%v", prefix, suffix)
	}

	return fmt.Sprintf("%v", suffix)
}

type keyValuePair struct {
	Key   string
	Value interface{}
}

func toKeyStringPairs(v interface{}, key string) []keyValuePair {
	raw, err := json.MarshalIndent(v, "  ", "")
	if err != nil {
		return []keyValuePair{}
	}
	return []keyValuePair{{key, string(raw)}}
}

func parseJSONTag(tag string) (name string, omitempty bool) {
	if tag == "" {
		return
	}

	arr := strings.Split(tag, ",")
	name = arr[0]
	omitempty = false

	for i := range arr[1:] {
		if arr[i] == "omitempty" {
			omitempty = true
		}
	}

	return
}

func toKeyValuePairs(v interface{}, keyPrefix string, omitempty bool) []keyValuePair {
	value := reflect.ValueOf(v)

	switch value.Kind() {
	case reflect.Bool:
		b, _ := v.(bool)
		// Convert to number.
		var d int
		if b {
			d = 1
		} else {
			d = 0
		}
		return []keyValuePair{{keyPrefix, d}}

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr, reflect.Complex64, reflect.Complex128, reflect.Float32, reflect.Float64:
		if s, _ := v.(int); omitempty && s == 0 {
			return []keyValuePair{} // returns empty list.
		}

		return []keyValuePair{{keyPrefix, v}}

	case reflect.String:
		if s, _ := v.(string); omitempty && s == "" {
			return []keyValuePair{} // returns empty list.
		}

		return []keyValuePair{{keyPrefix, v}}

	case reflect.Map:
		var kvList []keyValuePair

		keys := value.MapKeys()
		for i := 0; i < value.Len(); i++ {
			mValue := value.MapIndex(keys[i])
			key := combineKey(keyPrefix, keys[i].String())
			kvList = append(kvList, toKeyValuePairs(mValue.Interface(), key, false)...)
		}
		return kvList

	case reflect.Struct:
		t := value.Type()
		var pList []keyValuePair

		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			fieldName := f.Name

			vdata := value.FieldByName(f.Name)
			if !vdata.CanInterface() {
				continue
			}

			name, omit := parseJSONTag(f.Tag.Get("json"))
			if name != "" {
				fieldName = name
			}
			newKeyPrefix := combineKey(keyPrefix, fieldName)

			pList = append(pList, toKeyValuePairs(vdata.Interface(), newKeyPrefix, omit)...)
		}

		return pList

	case reflect.Array, reflect.Slice:
		var pList []keyValuePair

		for i := 0; i < value.Len(); i++ {
			newKeyPrefix := combineKey(keyPrefix, i)

			vdata := value.Index(i)
			pList = append(pList, toKeyValuePairs(vdata.Interface(), newKeyPrefix, false)...)
		}

		return pList

	case reflect.Ptr, reflect.UnsafePointer:
		if value.IsZero() || value.Elem().IsZero() {
			return []keyValuePair{}
		}

		return toKeyValuePairs(value.Elem().Interface(), keyPrefix, false)

	default: // will be ignored
		// Expected:
		// reflect.Chan, reflect.Interface, reflect.Ptr, reflect.Func,
		// reflect.UnsafePointer, reflect.Invalid
		return []keyValuePair{} // returns empty list.
	}
}

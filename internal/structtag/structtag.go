package structtag

import (
	"reflect"
	"sync"
)

// StructField represents a field in a struct that has a specific tag.
type StructField struct {
	Index     int
	Tag       string
	FieldName string
	value     string
}

// Value returns the value of a key in the tag, if present.
// For example, if the tag is `datastore:"name,noindex"`, calling Value("datastore")
// returns "name,noindex".
//
// Usage in orm.go:
// 1. structtag.GetFieldsByTag(e, "model")
//    - returns fields where `model:"..."` is present.
//    - field.Tag returns the value of the "model" tag.
//    - e.g. `model:"id"` -> tag="id", `model:"secret,encrypt"` -> tag="secret,encrypt"
//
// 2. field.Value("datastore") is used to check the datastore tag on the same field
//    for auto-exclude property name resolution.

// cache stores the parsed fields for a given type and key.
var cache sync.Map

type cacheKey struct {
	typ reflect.Type
	key string
}

// GetFieldsByTag returns a list of fields that have the specified tag key.
func GetFieldsByTag(entity any, key string) []*StructField {
	t := reflect.TypeOf(entity)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// We only handle simple structs for now
	if t.Kind() != reflect.Struct {
		return nil
	}

	ck := cacheKey{typ: t, key: key}
	if v, ok := cache.Load(ck); ok {
		return v.([]*StructField)
	}

	var fields []*StructField

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tagVal, ok := f.Tag.Lookup(key)
		if ok {
			fields = append(fields, &StructField{
				Index:     i,
				Tag:       tagVal, // The value of the requested tag key
				FieldName: f.Name,
				value:     string(f.Tag),
			})
		}
	}

	cache.Store(ck, fields)
	return fields
}

// Value looks up the value of a specific tag key in the original struct field tag.
// This allows checking for auxiliary tags like `encrypt:"..."`.
func (sf *StructField) Value(key string) (string, bool) {
	// structtag.Get doesn't exist in stdlib reflect.StructTag which is just a string.
	// But reflect.StructTag has a .Get() and .Lookup() method.
	// sf.value is the raw tag string
	return reflect.StructTag(sf.value).Lookup(key)
}

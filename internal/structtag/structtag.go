package structtag

import (
	"reflect"
	"sync"
)

// StructField represents a field in a struct that has a specific tag.
type StructField struct {
	Index int
	Tag   string
	value string
}

// Value returns the value of a key in the tag, if present.
// For example, if the tag is `json:"name,omitempty"`, calling Value("json") would likely require parsing that specific format.
// However, the usage in orm.go suggests usage like `GetFieldsByTag(e, "model")` where the tag content *is* the value we care about.
// But wait, `field.Value("encrypt")` is also called.
// This implies the tag format might be key:value pairs or similar?
// Let's re-examine orm.go usage:
// 1. structtag.GetFieldsByTag(b.entity, "model")
//    - returns fields where `model:"..."` is present.
//    - field.Tag seems to return the value of the "model" tag.
//    - e.g. `model:"id"` -> tag="id"
//
// 2. structtag.GetFieldsByTag(e, "marshal")
//    - returns fields where `marshal:"..."` is present.
//    - field.Tag returns the value of the "marshal" tag (the property name).
//    - field.Value("encrypt") checks if there is also an "encrypt" tag? Or is it part of the same tag string?
//    - A common pattern is `tag:"key:value;key2:value2"` or similar, OR looking up a different tag key on the same field.
//
// Looking at `orm.go` line 269: `if sKey, ok := field.Value("encrypt"); ok`
// This suggests `field` might hold info about *all* tags on that struct field, or it parses the initial tag for options.
//
// If `GetFieldsByTag` is `(entity, tagName)`, it conceptually finds fields having `tagName:"..."`.
// `field.Tag` is likely the content of `tagName`.
// `field.Value("otherTag")` might look up "otherTag" on the SAME field.

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
				Index: i,
				Tag:   tagVal, // The value of the requested tag key
				value: string(f.Tag),
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

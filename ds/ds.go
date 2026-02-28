package ds

import (
	"bytes"
	"crypto/sha1"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"reflect"
	"time"

	"cloud.google.com/go/datastore"
)

const (
	// cacheLockTime is the max duration for a cache lock (32s).
	// datastore calls retry for up to 30s.
	cacheLockTime = 32 * time.Second

	// cacheMaxKeySize max size for an item key before hashing.
	cacheMaxKeySize = 250
)

var (
	typeOfPropertyLoadSaver = reflect.TypeOf((*datastore.PropertyLoadSaver)(nil)).Elem()
	typeOfPropertyList      = reflect.TypeOf(datastore.PropertyList(nil))
	typeOfKeyLoader         = reflect.TypeOf((*datastore.KeyLoader)(nil)).Elem()
)

// testing hooks
var (
	marshal   = MarshalPropertyList
	unmarshal = UnmarshalPropertyList
)

const (
	noneItem uint32 = iota
	entityItem
	lockItem
)

func init() {
	gob.Register(time.Time{})
	gob.Register(&datastore.Key{})
	gob.Register(datastore.GeoPoint{})
	gob.Register(&datastore.Entity{})
	gob.Register([]interface{}{})
}

type valueType int

const (
	valueTypeInvalid valueType = iota
	valueTypePropertyLoadSaver
	valueTypeStruct
	valueTypeStructPtr
	valueTypeInterface
	valueTypeKeyLoader
)

func checkValueType(valType reflect.Type) valueType {

	if reflect.PtrTo(valType).Implements(typeOfKeyLoader) {
		return valueTypeKeyLoader
	}

	if reflect.PtrTo(valType).Implements(typeOfPropertyLoadSaver) {
		return valueTypePropertyLoadSaver
	}

	switch valType.Kind() {
	case reflect.Struct:
		return valueTypeStruct
	case reflect.Interface:
		return valueTypeInterface
	case reflect.Ptr:
		valType = valType.Elem()
		if valType.Kind() == reflect.Struct {
			return valueTypeStructPtr
		}
	}
	return valueTypeInvalid
}

func checkKeysValues(keys []*datastore.Key, values reflect.Value) error {
	if values.Kind() != reflect.Slice {
		return errors.New("dsorm: values is not a slice")
	}

	if len(keys) != values.Len() {
		return errors.New("dsorm: keys and values slices have different length")
	}

	isNilErr, nilErr := false, make(datastore.MultiError, len(keys))
	for i, key := range keys {
		if key == nil {
			isNilErr = true
			nilErr[i] = datastore.ErrInvalidKey
		}
	}
	if isNilErr {
		return nilErr
	}

	if values.Type() == typeOfPropertyList {
		return errors.New("dsorm: PropertyList not supported")
	}

	if ty := checkValueType(values.Type().Elem()); ty == valueTypeInvalid {
		return errors.New("dsorm: unsupported vals type")
	}
	return nil
}

func createCacheKey(prefix string, key *datastore.Key) string {
	cacheKey := prefix + key.Encode()
	if len(cacheKey) > cacheMaxKeySize {
		hash := sha1.Sum([]byte(cacheKey))
		cacheKey = hex.EncodeToString(hash[:])
	}
	return cacheKey
}

func MarshalPropertyList(pl datastore.PropertyList) ([]byte, error) {
	buf := bytes.Buffer{}
	if err := gob.NewEncoder(&buf).Encode(&pl); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func UnmarshalPropertyList(data []byte, pl *datastore.PropertyList) error {
	return gob.NewDecoder(bytes.NewBuffer(data)).Decode(pl)
}

func setValue(val reflect.Value, pl datastore.PropertyList, key *datastore.Key) error {

	valType := checkValueType(val.Type())

	if valType == valueTypePropertyLoadSaver || valType == valueTypeStruct || valType == valueTypeKeyLoader {
		val = val.Addr()
	}

	if valType == valueTypeStructPtr && val.IsNil() {
		val.Set(reflect.New(val.Type().Elem()))
	}

	if pls, ok := val.Interface().(datastore.PropertyLoadSaver); ok {
		err := pls.Load(pl)
		if err != nil {
			return err
		}
		if e, ok := val.Interface().(datastore.KeyLoader); ok {
			err = e.LoadKey(key)
		}
		return err
	}

	return datastore.LoadStruct(val.Interface(), pl)
}

func isErrorsNil(errs []error) bool {
	for _, err := range errs {
		if err != nil {
			return false
		}
	}
	return true
}

func groupErrors(errs []error, total, limit int) error {
	groupedErrs := make(datastore.MultiError, total)
	for i, err := range errs {
		lo := i * limit
		hi := (i + 1) * limit
		if hi > total {
			hi = total
		}
		if me, ok := err.(datastore.MultiError); ok {
			copy(groupedErrs[lo:hi], me)
		} else if err != nil {
			for j := lo; j < hi; j++ {
				groupedErrs[j] = err
			}
		}
	}
	return groupedErrs
}

// getCacheLocks creates cache locks for keys, deduping them.
func getCacheLocks(prefix string, keys []*datastore.Key) ([]string, []*Item) {
	lockCacheKeys := make([]string, 0, len(keys))
	lockCacheItems := make([]*Item, 0, len(keys))
	set := make(map[string]struct{})
	for _, key := range keys {
		// Worst case scenario is that we lock the entity for cacheLockTime.
		// datastore.Delete will raise the appropriate error.
		if key != nil && !key.Incomplete() {
			cacheKey := createCacheKey(prefix, key)
			if _, found := set[cacheKey]; !found {
				item := &Item{
					Key:        cacheKey,
					Flags:      lockItem,
					Value:      itemLock(),
					Expiration: cacheLockTime,
				}
				lockCacheItems = append(lockCacheItems, item)
				lockCacheKeys = append(lockCacheKeys, item.Key)
				set[cacheKey] = struct{}{}
			}
		}
	}
	return lockCacheKeys, lockCacheItems
}

package ardb

import (
	"errors"

	"github.com/garyburd/redigo/redis"
)

// Error is a helper that allows you to extract just the error from a reply.
func Error(_ interface{}, err error) error {
	return err
}

// Bytes is a helper that converts a command reply to a slice of bytes. If err
// is not equal to nil, then Bytes returns nil,
func Bytes(reply interface{}, err error) ([]byte, error) {
	return redis.Bytes(reply, err)
}

// OptBytes tries to interpret the given reply as a byte slice,
// however if no reply is returned, this function will return a nil slice,
// rather than an error.
func OptBytes(reply interface{}, err error) ([]byte, error) {
	content, err := redis.Bytes(reply, err)
	if err == redis.ErrNil {
		err = nil
	}
	return content, err
}

// Int64s is a helper that converts an array command reply to a []int64.
// If err is not equal to nil, then Int64s returns the error.
// Int64s returns an error if an array item is not an integer.
func Int64s(reply interface{}, err error) ([]int64, error) {
	values, err := redis.Values(reply, err)
	if err != nil {
		return nil, err
	}

	var ints []int64
	if err := redis.ScanSlice(values, &ints); err != nil {
		return nil, err
	}
	if len(ints) == 0 {
		return nil, redis.ErrNil
	}

	return ints, nil
}

// Int64ToBytesMapping is a helper that converts an array command reply to a map[int64][]byte.
// If err is not equal to nil, then this function returns the error.
// If the given reply can also not be transformed into a `map[int64][]byte` this function returns an error as well.
func Int64ToBytesMapping(reply interface{}, err error) (map[int64][]byte, error) {
	values, err := redis.Values(reply, err)
	if err != nil {
		return nil, err
	}
	if len(values)%2 != 0 {
		return nil, errors.New("Int64ToBytesMapping expects even number of values result")
	}
	m := make(map[int64][]byte, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		key, err := redis.Int64(values[i], nil)
		if err != nil {
			return nil, err
		}
		value, err := redis.Bytes(values[i+1], nil)
		if err != nil {
			return nil, err
		}
		m[key] = value
	}
	return m, nil
}

package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestType_Slice(t *testing.T) {
	var stringArray = Array(String)

	assert := assert.New(t)
	assert.Equal("array[string]", stringArray.Name())

	assert.True(stringArray.Check([]interface{}{"a", "b", "c"}))
	assert.False(stringArray.Check("test"))
	assert.False(stringArray.Check(1))
	v, err := stringArray.Convert([]interface{}{"a", "b", "c"})
	assert.Nil(err)
	assert.Equal([]interface{}{"a", "b", "c"}, v)
	v, err = stringArray.Convert("test")
	assert.Equal(ErrInvalidType, err)
	assert.Nil(v)
	assert.Equal(-1, stringArray.Compare([]interface{}{"a", "b"}, []interface{}{"a", "b", "c"}))
	assert.Equal(-1, stringArray.Compare([]interface{}{"a", "b", "c"}, []interface{}{"a", "b", "d"}))
	assert.Equal(0, stringArray.Compare([]interface{}{"a", "b", "c"}, []interface{}{"a", "b", "c"}))
	assert.Equal(0, stringArray.Compare([]interface{}{}, []interface{}{}))
	assert.Equal(1, stringArray.Compare([]interface{}{"a", "b", "c"}, []interface{}{"a", "b"}))
	assert.Equal(1, stringArray.Compare([]interface{}{"a", "b", "d"}, []interface{}{"a", "b", "c"}))
}
func TestType_String(t *testing.T) {
	var v interface{}
	var err error
	assert := assert.New(t)
	assert.True(String.Check(""))
	assert.False(String.Check(1))
	assert.False(String.Check(int32(1)))
	v, err = String.Convert("")
	assert.Nil(err)
	assert.Equal("", v)
	v, err = String.Convert(1)
	assert.Equal(ErrInvalidType, err)
	assert.Nil(v)
	assert.Equal(-1, String.Compare("a", "b"))
	assert.Equal(0, String.Compare("a", "a"))
	assert.Equal(1, String.Compare("b", "a"))
}

func TestType_Integer(t *testing.T) {
	var v interface{}
	var err error
	assert := assert.New(t)
	assert.True(Integer.Check(int32(1)))
	assert.False(Integer.Check(1))
	assert.False(Integer.Check(int64(1)))
	assert.False(Integer.Check(""))
	v, err = Integer.Convert(int32(1))
	assert.Nil(err)
	assert.Equal(int32(1), v)
	v, err = Integer.Convert(1)
	assert.Nil(err)
	assert.Equal(int32(1), v)
	v, err = Integer.Convert(int64(1))
	assert.Nil(err)
	assert.Equal(int32(1), v)
	v, err = Integer.Convert("")
	assert.NotNil(err)
	assert.Nil(v)
	v, err = Integer.Convert(int64(9223372036854775807))
	assert.NotNil(err)
	assert.Nil(v)
	v, err = Integer.Convert(uint32(4294967295))
	assert.NotNil(err)
	assert.Nil(v)
	v, err = Integer.Convert(uint64(18446744073709551615))
	assert.NotNil(err)
	assert.Nil(v)
	assert.Equal(-1, Integer.Compare(int32(1), int32(2)))
	assert.Equal(0, Integer.Compare(int32(1), int32(1)))
	assert.Equal(1, Integer.Compare(int32(2), int32(1)))
}

func TestType_BigInteger(t *testing.T) {
	var v interface{}
	var err error
	assert := assert.New(t)
	assert.True(BigInteger.Check(int64(1)))
	assert.False(BigInteger.Check(1))
	assert.False(BigInteger.Check(int32(1)))
	assert.False(BigInteger.Check(""))
	v, err = BigInteger.Convert(int64(1))
	assert.Nil(err)
	assert.Equal(int64(1), v)
	v, err = BigInteger.Convert(1)
	assert.Nil(err)
	assert.Equal(int64(1), v)
	v, err = BigInteger.Convert(int32(1))
	assert.Nil(err)
	assert.Equal(int64(1), v)
	v, err = BigInteger.Convert(int64(9223372036854775807))
	assert.Nil(err)
	assert.Equal(int64(9223372036854775807), v)
	v, err = BigInteger.Convert(uint32(4294967295))
	assert.Nil(err)
	assert.Equal(int64(4294967295), v)
	v, err = BigInteger.Convert(uint64(18446744073709551615))
	assert.NotNil(err)
	assert.Nil(v)
	v, err = BigInteger.Convert("")
	assert.NotNil(err)
	assert.Nil(v)
	assert.Equal(-1, BigInteger.Compare(int64(1), int64(2)))
	assert.Equal(0, BigInteger.Compare(int64(1), int64(1)))
	assert.Equal(1, BigInteger.Compare(int64(2), int64(1)))
}

package pq

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"github.com/lib/pq/oid"
	"strconv"
	"time"
)

// if a codec pack was not assigned to DB then this one will be used
var defaultCodec *codec

// map of named codecs set by RegisterCodec
var registeredCodecs map[string]*codec

// converts bytes from postgres into a driver.Value
type Decoder func(s []byte) (driver.Value, error)

// converts go-types to a suitable driver.Value
type Encoder func(v interface{}) (driver.Value, error)

// allow Encoder to be used as ValueConverter
func (e Encoder) ConvertValue(v interface{}) (driver.Value, error) {
	return e(v)
}

// Codec is a collection of Encoders/Decoders used to translate
// between postgres bytes and go-types
// The defaultCodec handles all the values as required by the
// database/sql package. You can extend this behaviour to better
// support custom types, arrays, composites etc by creating a custom
// codec, registering it by name and then refering to the codec in
// your connection dsn
type codec struct {
	encoders       map[oid.Oid]Encoder
	defaultEncoder Encoder
	decoders       map[oid.Oid]Decoder
	defaultDecoder Decoder
}

// register a Decoder function that will be called for the given
// (pg_type) oid. Decoders can only be registered once!
// Decoders for the default types are pre-consignured this
// means that you cannot override the default types. This is by
// design to ensure that you do not break compatibility with other
// packages that expect the driver.Value types to behave as specified
func (c *codec) RegisterDecoder(typ oid.Oid, d Decoder) error {
	_, exists := c.decoders[typ]
	if exists {
		return fmt.Errorf("Decoder already exists for oid %d", typ)
	}
	c.decoders[typ] = d
	return nil
}

// the default decoder is used when no other decoders match by oid
// you may use this as a catch-all decoder for all non-default types
func (c *codec) RegisterDefaultDecoder(d Decoder) error {
	c.defaultDecoder = d
	return nil
}

func (c *codec) RegisterEncoder(typ oid.Oid, e Encoder) error {
	_, exists := c.encoders[typ]
	if exists {
		return fmt.Errorf("Encoder already exists for oid %d", typ)
	}
	c.encoders[typ] = e
	return nil
}

func (c *codec) RegisterDefaultEncoder(e Encoder) error {
	c.defaultEncoder = e
	return nil
}

func (c *codec) decode(s []byte, typ oid.Oid) (interface{}, error) {
	dec, ok := c.decoders[typ]
	if !ok {
		dec = c.defaultDecoder
	}
	return dec(s)
}

// returns a ValueConverter for the given oid
func (c *codec) encoder(typ oid.Oid) driver.ValueConverter {
	enc, ok := c.encoders[typ]
	if !ok {
		enc = c.defaultEncoder
	}
	return enc
}

// converts the driver.Value into the raw bytes for postgres
func (c *codec) encode(x driver.Value, typ oid.Oid) []byte {
	switch v := x.(type) {
	case int64:
		return []byte(fmt.Sprintf("%d", v))
	case float32, float64:
		return []byte(fmt.Sprintf("%f", v))
	case []byte:
		if typ == oid.T_bytea {
			return []byte(fmt.Sprintf("\\x%x", v))
		}
		return v
	case string:
		if typ == oid.T_bytea {
			return []byte(fmt.Sprintf("\\x%x", v))
		}
		return []byte(v)
	case bool:
		return []byte(fmt.Sprintf("%t", v))
	case time.Time:
		return []byte(v.Format(time.RFC3339Nano))
	default:
		errorf("encode: unknown type for %T", v)
	}

	panic("not reached")
}

// Creates a new codec for configuring how values are encoded/decoded
// encoders/decoders for all default types will be pre-configured.
func NewCodec() *codec {
	c := new(codec)
	c.decoders = make(map[oid.Oid]Decoder)
	c.encoders = make(map[oid.Oid]Encoder)
	c.decoders[oid.T_bytea] = DefaultByteaDecoder
	c.encoders[oid.T_bytea] = DefaultEncoder
	c.decoders[oid.T_timestamptz] = DefaultTimestampzDecoder
	c.encoders[oid.T_timestamptz] = DefaultEncoder
	c.decoders[oid.T_timestamp] = DefaultTimestampDecoder
	c.encoders[oid.T_timestamp] = DefaultEncoder
	c.decoders[oid.T_time] = DefaultTimeDecoder
	c.encoders[oid.T_time] = DefaultEncoder
	c.decoders[oid.T_timetz] = DefaultTimezDecoder
	c.encoders[oid.T_timetz] = DefaultEncoder
	c.decoders[oid.T_date] = DefaultDateDecoder
	c.encoders[oid.T_date] = DefaultEncoder
	c.decoders[oid.T_bool] = DefaultBoolDecoder
	c.encoders[oid.T_bool] = DefaultEncoder
	c.decoders[oid.T_int2] = DefaultIntDecoder
	c.encoders[oid.T_int2] = DefaultEncoder
	c.decoders[oid.T_int4] = DefaultIntDecoder
	c.encoders[oid.T_int4] = DefaultEncoder
	c.decoders[oid.T_int8] = DefaultIntDecoder
	c.encoders[oid.T_int8] = DefaultEncoder
	c.decoders[oid.T_float4] = DefaultFloat32Decoder
	c.encoders[oid.T_float4] = DefaultEncoder
	c.decoders[oid.T_float8] = DefaultFloat64Decoder
	c.encoders[oid.T_float8] = DefaultEncoder
	c.defaultDecoder = DefaultDecoder
	c.defaultEncoder = DefaultEncoder
	return c
}

func DefaultEncoder(v interface{}) (driver.Value, error) {
	return driver.DefaultParameterConverter.ConvertValue(v)
}

func DefaultByteaDecoder(s []byte) (driver.Value, error) {
	s = s[2:] // trim off "\\x"
	d := make([]byte, hex.DecodedLen(len(s)))
	_, err := hex.Decode(d, s)
	if err != nil {
		errorf("%s", err)
	}
	return d, nil
}

func DefaultTimestampzDecoder(s []byte) (driver.Value, error) {
	return mustParse("2006-01-02 15:04:05-07", oid.T_timestamptz, s)
}

func DefaultTimestampDecoder(s []byte) (driver.Value, error) {
	return mustParse("2006-01-02 15:04:05", oid.T_timestamp, s)
}

func DefaultTimeDecoder(s []byte) (driver.Value, error) {
	return mustParse("15:04:05", oid.T_time, s)
}

func DefaultTimezDecoder(s []byte) (driver.Value, error) {
	return mustParse("15:04:05-07", oid.T_timetz, s)
}

func DefaultDateDecoder(s []byte) (driver.Value, error) {
	return mustParse("2006-01-02", oid.T_date, s)
}

func DefaultBoolDecoder(s []byte) (driver.Value, error) {
	return s[0] == 't', nil
}

func DefaultIntDecoder(s []byte) (driver.Value, error) {
	i, err := strconv.ParseInt(string(s), 10, 64)
	if err != nil {
		errorf("%s", err)
	}
	return i, nil
}

func DefaultFloat64Decoder(s []byte) (driver.Value, error) {
	return strconv.ParseFloat(string(s), 64)
}

func DefaultFloat32Decoder(s []byte) (driver.Value, error) {
	return strconv.ParseFloat(string(s), 32)
}

func DefaultDecoder(s []byte) (driver.Value, error) {
	return s, nil
}

// register a codec by name
// pass the registered codec name as a connection param
// in the dsn (when calling Open)
// will panic if a codec is already set by that name
func RegisterCodec(name string, c *codec) {
	if _, exists := registeredCodecs[name]; exists {
		panic("Named codec already exists")
	}
	registeredCodecs[name] = c
}

// register a Decoder function that will be called for the given
// (pg_type) oid. Decoders can only be registered once!
// Decoders for the default types are pre-consignured this
// means that you cannot override the default types. This is by
// design to ensure that you do not break compatibility with other
// packages that expect the driver.Value types to behave as specified
func RegisterDecoder(typ oid.Oid, d Decoder) error {
	return defaultCodec.RegisterDecoder(typ, d)
}

// the default decoder is used when no other decoders match by oid
// you may use this as a catch-all decoder for all non-default types
func RegisterDefaultDecoder(d Decoder) error {
	return defaultCodec.RegisterDefaultDecoder(d)
}

func RegisterEncoder(typ oid.Oid, e Encoder) error {
	return defaultCodec.RegisterEncoder(typ, e)
}

func RegisterDefaultEncoder(e Encoder) error {
	return defaultCodec.RegisterDefaultEncoder(e)
}

func mustParse(f string, typ oid.Oid, s []byte) (t time.Time, err error) {
	str := string(s)

	// Special case until time.Parse bug is fixed:
	// http://code.google.com/p/go/issues/detail?id=3487
	if str[len(str)-2] == '.' {
		str += "0"
	}

	// check for a 30-minute-offset timezone
	if (typ == oid.T_timestamptz || typ == oid.T_timetz) &&
		str[len(str)-3] == ':' {
		f += ":00"
	}
	t, err = time.Parse(f, str)
	if err != nil {
		return t, fmt.Errorf("decode: %s", err)
	}
	return t, nil
}

type NullTime struct {
	Time  time.Time
	Valid bool // Valid is true if Time is not NULL
}

// Scan implements the Scanner interface.
func (nt *NullTime) Scan(value interface{}) error {
	nt.Time, nt.Valid = value.(time.Time)
	return nil
}

// Value implements the driver Valuer interface.
func (nt NullTime) Value() (driver.Value, error) {
	if !nt.Valid {
		return nil, nil
	}
	return nt.Time, nil
}

func init() {
	registeredCodecs = make(map[string]*codec)
	defaultCodec = NewCodec()
}

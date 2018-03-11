package uuid

import (
  "fmt"
  "encoding/json"
  "database/sql/driver"
)

import (
  "github.com/bww/go-util/uuid"
)

// A UUID identifier
type UUID uuid.UUID

var   Zero UUID = [16]byte{}
const Bytes     = 16
const Len       = 36

// Encode an identifier from its string representation
func New() UUID {
  return UUID(uuid.Random())
}

// Create an identifier from its string representation
func Parse(s string) (UUID, error) {
  u, err := uuid.Parse(s)
  if err != nil {
    return Zero, err
  }else{
    return UUID(u), nil
  }
}

// Create an identifier from a slice of bytes
func FromBytes(b []byte) (UUID, error) {
  u, err := uuid.FromBytes(b)
  if err != nil {
    return Zero, err
  }else{
    return UUID(u), nil
  }
}

// Create a new identifier. This is implemented to conform to persist.Ident.
func (v UUID) New() interface{} {
  return New()
}

// Stringer
func (v UUID) String() string {
  return uuid.UUID(v).String()
}

// Marshal
func (v UUID) MarshalJSON() ([]byte, error) {
  if v == Zero {
    return []byte("null"), nil
  }else{
    return []byte(`"`+ v.String() +`"`), nil
  }
}

// Marshal
func (v *UUID) UnmarshalJSON(data []byte) error {
  var s string
  err := json.Unmarshal(data, &s)
  if err != nil {
    return err
  }
  if s == "null" {
    *v = Zero
  }else{
    *v, err = Parse(s)
    if err != nil {
      return err
    }
  }
  return nil
}

// Value
func (v UUID) Value() (driver.Value, error) {
  return v.String(), nil
}

// Scan
func (v *UUID) Scan(src interface{}) error {
  var err error
  switch c := src.(type) {
    case []byte:
      *v, err = Parse(string(c))
    case string:
      *v, err = Parse(c)
    default:
      err = fmt.Errorf("Unsupported type: %T", src)
  }
  return err
}

// Determine if an input string resembles a UUID
func ResemblesUUID(s string) bool {
  return uuid.ResemblesUUID(s)
}

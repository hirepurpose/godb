package persist

import (
  "reflect"
)

/**
 * A scanner
 */
type scanner struct {
  mapping *mapping
  value  reflect.Value
}

/**
 * Obtain a struct mapping
 */
func Scanner(v interface{}) *scanner {
  m, err := Mapping(reflect.TypeOf(v))
  if err != nil {
    panic(err)
  }
  return &scanner{m, reflect.ValueOf(v)}
}

/**
 * Obtain a list of primary key columns
 */
func (s *scanner) PrimaryKeys() []string {
  return s.mapping.PrimaryKeys()
}

/**
 * Obtain a list of property columns
 */
func (s *scanner) Properties() []string {
  return s.mapping.Properties()
}

/**
 * Obtain the single primary key identifier
 */
func (s *scanner) Id() (interface{}, error) {
  return s.mapping.Id(s.value)
}

/**
 * Set the single primary key identifier
 */
func (s *scanner) SetId(id interface{}) error {
  return s.mapping.SetId(s.value, id)
}

/**
 * Produce a new identifier
 */
func (s *scanner) NewId() (interface{}, error) {
  return s.mapping.NewId(s.value)
}

/**
 * Obtain a map of property names to values
 */
func (s *scanner) Values(pk bool, op Operation) (Columns, error) {
  return s.mapping.Values(s.value, pk, op)
}

/**
 * Obtain scanning destinations for the provided set of columns
 */
func (s *scanner) Dests(names []string) ([]interface{}, Columns, error) {
  return s.mapping.Dests(s.value, names)
}

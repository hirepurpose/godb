package persist

import (
  "reflect"
)

type mappingEntity mapping

func newMappingEntity(v interface{}) *mappingEntity {
  m, err := Mapping(reflect.TypeOf(v))
  if err != nil {
    panic(err)
  }
  return (*mappingEntity)(m)
}

func newMappingEntityForType(t reflect.Type) *mappingEntity {
  m, err := Mapping(t)
  if err != nil {
    panic(err)
  }
  return (*mappingEntity)(m)
}

func (e *mappingEntity) PrimaryKeys() []string {
  return (*mapping)(e).PrimaryKeys()
}

func (e *mappingEntity) Columns() []string {
  return (*mapping)(e).Properties()
}

func (e *mappingEntity) PersistentId(v interface{}) interface{} {
  x, err := (*mapping)(e).Id(reflect.ValueOf(v))
  if err != nil {
    panic(err)
  }
  return x
}

func (e *mappingEntity) SetPersistentId(v, id interface{}) error {
  return (*mapping)(e).SetId(reflect.ValueOf(v), id)
}

func (e *mappingEntity) NewPersistentId(v interface{}) interface{} {
  x, err := (*mapping)(e).NewId(reflect.ValueOf(v))
  if err != nil {
    panic(err)
  }
  return x
}

func (e *mappingEntity) PersistentValues(v interface{}) (Columns, error) {
  return (*mapping)(e).Values(reflect.ValueOf(v), false, Write)
}

func (e *mappingEntity) ValueDestinations(v interface{}, cols []string) ([]interface{}, Columns, error) {
  return (*mapping)(e).Dests(reflect.ValueOf(v), cols)
}

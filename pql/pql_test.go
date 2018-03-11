package pql

import (
  "fmt"
  "testing"
)

import (
  "github.com/stretchr/testify/assert"
)

func TestParser(t *testing.T) {
  var err error
  var q *Query
  
  cols := []string{
    "id",
    "name",
    "description",
    "created_at",
  }
  
  // lexing, meta isolation
  
  _, err = Parse(`select "'*" 'YES\'\\' from { * } yeah...`, cols)
  assert.Nil(t, err, fmt.Sprintf("%v", err))
  
  _, err = Parse(`select "'*" 'YES\'\\' from { ok, yes, that } yeah...`, cols)
  assert.Nil(t, err, fmt.Sprintf("%v", err))
  
  _, err = Parse(`select "'* 'YES\'\\' from { ok "\}" } yeah...`, cols)
  assert.NotNil(t, err)
  
  _, err = Parse(`select "'*" 'YES\'\\' from { ok "\}" } yeah...`, cols)
  assert.NotNil(t, err)
  
  // invalid grammars
  
  _, err = Parse(`select {} from gdb_test`, cols)
  assert.NotNil(t, err)
  
  _, err = Parse(`select {p.id, p.*} from gdb_test`, cols)
  assert.NotNil(t, err)
  
  _, err = Parse(`select {*.id} from gdb_test`, cols)
  assert.NotNil(t, err)
  
  // valid grammars
  
  q, err = Parse(`select {*} from gdb_test`, cols)
  if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    assert.Equal(t, `select id, name, description, created_at from gdb_test`, q.SQL)
    assert.Equal(t, []string{"id", "name", "description", "created_at"}, q.Columns)
  }
  
  q, err = Parse(`select {id} from gdb_test`, cols)
  if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    assert.Equal(t, `select id from gdb_test`, q.SQL)
    assert.Equal(t, []string{"id"}, q.Columns)
  }
  
  q, err = Parse(`select {p.*} from gdb_test`, cols)
  if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    assert.Equal(t, `select p.id, p.name, p.description, p.created_at from gdb_test`, q.SQL)
    assert.Equal(t, []string{"id", "name", "description", "created_at"}, q.Columns)
  }
  
  q, err = Parse(`select {p.id, p.name} from gdb_test`, cols)
  if assert.Nil(t, err, fmt.Sprintf("%v", err)) {
    assert.Equal(t, `select p.id, p.name from gdb_test`, q.SQL)
    assert.Equal(t, []string{"id", "name"}, q.Columns)
  }
  
}

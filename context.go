package godb

import (
  "fmt"
  "database/sql"
)

import (
  "github.com/bww/go-util/text"
)

// An SQL executable context
type Context interface {
  Exec(query string, args ...interface{})(sql.Result, error)
  Query(query string, args ...interface{})(*sql.Rows, error)
  QueryRow(query string, args ...interface{})(*sql.Row)
}

// A debug context which logs out statements
type DebugContext struct{
  prefix  string
  cxt     Context
}

// Create a debug context
func NewDebugContext(cxt Context) DebugContext {
  return DebugContext{cxt:cxt}
}

// Create a debug context
func NewDebugContextWithPrefix(prefix string, cxt Context) DebugContext {
  return DebugContext{prefix, cxt}
}

func (d DebugContext) Exec(query string, args ...interface{}) (sql.Result, error) {
  fmt.Println("db/exec:"+ d.prefix, text.CollapseSpaces(query), args)
  return d.cxt.Exec(query, args...)
}

func (d DebugContext) Query(query string, args ...interface{}) (*sql.Rows, error) {
  fmt.Println("db/query/n:"+ d.prefix, text.CollapseSpaces(query), args)
  return d.cxt.Query(query, args...)
}

func (d DebugContext) QueryRow(query string, args ...interface{}) *sql.Row {
  fmt.Println("db/query/1:"+ d.prefix, text.CollapseSpaces(query), args)
  return d.cxt.QueryRow(query, args...)
}

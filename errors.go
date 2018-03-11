package godb

import (
  "fmt"
)

import (
  "github.com/lib/pq"
)

var (
  ErrNotFound       = fmt.Errorf("Not found")
  ErrTransient      = fmt.Errorf("Transient")
  ErrImmutable      = fmt.Errorf("Immutable")
  ErrInconvertible  = fmt.Errorf("Inconvertible")
  ErrForbidden      = fmt.Errorf("Forbidden")
	ErrInvalidEntity  = fmt.Errorf("Invalid Entity")
)

// Postgres errors from: https://github.com/lib/pq/blob/master/error.go#L78
const (
  PG_ERROR_UNIQUE_VIOLATION       = "23505"
  PG_ERROR_FOREIGN_KEY_VIOLATION  = "23503"
)

// Is the error a Postgres unique violation?
func IsUniqueViolation(e error) bool {
  switch c := e.(type) {
    case pq.Error:
      return c.Code == PG_ERROR_UNIQUE_VIOLATION
    case *pq.Error:
      return c.Code == PG_ERROR_UNIQUE_VIOLATION
    default:
      return false
  }
}

// Is the error a Postgres foreign key violation?
func IsForeignKeyViolation(e error) bool {
  switch c := e.(type) {
    case pq.Error:
      return c.Code == PG_ERROR_FOREIGN_KEY_VIOLATION
    case *pq.Error:
      return c.Code == PG_ERROR_FOREIGN_KEY_VIOLATION
    default:
      return false
  }
}

func ErrNotFoundInTable(table string) error {
	return fmt.Errorf("Not found in table: %v", table)
} 

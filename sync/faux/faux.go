package faux

import (
  "github.com/hirepurpose/godb/sync"
)

// A faux mutex which does no actual locking
type fauxMutex struct {}

func (m fauxMutex) Lock() error {
  return nil
}

func (m fauxMutex) Unlock() error {
  return nil
}

func (m fauxMutex) Perform(f func()error) error {
  return f()
}

// A faux lock service which does no actual locking
type fauxService struct {}

func New() sync.Service {
  return fauxService{}
}

func (s fauxService) Mutex(string) (sync.Mutex, error) {
  return fauxMutex{}, nil
}

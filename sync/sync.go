package sync

// A mutex
type Mutex interface {
  Lock()(error)
  Unlock()(error)
  Perform(func()error)(error)
}

// A sync service
type Service interface {
  Mutex(string)(Mutex, error)
}

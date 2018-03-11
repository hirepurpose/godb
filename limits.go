package gdb

// A range
type Range struct {
  Location  int  `json:"location"`
  Length    int  `json:"length"`
}

var InvalidRange  = Range{0, -1}
var ZeroRange     = Range{0,  0}

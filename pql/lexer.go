package pql

import (
  "fmt"
  "unicode"
)

// Token types
const (
  
  tokenError tokenType  = iota
  tokenEOF
  
  tokenIdent
  
  tokenComma            = ','
  tokenDot              = '.'
  tokenStar             = '*'
  
)

// Token type string
func (t tokenType) String() string {
  switch t {
    case tokenError:
      return "Error"
    case tokenEOF:
      return "EOF"
    case tokenIdent:
      return "Ident"
    default:
      if t < 128 {
        return fmt.Sprintf("'%v'", string(t))
      }else{
        return fmt.Sprintf("%U", t)
      }
  }
}

// Entry action.
func entryAction(s *scanner) scannerAction {
  for {
    switch r := s.next(); {
      
      case r == eof:
        s.emit(token{span{s.text, len(s.text), 0}, tokenEOF, nil})
        return nil
        
      case unicode.IsSpace(r):
        s.ignore()
        
      case r == '_' || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z'):
        s.backup() // unget the first character
        return identifierAction
        
      case r == '*', r == '.', r == ',':
        s.emit(token{span{s.text, s.start, s.index - s.start}, tokenType(r), string(r)})
        return entryAction
        
      default:
        return s.error(s.errorf(span{s.text, s.index, 1}, nil, "Syntax error"))
        
    }
  }
  return entryAction
}

// Identifier
func identifierAction(s *scanner) scannerAction {
  
  v, err := s.scanIdentifier()
  if err != nil {
    s.error(s.errorf(span{s.text, s.index, 1}, err, "Invalid identifier"))
  }
  
  t := span{s.text, s.start, s.index - s.start}
  s.emit(token{t, tokenIdent, v})
  
  return entryAction
}

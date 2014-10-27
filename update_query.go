package datastore

import (
  "errors"
  "fmt"
  "reflect"
  "strings"

  "github.com/gocql/gocql"
)

func NewUpdateQuery(typ reflect.Type) (*UpdateQuery, error) {
  codec, err := getStructCodec(typ)
  if err != nil {
    return nil, err
  }
  return &UpdateQuery{
    codec:   codec,
    updates: make(map[string]interface{}),
  }, nil
}

type UpdateQuery struct {
  filter  []filter
  ttl     int64
  updates map[string]interface{}
  codec   *structCodec

  err error
}

func (q *UpdateQuery) clone() *UpdateQuery {
  x := *q
  if len(q.filter) > 0 {
    x.filter = make([]filter, len(q.filter))
    copy(x.filter, q.filter)
  }
  if len(q.updates) > 0 {
    x.updates = make(map[string]interface{})
    for k, v := range q.updates {
      x.updates[k] = v
    }
  }
  return &x
}

// Filter returns a derivative query with a field-based filter.
// The filterStr argument must be a field name followed by optional space,
// followed by an operator, one of ">", "<", ">=", "<=", or "=".
// Fields are compared against the provided value using the operator.
// Multiple filters are AND'ed together.
func (q *UpdateQuery) Filter(filterStr string, value interface{}) *UpdateQuery {
  q = q.clone()
  filterStr = strings.TrimSpace(filterStr)
  if len(filterStr) < 1 {
    q.err = errors.New("datastore: invalid filter: " + filterStr)
    return q
  }
  f := filter{
    FieldName: strings.TrimRight(filterStr, " ><=!"),
    Value:     value,
  }
  switch op := strings.TrimSpace(filterStr[len(f.FieldName):]); op {
  case "<=":
    f.Op = lessEq
  case ">=":
    f.Op = greaterEq
  case "<":
    f.Op = lessThan
  case ">":
    f.Op = greaterThan
  case "=":
    f.Op = equal
  default:
    q.err = fmt.Errorf("datastore: invalid operator %q in filter %q", op, filterStr)
    return q
  }
  q.filter = append(q.filter, f)
  return q
}

func (q *UpdateQuery) TTL(ttl int64) *UpdateQuery {
  q = q.clone()
  q.ttl = ttl
  return q
}

func (q *UpdateQuery) Update(fieldName string, fieldVal interface{}) *UpdateQuery {
  q = q.clone()
  q.updates[fieldName] = fieldVal
  return q
}

func (q *UpdateQuery) toCQL() (cql string, args []interface{}, err error) {
  usingTTL := " "

  if q.ttl > 0 {
    usingTTL = fmt.Sprint(" USING TTL %d ", q.ttl)
  }

  cql = fmt.Sprintf("UPDATE %s%sSET ", q.codec.columnFamily, usingTTL)

  if len(q.updates) > 0 {
    updates := make([]string, len(q.updates))
    i := 0
    for k, v := range q.updates {
      updates[i] = fmt.Sprintf("%s = ?", k)
      args = append(args, v)
      i += 1
    }
    cql = cql + strings.Join(updates, ", ")
  }

  whereClause, whereArgs, err := getWhereClause(q.codec, q.filter)
  if err != nil {
    return "", whereArgs, err
  }
  cql = cql + whereClause
  args = append(args, whereArgs...)

  return cql, args, nil
}

func (q *UpdateQuery) CQL() (string, error) {
  cql, _, err := q.toCQL()
  return cql, err
}

func (q *UpdateQuery) Run(session *gocql.Session) error {
  cql, args, err := q.toCQL()
  if err != nil {
    return err
  }
  cqlQ := session.Query(cql, args...)
  return cqlQ.Exec()
}

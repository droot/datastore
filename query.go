package datastore

import (
  "errors"
  "fmt"
  "math"
  "strings"

  "github.com/gocql/gocql"
)

type operator int

const (
  lessThan operator = iota
  lessEq
  equal
  greaterEq
  greaterThan
)

// filter is a conditional filter on query results.
type filter struct {
  FieldName string
  Op        operator
  Value     interface{}
}

type sortDirection int

const (
  ascending sortDirection = iota
  descending
)

// order is a sort order on query results.
type order struct {
  FieldName string
  Direction sortDirection
}

// NewQuery creates a new Query for a specific entity kind.
func NewQuery(kind string, entity interface{}) (*Query, error) {
  cls, err := newStructCLS(entity) // column load saver type
  if err != nil {
    return nil, err
  }
  return &Query{
    kind:  kind,
    limit: -1,
    cls:   cls,
  }, nil
}

// Query represents a CQL query.
type Query struct {
  kind       string
  filter     []filter
  order      []order
  projection []string
  cls        *structCLS

  limit int32

  err error
}

func (q *Query) clone() *Query {
  x := *q
  // Copy the contents of the slice-typed fields
  if len(q.filter) > 0 {
    x.filter = make([]filter, len(q.filter))
    copy(x.filter, q.filter)
  }
  if len(q.order) > 0 {
    x.order = make([]order, len(q.order))
    copy(x.order, q.order)
  }
  return &x
}

// Filter returns a derivative query with a field-based filter.
// The filterStr argument must be a field name followed by optional space,
// followed by an operator, one of ">", "<", ">=", "<=", or "=".
// Fields are compared against the provided value using the operator.
// Multiple filters are AND'ed together.
func (q *Query) Filter(filterStr string, value interface{}) *Query {
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

// Order returns a derivative query with a field-based sort order. Orders are
// applied in the order they are added. The default order is ascending; to sort
// in descending order prefix the fieldName with a minus sign (-).
func (q *Query) Order(fieldName string) *Query {
  q = q.clone()
  fieldName = strings.TrimSpace(fieldName)
  o := order{
    Direction: ascending,
    FieldName: fieldName,
  }
  if strings.HasPrefix(fieldName, "-") {
    o.Direction = descending
    o.FieldName = strings.TrimSpace(fieldName[1:])
  } else if strings.HasPrefix(fieldName, "+") {
    q.err = fmt.Errorf("datastore: invalid order: %q", fieldName)
    return q
  }
  if len(o.FieldName) == 0 {
    q.err = errors.New("datastore: empty order")
    return q
  }
  q.order = append(q.order, o)
  return q
}

// Project returns a derivative query that yields only the given fields.
func (q *Query) Project(fieldNames ...string) *Query {
  q = q.clone()
  q.projection = append([]string(nil), fieldNames...)
  return q
}

// Limit returns a derivative query that has a limit on the number of results
// returned. A negative value means unlimited.
func (q *Query) Limit(limit int) *Query {
  q = q.clone()
  if limit < math.MinInt32 || limit > math.MaxInt32 {
    q.err = errors.New("datastore: query limit overflow")
    return q
  }
  q.limit = int32(limit)
  return q

}

var filterOpMapping = map[operator]string{
  lessEq:      "<=",
  greaterEq:   ">=",
  lessThan:    "<",
  greaterThan: ">",
  equal:       "=",
}

// toCQL returns CQL query statement corresponding to the query q.
func (q *Query) toCQL() (string, []interface{}, error) {

  cls := q.cls

  var columnStr string
  if len(q.projection) > 0 {
    columnStr = strings.Join(q.projection, ",")
  } else {
    columnStr = cls.getColumnStr()
  }

  cql := fmt.Sprintf("SELECT %s FROM %s", columnStr, q.kind)

  var args []interface{}

  if len(q.filter) > 0 {

    conditions := make([]string, len(q.filter))
    args = make([]interface{}, len(q.filter))

    for i, filter := range q.filter {
      _, ok := cls.codec.byName[filter.FieldName]
      if !ok {
        return "", args, fmt.Errorf("query : fieldname %s not found", filter.FieldName)
      }
      conditions[i] = fmt.Sprintf("%s %s ?", filter.FieldName,
        filterOpMapping[filter.Op])
      args[i] = filter.Value
    }

    cql = cql + " WHERE " + strings.Join(conditions, " AND ")
  }

  if q.limit > 0 {
    cql = cql + fmt.Sprintf(" LIMIT %d", q.limit)
  }

  if len(q.order) > 0 {
    // TODO (sunil): implement order by clause
  }

  return cql, args, nil
}

// Run returns Iterator by executing the query.
func (q *Query) Run(session *gocql.Session) *Iterator {

  cql, args, err := q.toCQL()
  if err != nil {
    return &Iterator{err: err}
  }

  cqlQ := session.Query(cql, args...)
  iter := cqlQ.Iter()

  t := &Iterator{
    q:        q,
    iter:     iter,
    cql:      cql,
    cqlQuery: cqlQ,
  }
  return t
}

// First captures the first query result in dst object.
func (q *Query) First(session *gocql.Session, dst interface{}) error {
  iter := q.Run(session)
  if iter.err != nil {
    return iter.err
  }
  iter.Next(dst)
  return iter.Close()
}

// Iterator is the result of running a query.
type Iterator struct {
  session  *gocql.Session
  iter     *gocql.Iter
  cql      string
  cqlQuery *gocql.Query
  err      error
  // limit is the limit on the number of results this iterator should return.
  // A negative value means unlimited.
  limit int32
  // q is the original query which yielded this iterator.
  q *Query
}

// Next returns row of the next result. When there are no more results,
// Done is returned as the error.
func (t *Iterator) Next(dst interface{}) error {
  iter := t.iter
  return LoadEntity(dst, iter)
}

func (t *Iterator) Close() error {
  return t.iter.Close()
}

// Done is returned when a query iteration has completed.
var Done = errors.New("datastore: query has no more results")

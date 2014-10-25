package datastore

import (
  "fmt"
  "reflect"
  "strings"
  "sync"

  "github.com/gocql/gocql"
)

// structTag is the parsed `cql:"name,options"` tag of a struct field.
// if a field has no tag, or the tag has an empty name, then the structTag's
// name is just the field name. A "-" name means that the datastore ignores
// that field.
type structTag struct {
  name string
  opts string
}

// structCodec describes how to convert a struct to and from a sequence of
// column values.
type structCodec struct {
  // byIndex gives the structTag for the i'th field.
  byIndex []structTag
  // byName gives the field codec for the structTag with the given name.
  byName map[string]fieldCodec
}

// fieldCodec is a struct field's index
type fieldCodec struct {
  index int
}

// structCodecs collects the structCodecs that have already been calculated.
var (
  structCodecsMutex sync.Mutex
  structCodecs      = make(map[reflect.Type]*structCodec)
)

func getStructCodec(t reflect.Type) (*structCodec, error) {
  structCodecsMutex.Lock()
  defer structCodecsMutex.Unlock()
  return getStructCodecLocked(t)
}

func getStructCodecLocked(t reflect.Type) (ret *structCodec, err error) {
  c, ok := structCodecs[t]
  if ok {
    return c, nil
  }
  c = &structCodec{
    byIndex: make([]structTag, t.NumField()),
    byName:  make(map[string]fieldCodec),
  }

  structCodecs[t] = c
  defer func() {
    if err != nil {
      delete(structCodecs, t)
    }
  }()

  // iterate over each struct field
  for i := range c.byIndex {

    f := t.Field(i)
    name, opts := f.Tag.Get("cql"), ""

    if i := strings.Index(name, ","); i != -1 {
      // comma found in the tag
      name, opts = name[:i], name[i+1:]
    }

    if name == "" {
      if !f.Anonymous {
        // if no name has been assigned, use the struct field name
        name = f.Name
      }
    } else if name == "-" {
      c.byIndex[i] = structTag{name: name}
    }
    // we have a name by now
    // TODO (sunil): Check if the name is valid or not
    c.byName[name] = fieldCodec{index: i}
    c.byIndex[i] = structTag{
      name: name,
      opts: opts,
    }
  }
  return c, nil
}

// structCLS adapt a struct to be a ColumnLoadSaver.
type structCLS struct {
  v     reflect.Value
  codec *structCodec
}

func (cls *structCLS) Load(iter *gocql.Iter) error {
  rowData, err := iter.RowData()
  if err != nil {
    return err
  }
  for i, col := range rowData.Columns {
    f, ok := cls.codec.byName[col]
    if ok {
      rowData.Values[i] = cls.v.Field(f.index).Addr().Interface()
    }
    // TODO (sunil): Check what to do with slice values
  }
  if iter.Scan(rowData.Values...) {
    return nil
  }
  err = iter.Close()
  if err != nil {
    return err
  }
  // we are here means result exhausted
  return Done
}

func (cls *structCLS) getColumnStr() string {
  cols := make([]string, len(cls.codec.byIndex))
  for i, v := range cls.codec.byIndex {
    cols[i] = v.name
  }
  return strings.Join(cols, ",")
}

func (cls *structCLS) save(kind string, session *gocql.Session) error {

  qqs := make([]string, len(cls.codec.byIndex))
  vals := make([]interface{}, len(cls.codec.byIndex))

  for i, v := range cls.codec.byIndex {
    qqs[i] = "?"
    vals[i] = cls.v.Field(cls.codec.byName[v.name].index).Interface()
  }
  // columnStr := strings.Join(cols, ",")
  qqStr := strings.Join(qqs, ",")
  queryStr := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
    kind, cls.getColumnStr(), qqStr)

  if err := session.Query(queryStr, vals...).Exec(); err != nil {
    return err
  }
  return nil
}

// newStructCLS returns structCLS (column load saver struct).
func newStructCLS(p interface{}) (*structCLS, error) {
  v := reflect.ValueOf(p)
  if v.Kind() != reflect.Ptr || v.IsNil() || v.Elem().Kind() != reflect.Struct {
    return nil, fmt.Errorf("invalid entity type")
  }
  v = v.Elem()
  codec, err := getStructCodec(v.Type())
  if err != nil {
    return nil, err
  }
  return &structCLS{v, codec}, nil
}

// LoadEntity loads the columns from iter to dst, dst must be a struct pointer.
func LoadEntity(dst interface{}, iter *gocql.Iter) error {
  x, err := newStructCLS(dst)
  if err != nil {
    return err
  }
  return x.Load(iter)
}

// SaveEntity saves a given entity instance in datastore, src must be a struct
// pointer of column family kind.
func SaveEntity(session *gocql.Session, kind string, src interface{}) error {
  x, err := newStructCLS(src)
  if err != nil {
    return err
  }
  return x.save(kind, session)
}

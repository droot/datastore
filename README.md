datastore
=========

Package datastore implements a client library for Cassandra datastore. It is
heavily inspired from google client library for appengine datastore. Infact, I
have borrowed a few pieces of code from the same package. It uses gocql package 
for all the data operations under the hood. 

Installation
------------
    go install github.com/droot/datastore

Example
-------
```go
/* Before you execute the program, Launch `cqlsh` and execute:
create keyspace example with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1  };
create table example.tweet(timeline text, id UUID, text text, PRIMARY KEY(id));
create index on example.tweet(timeline);
*/

package main

import (
  "fmt"
  "log"
  "reflect"
  "time"

  "github.com/droot/datastore"
  "github.com/gocql/gocql"
)

type Tweet struct {
  ColumnFamily string     `cql:"tweet"`
  Timeline     string     `cql:"timeline,"`
  Id           gocql.UUID `cql:"id,"`
  TextVal      string     `cql:"text,"`
}

var typeOfTweet = reflect.TypeOf(Tweet{})

func main() {
  // connect to the cluster
  cluster := gocql.NewCluster("10.0.0.10")
  cluster.Keyspace = "example"
  cluster.Consistency = gocql.Quorum
  session, _ := cluster.CreateSession()
  defer session.Close()

  tw := &Tweet{
    Timeline: "me",
    Id:       gocql.TimeUUID(),
    TextVal:  fmt.Sprintf("Auto generated at %s", time.Now()),
  }
  if err := datastore.SaveEntity(session, tw); err != nil {
    log.Fatalln("Error inserting new tweet ::", err)
  }

  var tweet Tweet
  q, err := datastore.NewQuery(typeOfTweet)
  if err != nil {
    log.Fatalln(err)
  }
  q = q.Project("id", "timeline").Filter("id =", tw.Id).Limit(5)
  iter := q.Run(session)
  for err := iter.Next(&tweet); err != datastore.Done; err = iter.Next(&tweet) {
    fmt.Printf("Read a tweet --> %v \n", tweet)
  }

  q1, err := datastore.NewQuery(typeOfTweet)
  if err != nil {
    log.Fatalln(err)
  }
  if err := q1.First(session, &tweet); err != nil {
    log.Fatalln(err)
  }
  fmt.Printf("First tweet --> %s \n", tweet)

  // lets do an update
  qu, err := datastore.NewUpdateQuery(typeOfTweet)
  if err != nil {
    log.Fatalln(err)
  }
  qu = qu.Filter("id =", tw.Id).Update("text", "updated by me :)")
  qStr, err := qu.CQL()
  if err != nil {
    log.Fatalln(err)
  }
  fmt.Println("Query -> ", qStr)
  err = qu.Run(session)
  if err != nil {
    log.Fatalln(err)
  }
}
```

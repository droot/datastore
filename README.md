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
  "time"

  "github.com/droot/datastore"
  "github.com/gocql/gocql"
)

// Tweet defines a struct for operating on tweet columnfamily.
type Tweet struct {
  Timeline string     `cql:"timeline,"`
  Id       gocql.UUID `cql:"id,"`
  TextVal  string     `cql:"text,"`
}

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

  // inserting new row in tweet columnfamily.
  if err := datastore.SaveEntity(session, "tweet", tw); err != nil {
    log.Fatalln("Error inserting new tweet ::", err)
  }

  // Reading multiple rows
  var tweet Tweet

  // create a new query instance. 
  q, err := datastore.NewQuery("tweet", &Tweet{})
  if err != nil {
    log.Fatalln(err)
  }

  // project columns you are interested in. Additionally specify filtering/limiting in the query.
  q = q.Project("id", "timeline").Filter("id =", tw.Id).Limit(5)

  // get hold of an iterator and loop over the tweets.
  iter := q.Run(session)
  for err := iter.Next(&tweet); err != datastore.Done; err = iter.Next(&tweet) {
    fmt.Printf("Read a tweet --> %v \n", tweet)
  }

  // Fetching first row for a query.

  // create a new query
  q1, err := datastore.NewQuery("tweet", &Tweet{})
  if err != nil {
    log.Fatalln(err)
  }

  // First captures the first row from the query
  if err := q1.First(session, &tweet); err != nil {
    log.Fatalln(err)
  }
  fmt.Printf("First tweet --> %s \n", tweet)
}
```

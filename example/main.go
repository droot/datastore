package main

import (
  "fmt"
  "log"
  "time"

  "github.com/droot/datastore"
  "github.com/gocql/gocql"
)

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
  if err := datastore.SaveEntity(session, "tweet", tw); err != nil {
    log.Fatalln("Error inserting new tweet ::", err)
  }

  var tweet Tweet
  q, err := datastore.NewQuery("tweet", &Tweet{})
  if err != nil {
    log.Fatalln(err)
  }
  q = q.Project("id", "timeline").Filter("id =", tw.Id).Limit(5)
  iter := q.Run(session)
  for err := iter.Next(&tweet); err != datastore.Done; err = iter.Next(&tweet) {
    fmt.Printf("Read a tweet --> %v \n", tweet)
  }

  q1, err := datastore.NewQuery("tweet", &Tweet{})
  if err != nil {
    log.Fatalln(err)
  }
  if err := q1.First(session, &tweet); err != nil {
    log.Fatalln(err)
  }
  fmt.Printf("First tweet --> %s \n", tweet)
}
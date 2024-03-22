# TimeZynk/mongo [![Clojars Project](https://img.shields.io/clojars/v/com.timezynk/mongo.svg)](https://clojars.org/com.timezynk/mongo) 

A Clojure wrapper for the [modern MongoDB Java driver](https://mongodb.github.io/mongo-java-driver/4.5/apidocs/mongodb-driver-sync/com/mongodb/client/package-summary.html).

## Releases and Dependency Information

### Leiningen/Boot

    [com.timezynk/mongo "0.9.4"]

### Clojure CLI/deps.edn

    com.timezynk/mongo {:mvn/version "0.9.4"}

### Gradle

    implementation("com.timezynk:mongo:0.9.4")

### Maven

    <dependency>
      <groupId>com.timezynk</groupId>
      <artifactId>mongo</artifactId>
      <version>0.9.4</version>
    </dependency>

## API

[cljdoc.org](https://cljdoc.org/d/com.timezynk/mongo/0.9.4/api/com.timezynk.mongo)

## Testing

You need MongoDB version 4.4 or later installed.

1. Create a database folder:

       mkdir mongodb

1. Start a server:

       mongod --replSet rs0 --dbpath ./mongodb --port 27017

1. In a new terminal window, create the replica set (this is only required once):

       mongo --eval "rs.initiate({ \"_id\": \"rs0\", members: [{ \"_id\": 0, host: \"localhost:27017\" }]}, { force: true })"

1. Start testing.

## Change Log

[CHANGELOG.md](CHANGELOG.md)

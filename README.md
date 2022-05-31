# mongo

A clojure wrapper for the com.mongodb.client Java API. Loosely based on the somnium/congomongo toolkit.

# Usage

The wrapper is intended to be functional, and not introduce state changes to a program environment.

## Configuration functions

Functions that add an environment or special conditions for other functions.

### with-mongo

`(with-mongo <connection> <database> <body>)`

Sets up a connection with a database.

Example:

    (with-mongo "mongodb://localhost:27017" "my-database"
      (insert! :users {:name "My Name"})
      (fetch! :users))

### with-db

`(with-db <database> <body>)`

Switch database.

Example:

    (with-db "my-database-2"
      (insert! :users {:name "My Name"})
      (fetch! :users))

### transaction

`(transaction <body>)`

All database calls in the body are collected and executed atomically at end of scope.

Example:

    (transaction
     (insert! :users {:name "My Name"})
     (fetch! :users))

## Operations

Functions for making database requests.
### fetch

`(fetch <collection>)`  
`(fetch <collection> <filter> <options>)`

# Testing

You need MongoDB version 4.4 or later installed.

1. Create a database folder:

    `mkdir mongodb`

1. Start a server:

    `mongod --replSet rs0 --dbpath ./mongodb --port 27017`

1. In a new terminal window, start the replica set:

    `mongo --eval "rs.initiate({ \"_id\": \"rs0\", members: [{ \"_id\": 0, host: \"localhost:27017\" }]}, { force: true })"`

1. Start testing.

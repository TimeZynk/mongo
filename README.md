# mongo

A clojure wrapper for the com.mongodb.client Java API. Loosely based on the somnium/congomongo toolkit.

# Testing

You need MongoDB version 4.4 or later installed.

1. Create a database folder:

    `mkdir mongodb`

1. Start a server:

    `mongod --replSet rs0 --dbpath ./mongodb --port 27017`

1. In a new terminal window, start the replica set:

    `mongo --eval "rs.initiate({ \"_id\": \"rs0\", members: [{ \"_id\": 0, host: \"localhost:27017\" }]}, { force: true })"`

1. Start testing.

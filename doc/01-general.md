# The Timezynk/mongo wrapper

This is a Clojure wrapper utilizing the modern (as of 2022), synchronous java API driver for MongoDB.

## Installation

The intended way of use is to add the clojar to your `project.clj` file:

```clojure
 :dependencies [[com.timezynk/mongo "0.10.3"]]
```

In order for all API function calls to work properly, you need to have at least MongoDB version 4.4 installed. It is recommended that you have version 5.0 installed because it improves significantly exception messages.

## Connecting to MongoDB

A MongoDB server can be connected to using a connection string or a connection object:

```clojure
(require '[com.timezynk.mongo :as m])

(def connection-string "mongodb://mongodb/my-db")

; Creates a connection here
(def connection-object (m/create-connection! "mongodb://mongodb/my-db-2"
                                             :write-concern :unacknowledged))

; Creates a local connection that is closed when running out of scope
(m/with-mongo connection-string
  (m/create-collection! :new-coll)
  (m/insert! :new-coll {:key "value"}))

(m/with-mongo connection-object
  (m/create-collection! :new-coll-2)
  (m/insert! :new-coll-2 {:key "value"}))

(m/close-connection! connection-object)
```

A call to `create-connection!` sets up a physical connection that can be reused. A call to `with-mongo` will not close it upon completion. A call using a connection string will create a local, physical connection that is then closed when completed.

There are utility functions for reusing a process-local connection:

```clojure
(require '[com.timezynk.mongo.util :as mu])

(mu/set-mongo-uri! "mongodb://mongodb/my-db" :write-concern :w1)

(mu/wrap-mongo
 (m/create-collection! :new-coll)
 (m/insert! :new-coll {:key "value"}))
```

The first call to `wrap-mongo` will create a persistent connection object. Subsequent calls in the same process will reuse the same connection object.

When creating the persistent connection object, the connecion string and options passed by `set-mongo-uri!` will be used first. If no such call has been made, `wrap-mongo` will look for a connection string stored in the `MONGO_URI` environment variable. Failing that, a `MongoClientException` will be thrown.

## Database function calls

Most function calls are variadic, i.e. accept optional parameters. The optional parameters are labelled:

```clojure
(def collation (m/collation "se" :alternate :shifted))
```

In the example above, `"se"` is a required parameter, setting the collation language to Swedish, and `:alternate :shifted` is an optional, labelled parameter, where `:shifted` is an `enum` value, setting this collation to ignore white-space and back-space in ordering and searches.

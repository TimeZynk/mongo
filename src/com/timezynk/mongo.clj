(ns com.timezynk.mongo
  "A functional Clojure wrapper for the modern Java MongoDB API."
  ^{:doc "A wrapper for the com.mongodb.client Java API.
          
          Requires MongoDB version 4.4 or later."}
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-client* *mongo-codecs* *mongo-database* *mongo-session* *mongo-types*]]
   [com.timezynk.mongo.convert :refer [it->clj list->map]]
   [com.timezynk.mongo.guards :refer [catch-return *insert-guard* *replace-guard* *update-guard*]]
   [com.timezynk.mongo.helpers :as h]
   [com.timezynk.mongo.methods.aggregate :refer [aggregate-method]]
   [com.timezynk.mongo.methods.collation :refer [collation-method]]
   [com.timezynk.mongo.methods.connection :refer [connection-method]]
   [com.timezynk.mongo.methods.count :refer [count-method]]
   [com.timezynk.mongo.methods.create-collection :refer [create-collection-method collection-options]]
   [com.timezynk.mongo.methods.create-index :refer [create-index-method]]
   [com.timezynk.mongo.methods.delete :refer [delete-method delete-options delete-one-method]]
   [com.timezynk.mongo.methods.drop-collection :refer [drop-collection-method]]
   [com.timezynk.mongo.methods.drop-index :refer [drop-index-method]]
   [com.timezynk.mongo.methods.fetch :refer [fetch-method]]
   [com.timezynk.mongo.methods.fetch-and-delete :refer [fetch-and-delete-method fetch-and-delete-options]]
   [com.timezynk.mongo.methods.fetch-and-replace :refer [fetch-and-replace-method fetch-and-replace-options]]
   [com.timezynk.mongo.methods.fetch-and-update :refer [fetch-and-update-method fetch-and-update-options]]
   [com.timezynk.mongo.methods.insert :refer [insert-method]]
   [com.timezynk.mongo.methods.list-collections :refer [list-collections-method]]
   [com.timezynk.mongo.methods.list-databases :refer [list-databases-method]]
   [com.timezynk.mongo.methods.modify-collection :refer [modify-collection-method]]
   [com.timezynk.mongo.methods.replace :refer [replace-method replace-options]]
   [com.timezynk.mongo.methods.update :refer [update-method update-one-method update-options]])
  (:import [clojure.lang PersistentArrayMap]
           [com.mongodb MongoClientSettings]
           [com.mongodb.client ClientSession TransactionBody]
           [com.mongodb.client.model Collation]))

; ------------------------
; Connection
; ------------------------

(defn create-connection!
  "Create a connection object.
   
   | Parameter        | Description |
   | ---              | --- |
   | `uri`            | `string` Database location. |
   | `:retry-writes?` | `optional boolean` Sets whether writes should be retried if they fail due to a network error. Default is `false`. |
   | `:write-concern` | `optional enum` Set write concern: |
   |                  | `:acknowledged` Write operations that use this write concern will wait for acknowledgement. Default. |
   |                  | `:majority` Exceptions are raised for network issues, and server errors; waits on a majority of servers for the write operation. |
   |                  | `:unacknowledged` Write operations that use this write concern will return as soon as the message is written to the socket. |
   |                  | `:w1` Write operations that use this write concern will wait for acknowledgement from a single member. |
   |                  | `:w2` Write operations that use this write concern will wait for acknowledgement from two members. |
   |                  | `:w3` Write operations that use this write concern will wait for acknowledgement from three members. |
   
   **Returns**
   
   The connection object.
   
   **Examples**
   
   ```Clojure
   ; Create a connection with default options
   (connection \"mongodb://localhost:27017\")

   ; Create a custom connection
   (connection \"mongodb://localhost:27017\" :retry-writes true :write-concern :w2)
   ```"
  {:arglists '([<uri> & :retry-writes? <boolean> :write-concern [:acknowledged :unacknowledged :journaled :majority :w1 :w2 :w3]])}
  ^MongoClientSettings [^String uri & options]
  {:pre [uri]}
  (connection-method uri options))

(defn close-connection!
  [conn]
  (.close (:client conn)))

(defmacro with-mongo
  "Functionally set up or change mongodb connection. Reverts to earlier settings when leaving scope.
   
   | Parameter    | Description |
   | ---          | --- |
   | `uri`        | `string` Connection string. See the [API documentation](http://mongodb.github.io/mongo-java-driver/4.5/apidocs/mongodb-driver-core/com/mongodb/ConnectionString.html) for more details. |
   | `connection` | `Connection` A connection object. |
   | `body`       | Encapsulated program utilizing the connection. |
   
   **Returns**

   The result of the last encapsulated expression.
   
   **Examples**

   ```Clojure
   (with-mongo \"mongodb://localhost:27017/my-database\"
     (insert! :users {:name \"My Name\"})
     (fetch! :users))
   ```"
  {:arglists '([<uri> & <body>]
               [<connection> & <body>])}
  [conn & body]
  `(let [conn#   ~conn
         client# (if (= (type conn#) String)
                   (connection-method conn# [])
                   conn#)]
     (binding [*mongo-client*   (:client client#)
               *mongo-database* (h/codec-registry (:database client#)
                                                  *mongo-codecs*)]
       (try
         ~@body
         (finally
           (when (= (type conn#) String)
             (.close *mongo-client*)))))))

; ------------------------
; Database
; ------------------------

(defmacro with-database
  "Functionally set up or change database. Reverts to earlier settings when leaving scope.
   
   | Parameter  | Description |
   | ---        | --- |
   | `database` | `string` Name of database to use. |
   | `body`     | Encapsulated program calling the database. |

   **Returns**

   The result of the last encapsulated expression.

   **Examples**
   
   ```Clojure
   (with-database \"my-database-2\"
     (insert! :users {:name \"My Name\"})
     (fetch! :users))
   ```"
  {:arglists '([<database> & <body>])}
  [db & body]
  `(binding [*mongo-database* (h/codec-registry (.getDatabase *mongo-client* ~db)
                                                *mongo-codecs*)]
     ~@body))

(defn list-databases
  "List databases for this connection.
   
   **Returns**
   
   A lazy sequence of database objects."
  []
  (-> (list-databases-method)
      (it->clj)))

(defmacro with-codecs
  "Functionally add or change codecs. Reverts to earlier settings when leaving scope.
   
   | Parameter    | Description |
   | ---          | --- |
   | `codecs`     | `list` A list of codec objects. |
   | `bson-types` | `map` A map of Bson types and their corresponding Java classes. |
   | `body`       | Encapsulated program calling the database. |

   "
  {:arglists '([<codecs> <bson-types> & <body>])}
  [codecs bson-types & body]
  `(let [new-codecs# (concat *mongo-codecs*
                             ~codecs)
         new-types#  (merge *mongo-types*
                            ~bson-types)]
     (binding [*mongo-database* (h/codec-registry *mongo-database* new-codecs#)
               *mongo-codecs*   new-codecs#
               *mongo-types*    new-types#]
       ~@body)))

; ------------------------
; Collation
; ------------------------

(defn collation
  "Create collation.
   
   | Parameter            | Description |
   | ---                  | --- |
   | `locale`             | `string` The two-letter ICU locale string. |
   | `:alternate`         | `optional enum` Should whitespace and punctuation be considered as base characters for purposes of comparison? |
   |                      | `:non-ignorable` Whitespace and punctuation are considered base characters. |
   |                      | `:shifted` Whitespace and punctuation are not considered base characters and are only distinguished at strength levels greater than 3. |
   | `:backwards?`        | `optional boolean` Whether strings with diacritics sort from back of the string, such as with some French dictionary ordering. Default is `false`. |
   | `:case-first`        | `optional enum` Sort order of case differences during tertiary level comparisons. |
   |                      | `:lower` Uppercase sorts before lowercase. |
   |                      | `:upper` Lowercase sorts before uppercase. |
   |                      | `:off` Default value. Similar to `:lower` with slight differences. |
   | `:case-level?`       | `optional boolean` Flag that determines whether to include case comparison at strength level 1 or 2. |
   | `:max-variable`      | `optional enum` Which characters are considered ignorable when `:alternate = :shifted`? Has no effect if `:alternate = :non-ignorable`. |
   |                      | `:punct` Both whitespace and punctuation are ignorable and not considered base characters. |
   |                      | `:space` Whitespace is ignorable and not considered to be base characters. |
   | `:normalization?`    | `optional boolean` Check if text requires normalization and to perform normalization. Default is `false`. |
   | `:numeric-ordering?` | `optional boolean` Compare numeric strings as numbers or as strings. Default is `false`. |
   | `:strength`          | `optional enum` The level of comparison to perform. |
   |                      | `:identical` Limited for specific use case of tie breaker. |
   |                      | `:primary` Collation performs comparisons of the base characters only, ignoring other differences such as diacritics and case. |
   |                      | `:secondary` Collation performs comparisons up to secondary differences, such as diacritics. That is, collation performs comparisons of base characters (primary differences) and diacritics (secondary differences). Differences between base characters takes precedence over secondary differences. |
   |                      | `:tertiary` Collation performs comparisons up to tertiary differences, such as case and letter variants. That is, collation performs comparisons of base characters (primary differences), diacritics (secondary differences), and case and variants (tertiary differences). Differences between base characters takes precedence over secondary differences, which takes precedence over tertiary differences. Default level. |
   |                      | `:quaternary` Limited for specific use case to consider punctuation when levels 1-3 ignore punctuation or for processing Japanese text. |

   For more details, see the [manual page on collation](https://www.mongodb.com/docs/v5.3/reference/collation/).

   **Returns**

   The collation object.

   **Examples**

   ```Clojure
   (collation)
   ```"
  {:arglists '([<locale> & :alternate [:non-ignorable :shifted] :backwards? <boolean> :case-first [:lower :off :upper]
                :case-level? <boolean> :max-variable [:punct :space] :normalization? <boolean> :numeric-ordering? <boolean>
                :strength [:identical :primary :quaternary :secondary :tertiary]])}
  ^Collation [locale & options]
  (collation-method locale options))

; ------------------------
; Collection
; ------------------------

(defn list-collections
  "List full info of all collections in database."
  []
  (-> (list-collections-method)
      (it->clj)))

(defn list-collection-names
  "List keyworded names of all collections in database."
  []
  (->> (list-collections-method)
       (it->clj)
       (map :name)
       (map keyword)))

(defn collection-info
  "List full info of collection.
   
   | Parameter | Description |
   | ---       | --- |
   | `name`    | `keyword/string` Collection name. |"
  [coll]
  (->> (list-collections)
       (filter #(= (name coll) (:name %)))
       (first)))

(defn create-collection!
  "Create collection.
   
   | Parameter     | Description |
   | ---           | --- |
   | `name`        | `keyword/string` Collection name. |
   | `:collation`  | `optional collation object` The collation of the collection. |
   | `:schema`     | `optional map` The schema validation map. |
   | `:validation` | `optional map` Validation logic outside of the schema. |
   | `:level`      | `optional enum` Validaton level: |
   |               | `:strict` Apply validation rules to all inserts and all updates. Default value. |
   |               | `:moderate` Applies validation rules to inserts and to updates on existing valid documents. |
   |               | `:off` No validation for inserts or updates. |
   
   **Returns**
   
   The collection object.
   
   **Examples**
   
   ```Clojure
   ; Collection with exactly one required field `name` of type `string`:
   (create-collection! :users :schema {:name (string)})

   ; Collection where each document can have either a `name` field or an `address` field, but not both:
   (create-collection! :users :validation {:$or [{:name {:$ne nil} :address {:$exists 0}}
                                                 {:name {:$exists 0} :address {:$ne nil}}]})
   ```"
  {:arglists '([<name> & :collation <collation object> :level <integer> :schema {} :validation {}])}
  [coll & options]
  {:pre [coll]}
  (create-collection-method (name coll) (collection-options options)))

(defn modify-collection!
  "Make updates to a collection.
   
   | Parameter     | Description |
   | ---           | --- |
   | `name`        | `keyword/string` Collection name. |
   | `:name`       | `optional keyword/string` New name. |
   | `:collation`  | `optional collation object` The collation of the collection. |
   | `:schema`     | `optional map` The schema validation map. |
   | `:validation` | `optional map` Validation logic outside of the schema. |
   | `:level`      | `optional enum` Validaton level: |
   |               | `:strict` Apply validation rules to all inserts and all updates. Default value. |
   |               | `:moderate` Applies validation rules to inserts and to updates on existing valid documents. |
   |               | `:off` No validation for inserts or updates. |
   | `:validate?`  | `optional boolean` Ensure that existing documents in the collection conform to the new schema or validation. Default `false`. |
   
   **Returns**
   
   The collection object.
   
   **Examples**
   
   ```Clojure
   (modify-collection! :coll :name :coll-2)
   ```"
  {:arglists '([<name> & :collation <collation object> :level <integer> :schema {} :validation {}])}
  [coll & options]
  {:pre [coll]}
  (modify-collection-method coll options))

(defn drop-collection! [coll]
  {:pre [coll]}
  (drop-collection-method (h/get-collection coll)))

; ------------------------
; Index
; ------------------------

(defn list-indexes [coll]
  (-> (.listIndexes (h/get-collection coll) PersistentArrayMap)
      (it->clj)))

(defn create-index!
  "Create an index for a collection.
   
   | Parameter      | Description |
   | ---            | --- |
   | `collection`   | `keyword/string` Collection name. |
   | `keys`         | `map/list(keyword/string)` A document or a list of keywords or strings. |
   | `:collation`   | `optional collation object` Collation of index. |
   | `:background?` | `optional boolean` Create the index in the background. Default `false`. |
   | `:name`        | `optional string` A custom name for the index. |
   | `:filter`      | `optional map` A partial-filter-expression for the index. |
   | `:sparse?`     | `optional boolean` Don't index null values. Default `false`. |
   | `:unique?`     | `optional boolean` Index values must be unique. Default `false`. |
   
   **Returns**
   
   The index name.

   **Examples**
   
   ```Clojure
   ; Index over field-1 in descending order, field-2 as hashed
   (create-index! :coll {:field-1 -1 :field-2 \"hashed\"})

   ; Shorthand for indexing over fields in ascending order
   (create-index! :coll [:field-1 :field-2])

   ; Only flagged documents are indexed and searchable
   (create-index! :coll [:field-1] :filter {:flag-field true})
   ```"
  {:arglists '([<collection> <keys> & :collation <collation object> :background? <boolean> :name <string> :filter {} :sparse? <boolean> :unique? <boolean>])}
  [coll keys & options]
  {:pre [coll keys]}
  (create-index-method (h/get-collection coll)
                       (->bson (list->map keys))
                       options))

(defn drop-index! [coll index]
  {:pre [coll index]}
  (drop-index-method (h/get-collection coll)
                     index))

; ------------------------
; Fetch
; ------------------------

(defn fetch
  "Fetch documents from collection.
   
   | Parameter    | Description |
   | ---          | --- |
   | `collection` | `keyword/string` The collection. |
   | `query`      | `map` A standard MongoDB query. |
   | `:collation` | `optional collation object` Collation used. |
   | `:limit`     | `optional int` Number of documents to fetch. |
   | `:only`      | `optional map/list` A map/list of fields to include or exclude. |
   | `:skip`      | `optional int` Number of documents to skip before fetching. |
   | `:sort`      | `optional map` A MongoDB map of sorting criteria. |

   **Returns**

   A lazy sequence of matching documents.
   
   **Examples**

   ```Clojure
   ; Fetch five documents from collection :users
   (fetch :users {} :limit 5)
   ```"
  {:arglists '([<collection>]
               [<collection> <query> & :collation <collation object> :limit <count> :only {} :skip <count> :sort {}])}
  ([coll]                 (fetch coll {}))
  ([coll query & options] {:pre [coll query]}
                          (-> (fetch-method (h/get-collection coll)
                                            (->bson query)
                                            options)
                              (it->clj))))

(defn fetch-one
  "Return only the first document retrieved.
   
   | Parameter    | Description |
   | ---          | --- |
   | `collection` | `keyword/string` The collection. |
   | `query`      | `map` A standard MongoDB query. |
   | `:collation` | `optional collation object` Collation used. |
   | `:only`      | `optional map` A MongoDB map of fields to include or exclude. |
   | `:skip`      | `optional int` Number of documents to skip before fetching. |
   | `:sort`      | `optional map` A MongoDB map of sorting criteria. |
   
   **Returns**
   
   A single document or `nil`."
  {:arglists '([<collection>]
               [<collection> <query> & :collation <collation object> :only {} :skip <count> :sort {}])}
  ([coll]                 (fetch-one coll {}))
  ([coll query & options] (-> (apply fetch coll query (concat options [:limit 1]))
                              (first))))

(defn fetch-by-id
  "Fetch a single document by its id.
   
   | Parameter    | Description |
   | ---          | --- |
   | `collection` | `keyword/string` The collection. |
   | `id`         | `ObjectId/string` The id. A string will be converted to an ObjectId. |
   | `:only`      | `optional map` A MongoDB map of fields to include or exclude. |
   
   **Returns**
   
   A single document or `nil`."
  {:arglists '([<collection> <id> & :only {}])}
  ([coll id & options] (apply fetch-one coll {:_id id} options)))

(defn fetch-count
  "Count the number of documents returned.
   
   | Parameter    | Description |
   | ---          | --- |
   | `collection` | `keyword/string` The collection. |
   | `query`      | `map` A standard MongoDB query. |
   
   **Returns**

   Number of matching documents."
  {:arglists '([<collection>] [<collection> <query>])}
  ([coll]       (fetch-count coll {}))
  ([coll query] {:pre [coll query]}
                (count-method (h/get-collection coll) query)))

; ------------------------
; Insert
; ------------------------

(defn insert!
  "Insert one document or a list thereof in a collection. Inserting a list is atomic.
   
   | Parameter        | Description |
   | ---              | --- |
   | `collection`     | `keyword/string` The collection. |
   | `document`       | `map` A document. |
   | `document-list`  | `list(map)` A list of documents. |
   | `:write-concern` | `optional enum` Set write concern: |
   |                  | `:acknowledged` Write operations that use this write concern will wait for acknowledgement. Default. |
   |                  | `:majority` Exceptions are raised for network issues, and server errors; waits on a majority of servers for the write operation. |
   |                  | `:unacknowledged` Write operations that use this write concern will return as soon as the message is written to the socket. |
   |                  | `:w1` Write operations that use this write concern will wait for acknowledgement from a single member. |
   |                  | `:w2` Write operations that use this write concern will wait for acknowledgement from two members. |
   |                  | `:w3` Write operations that use this write concern will wait for acknowledgement from three members. |

   **Returns**

   The document/s with `_id` fields, either a single document or a lazy sequence.

   **Examples**

   ```Clojure
   (insert! :users {:name \"Alice\"})

   (insert! :users [{:name \"Alice\"} {:name \"Bob\"}])
   ```"
  {:arglists '([<collection> <document> & :write-concern [:acknowledged :unacknowledged :journaled :majority :w1 :w2 :w3]]
               [<collection> <document-list> & :write-concern [:acknowledged :unacknowledged :journaled :majority :w1 :w2 :w3]])}
  [coll docs & {:keys [write-concern]}]
  {:pre [coll]}
  (catch-return
   (*insert-guard* docs)
   (-> (h/get-collection coll)
       (h/write-concern-options write-concern)
       (insert-method docs))))

(defn insert-one!
  "This is identical to `insert!`, except if payload is nil, return nil instead of throwing exception. Use this function when
   the payload is expected to be a nil-able document."
  [coll doc & options]
  (when doc
    (apply insert! coll doc options)))

; ------------------------
; Update
; ------------------------

(defn update!
  "Update matching documents.
   
   | Parameter        | Description |
   | ---              | --- |
   | `collection`     | `keyword/string` The collection. |
   | `query`          | `map` A standard MongoDB query. |
   | `update`         | `map` A valid update document. |
   | `:upsert?`       | `optional boolean` If no document is found, create a new one. Default is `false`. |
   | `:collation`     | `optional collation object` Collation used. |
   | `:hint`          | `optional map/list` Indexing hint. |
   | `:write-concern` | `optional enum` Set write concern: |
   |                  | `:acknowledged` Write operations that use this write concern will wait for acknowledgement. Default. |
   |                  | `:majority` Exceptions are raised for network issues, and server errors; waits on a majority of servers for the write operation. |
   |                  | `:unacknowledged` Write operations that use this write concern will return as soon as the message is written to the socket. |
   |                  | `:w1` Write operations that use this write concern will wait for acknowledgement from a single member. |
   |                  | `:w2` Write operations that use this write concern will wait for acknowledgement from two members. |
   |                  | `:w3` Write operations that use this write concern will wait for acknowledgement from three members. |

   **Returns**

   ```Clojure
   {:matched-count <number of matching documents>
    :modified-count <number of modified documents>}
   ```
   
   **Examples**

   ```Clojure
   (update!)
   ```"
  {:arglists '([<collection> <query> <update> & :upsert? <boolean> :collation <collation object> :hint {} :write-concern [:acknowledged :unacknowledged :journaled :majority :w1 :w2 :w3]])}
  [coll query update & {:keys [write-concern] :as options}]
  {:pre [coll query]}
  (catch-return
   (*update-guard* update)
   (-> (h/get-collection coll)
       (h/write-concern-options write-concern)
       (update-method (->bson query)
                      (->bson update)
                      (update-options options))
       (h/update-result))))

(defn set!
  "Shorthand for `update!` with a single `:$set` modifier.
   
   **Examples**
  
  ```Clojure
  (set! :coll {} {:a 1})
  ```
  translates to: 
  ```Clojure
  (update! :coll {} {:$set {:a 1}})
  ```"
  [coll query update & options]
  (apply update coll query {:$set update} options))

(defn update-one!
  "Update first matching document.
   
   | Parameter        | Description |
   | ---              | --- |
   | `collection`     | `keyword/string` The collection. |
   | `query`          | `map` A standard MongoDB query. |
   | `update`         | `map/list` A valid update document or pipeline. |
   | `:upsert?`       | `optional boolean` If no document is found, create a new one. Default is `false`. |
   | `:collation`     | `optional collation object` Collation used. |
   | `:hint`          | `optional map/list` Indexing hint. |
   | `:write-concern` | `optional enum` Set write concern: |
   |                  | `:acknowledged` Write operations that use this write concern will wait for acknowledgement. Default. |
   |                  | `:majority` Exceptions are raised for network issues, and server errors; waits on a majority of servers for the write operation. |
   |                  | `:unacknowledged` Write operations that use this write concern will return as soon as the message is written to the socket. |
   |                  | `:w1` Write operations that use this write concern will wait for acknowledgement from a single member. |
   |                  | `:w2` Write operations that use this write concern will wait for acknowledgement from two members. |
   |                  | `:w3` Write operations that use this write concern will wait for acknowledgement from three members. |
   
   **Returns**
   
   ```Clojure
   {:matched-count <0 or 1>
    :modified-count <0 or 1>}
   ```
   
   **Examples**

   ```Clojure
   (update-one!)
   ```"
  {:arglists '([<collection> <query> <update> & :upsert? <boolean> :collation <collation object> :hint {} :write-concern [:acknowledged :unacknowledged :journaled :majority :w1 :w2 :w3]])}
  [coll query update & {:keys [write-concern] :as options}]
  {:pre [coll query]}
  (catch-return
   (*update-guard* update)
   (-> (h/get-collection coll)
       (h/write-concern-options write-concern)
       (update-one-method (->bson query)
                          (->bson update)
                          (update-options options))
       (h/update-result))))

(defn update-by-id! [coll id update & options]
  (apply update-one! coll {:_id id} update options))

(defn set-one!
  "Shorthand for `update-one!` with a single `:$set` modifier.
   
   **Examples**
  
  ```Clojure
  (set-one! :coll {} {:a 1})
  ```
  translates to: 
  ```Clojure
  (update-one! :coll {} {:$set {:a 1}})
  ```"
  [coll query update & options]
  (apply update-one! coll query {:$set update} options))

(defn set-by-id! [coll id update & options]
  (apply set-one! coll {:_id id} update options))

(defn fetch-and-update-one!
  "Update first matching document.
   
   | Parameter      | Description |
   | ---            | --- |
   | `collection`   | `keyword/string` The collection. |
   | `query`        | `map` A standard MongoDB query. |
   | `:return-new?` | `optional boolean` Return the updated document? Default if `false`. |
   | `:upsert?`     | `optional boolean` If no document is found, create a new one. Default is `false`. |
   | `:collation`   | `optional collation object` Collation used. |
   | `:only`        | `optional map` A MongoDB map of fields to include or exclude. |
   | `:hint`        | `optional map` Indexing hint. |
   | `:sort`        | `optional map` A MongoDB map of sorting criteria. |
   
   **Returns**

   A single document or nil."
  {:arglists '([<collection> <query> & :return-new? <boolean> :upsert? <boolean> :collation <collation object> :only {} :hint {} :sort {}])}
  [coll query update & options]
  {:pre [coll query]}
  (catch-return
   (*update-guard* update)
   (-> (fetch-and-update-method (h/get-collection coll)
                                (->bson query)
                                (->bson update)
                                (fetch-and-update-options options)))))

(defn fetch-and-update-by-id! [coll id update & options]
  (apply fetch-and-update-one! coll {:_id id} update options))

(defn fetch-and-set-one!
  "Shorthand for `fetch-and-update-one!` with a single `:$set` modifier.
   
   **Examples**
  
  ```Clojure
  (fetch-and-set-one! :coll {} {:a 1})
  ```
  translates to: 
  ```Clojure
  (fetch-and-update-one! :coll {} {:$set {:a 1}})
  ```"
  [coll query update & options]
  (apply fetch-and-update-one! coll query {:$set update} options))

(defn fetch-and-set-by-id! [coll id update & options]
  (apply fetch-and-set-one! coll {:_id id} update options))

; ------------------------
; Replace
; ------------------------

(defn replace-one!
  "Replace a single document.
   
   | Parameter    | Description |
   | ---          | --- |
   | `collection` | `keyword/string` The collection. |
   | `query`      | `map` A standard MongoDB query. |
   | `document`   | `map` The new document. |
   | `:upsert?`   | `optional boolean` If no document is found, create a new one. Default is `false`. |
   | `:collation` | `optional collation object` Collation used. |
   | `:hint`      | `optional map` Indexing hint. |

   **Returns**

   ```Clojure
   {:matched-count <0 or 1>
    :modified-count <0 or 1>}
   ```"
  {:arglists '([<collection> <query> <document> & :upsert? <boolean> :collation <collation object> :hint {}])}
  [coll query doc & options]
  {:pre [coll query]}
  (catch-return
   (*replace-guard* doc)
   (let [result (replace-method (h/get-collection coll)
                                (->bson query)
                                doc
                                (replace-options options))]
     {:matched-count  (.getMatchedCount result)
      :modified-count (.getModifiedCount result)
      :_id            (when-let [v (.getUpsertedId result)]
                        (.getValue v))
      :acknowledged   (.wasAcknowledged result)})))

(defn replace-one-by-id! [coll id doc & options]
  (apply replace-one! coll {:_id id} doc options))

; TODO: test
(defn fetch-and-replace-one!
  [coll query doc & options]
  {:pre [coll query]}
  (catch-return
   (*replace-guard* doc)
   (-> (fetch-and-replace-method (h/get-collection coll)
                                 (->bson query)
                                 doc
                                 (fetch-and-replace-options options)))))

(defn fetch-and-replace-one-by-id! [coll id doc & options]
  (apply fetch-and-replace-one! coll {:_id id} doc options))

; ------------------------
; Delete
; ------------------------

(defn delete!
  "Delete matching documents.

   | Parameter    | Description |
   | ---          | --- |
   | `collection` | `keyword/string` The collection. |
   | `query`      | `map` A standard MongoDB query. |
   | `:collation` | `optional collation object` Collation used. |
   | `:hint`      | `optional map` Indexing hint. |
   
   **Returns**

   ```Clojure
   {:deleted-count <number of matching documents>}
   ```"
  {:arglists '([<collection> <query> & :collation <collation object> :hint {}])}
  [coll query & options]
  {:pre [coll query]}
  (let [result (delete-method (h/get-collection coll)
                              (->bson query)
                              (delete-options options))]
    {:deleted-count (.getDeletedCount result)
     :acknowledged  (.wasAcknowledged result)}))

(defn delete-one!
  "Delete first matching document.
   
   | Parameter    | Description |
   | ---          | --- |
   | `collection` | `keyword/string` The collection. |
   | `query`      | `map` A standard MongoDB query. |
   | `:collation` | `optional collation object` Collation used. |
   | `:hint`      | `optional map` Indexing hint. |
   
   **Returns**

   ```Clojure
   {:deleted-count <0 or 1>}
   ```"
  {:arglists '([<collection> <query> & :collation <collation object> :hint {}])}
  [coll query & options]
  {:pre [coll query]}
  (let [result (delete-one-method (h/get-collection coll)
                                  (->bson query)
                                  (delete-options options))]
    {:deleted-count (.getDeletedCount result)
     :acknowledged  (.wasAcknowledged result)}))

(defn delete-one-by-id! [coll id & options]
  (apply delete-one! coll {:_id id} options))

(defn fetch-and-delete-one!
  [coll query & options]
  {:pre [query]}
  (-> (fetch-and-delete-method (h/get-collection coll)
                               (->bson query)
                               (fetch-and-delete-options options))))

(defn fetch-and-delete-one-by-id! [coll id & options]
  (apply fetch-and-delete-one! coll {:_id id} options))

; ------------------------
; Transaction
; ------------------------

(defmacro transaction
  "Functionally perform a transaction. Encapsulated database requests are queued and then
   atomically executed when the function goes out of scope.

   **Returns**

   The result of the last encapsulated expression.

   **Examples**
   
   ```Clojure
   (transaction
    (insert! :users {:name \"My Name\"})
    (fetch! :users))
   ```"
  [& body]
  `(binding [*mongo-session* ^ClientSession (.startSession *mongo-client*)]
     (let [txn-body# (reify TransactionBody (execute [_this] ~@body))]
       (try
         (.withTransaction *mongo-session* txn-body#)
         (finally
           (.close *mongo-session*))))))

; ------------------------
; Aggregate
; ------------------------

(defn aggregate
  "MongoDB aggregation.
   
   | Parameter    | Description |
   | ---          | --- |
   | `collection` | `keyword/string` Collection name. |
   | `pipeline`   | The request pipeline queries. |

   **Returns**

   Aggregation result.

   **Examples**
   
   ```Clojure
   (aggregate :users
              {:$match {:age {:$gte 20}}}
              {:$project {:_id 0
                          :name 1}})
   ```"
  {:arglists '([<collection> & <pipeline>])}
  [coll & pipeline]
  {:pre [coll pipeline]}
  (-> (aggregate-method (h/get-collection coll)
                        (map ->bson pipeline))
      (it->clj)))

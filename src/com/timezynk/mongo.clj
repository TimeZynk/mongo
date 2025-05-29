(ns com.timezynk.mongo
  "A functional Clojure wrapper for the modern Java MongoDB API."
  ^{:doc "A wrapper for the com.mongodb.client Java API.
          
          Requires MongoDB version 5.0 or later."}
  (:refer-clojure :exclude [distinct])
  (:require
   [com.timezynk.mongo.assert :refer [assert-keys]]
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-client* *mongo-codecs* *mongo-database* *mongo-session* *mongo-types*]]
   [com.timezynk.mongo.convert :refer [it->clj list->map]]
   [com.timezynk.mongo.guards :refer [catch-return *insert-guard* *replace-guard* *update-guard*]]
   [com.timezynk.mongo.helpers :as h]
   [com.timezynk.mongo.methods.aggregate :refer [aggregate-method]]
   [com.timezynk.mongo.methods.collation :refer [collation-method]]
   [com.timezynk.mongo.methods.connection :refer [get-read-concern get-write-concern connection-method]]
   [com.timezynk.mongo.methods.count :refer [count-method]]
   [com.timezynk.mongo.methods.create-collection :refer [create-collection-method collection-options]]
   [com.timezynk.mongo.methods.create-index :refer [create-index-method]]
   [com.timezynk.mongo.methods.delete :refer [delete-method delete-options delete-one-method delete-result]]
   [com.timezynk.mongo.methods.distinct :refer [distinct-method]]
   [com.timezynk.mongo.methods.drop-collection :refer [drop-collection-method]]
   [com.timezynk.mongo.methods.drop-index :refer [drop-index-method]]
   [com.timezynk.mongo.methods.fetch :refer [fetch-method]]
   [com.timezynk.mongo.methods.fetch-and-delete :refer [fetch-and-delete-method fetch-and-delete-options]]
   [com.timezynk.mongo.methods.fetch-and-replace :refer [fetch-and-replace-method fetch-and-replace-options]]
   [com.timezynk.mongo.methods.fetch-and-update :refer [fetch-and-update-method fetch-and-update-options]]
   [com.timezynk.mongo.methods.insert :refer [insert-method insert-result]]
   [com.timezynk.mongo.methods.list-collections :refer [list-collections-method]]
   [com.timezynk.mongo.methods.list-databases :refer [list-databases-method]]
   [com.timezynk.mongo.methods.modify-collection :refer [modify-collection-method]]
   [com.timezynk.mongo.methods.replace :refer [replace-method replace-options replace-result]]
   [com.timezynk.mongo.methods.run-command :refer [run-command-method]]
   [com.timezynk.mongo.methods.server-status :refer [server-status-method]]
   [com.timezynk.mongo.methods.update :refer [update-method update-one-method update-options update-result]])
  (:import [clojure.lang PersistentArrayMap]
           [com.mongodb MongoClientSettings]
           [com.mongodb.client ClientSession TransactionBody]
           [com.mongodb.client.model Collation]))

; ------------------------
; Connection
; ------------------------

(defmacro create-connection!
  "Create a connection object.
   
   | Parameter        | Description
   | ---              | ---
   | `uri`            | `string` Database location.
   | `:read-concern`  | `optional keyword enum` Set read concern:
   |                  | `:available` The query returns data from the instance with no guarantee that the data has been written to a majority of the replica set members (i.e. may be rolled back).
   |                  | `:default` Sets the default concern, which is usually `local`
   |                  | `:linearizable` The query returns data that reflects all successful majority-acknowledged writes that completed prior to the start of the read operation.
   |                  | `:local` The query returns data from the instance with no guarantee that the data has been written to a majority of the replica set members (i.e. may be rolled back).
   |                  | `:majority` The query returns the data that has been acknowledged by a majority of the replica set members. The documents returned by the read operation are durable, even in the event of failure.
   |                  | `:snapshot` Returns majority-committed data as it appears across shards from a specific single point in time in the recent past.
   |                  | [Read more about read concerns](https://www.mongodb.com/docs/manual/reference/read-concern/).
   | `:retry-reads?`  | `optional boolean` Sets whether reads should be retried if they fail due to a network error.
   | `:retry-writes?` | `optional boolean` Sets whether writes should be retried if they fail due to a network error.
   | `:write-concern` | `optional keyword enum` Set write concern:
   |                  | `:acknowledged` Write operations that use this write concern will wait for acknowledgement. Default.
   |                  | `:journaled` Wait for the server to group commit to the journal file on disk.
   |                  | `:majority` Exceptions are raised for network issues, and server errors; waits on a majority of servers for the write operation. Usually the default option.
   |                  | `:unacknowledged` Return as soon as the message is written to the socket.
   |                  | `:w1` Wait for acknowledgement from a single member.
   |                  | `:w2` Wait for acknowledgement from two members.
   |                  | `:w3` Wait for acknowledgement from three members.
   |                  | [Read more about write concerns](https://www.mongodb.com/docs/manual/reference/write-concern/).

   **Returns**
   
   The connection object.
   
   **Examples**
   
   ```clojure
   ; Create a connection with default options
   (create-connection! \"mongodb://localhost:27017/my-database\")

   ; Create a custom connection
   (create-connection! \"mongodb://localhost:27017/my-database\" :retry-writes? true :write-concern :w2)
   ```"
  {:added "1.0"
   :arglists '([<uri> & :retry-reads? <boolean> :retry-writes? <boolean> :read-concern [:available :default :linearizable :local :majority :snapshot] :write-concern [:acknowledged :journaled :majority :unacknowledged :w1 :w2 :w3]])}
  ^MongoClientSettings [^String uri & {:as options}]
  (assert-keys options #{:retry-reads? :retry-writes? :read-concern :write-concern})
  `(connection-method ~uri ~options))

(defn close-connection!
  [conn]
  (.close (:client conn)))

(defmacro with-mongo
  "Functionally set up or change mongodb connection.
   Reverts to earlier settings when leaving scope.
   
   | Parameter    | Description
   | ---          | ---
   | `uri`        | `string` Connection string. See the [API documentation](http://mongodb.github.io/mongo-java-driver/4.5/apidocs/mongodb-driver-core/com/mongodb/ConnectionString.html) for more details.
   | `connection` | `connection` A connection object.
   | `body`       | Encapsulated program utilizing the connection.
   
   **Returns**

   The result of the last encapsulated expression.

   **Examples**

   ```clojure
   (with-mongo \"mongodb://localhost:27017/my-database\"
     (insert! :users {:name \"My Name\"})
     (fetch! :users))
   ```"
  {:added "1.0"
   :arglists '([<uri> & <body>]
               [<connection> & <body>])}
  [conn & body]
  `(let [client# (if (= (type ~conn) String)
                   (connection-method ~conn [])
                   ~conn)]
     (binding [*mongo-client*   (:client client#)
               *mongo-database* (h/codec-registry (:database client#)
                                                  *mongo-codecs*)]
       (try
         ~@body
         (finally
           (when (= (type ~conn) String)
             (.close *mongo-client*)))))))

; ------------------------
; Database
; ------------------------

(defmacro with-database
  "Functionally set up or change database.
   Reverts to earlier settings when leaving scope.
   
   | Parameter  | Description
   | ---        | ---
   | `database` | `string` Name of database to use.
   | `body`     | Encapsulated program calling the database.

   **Returns**

   The result of the last encapsulated expression.

   **Examples**
   
   ```clojure
   (with-database \"my-database-2\"
     (insert! :users {:name \"My Name\"})
     (fetch! :users))
   ```"
  {:added "1.0"
   :arglists '([<database> & <body>])}
  [db & body]
  `(binding [*mongo-database* (h/codec-registry (.getDatabase *mongo-client*
                                                              (name ~db))
                                                *mongo-codecs*)]
     ~@body))

(defn list-databases
  "List databases for this connection.
   
   **Returns**
   
   A lazy sequence of database objects."
  {:added "1.0"}
  []
  (-> (list-databases-method)
      (it->clj)))

(defn database-name
  "Get the name of the currently active database."
  {:added "1.0"}
  []
  (keyword (.getName *mongo-database*)))

(defmacro with-codecs
  "Add or change codecs.
   Reverts to earlier settings when leaving scope.
   
   | Parameter    | Description
   | ---          | ---
   | `codecs`     | `list` A list of codec objects.
   | `bson-types` | `map` A map of Bson types and their corresponding Java classes.
   | `body`       | Encapsulated program calling the database.

   **Returns**

   **Example**"
  {:added "1.0"
   :arglists '([<codecs> <bson-types> & <body>])}
  [codecs bson-types & body]
  `(let [new-codecs# (concat *mongo-codecs* ~codecs)]
     (binding [*mongo-database* (h/codec-registry *mongo-database* new-codecs#)
               *mongo-codecs*   new-codecs#
               *mongo-types*    (merge *mongo-types* ~bson-types)]
       ~@body)))

(defmacro with-read-concern
  "Set read concern of current active database.
   Reverts to earlier settings when leaving scope.

   | Parameter        | Description
   | ---              | ---
   | `:read-concern`  | `optional keyword enum` Set read concern:
   |                  | `:available` The query returns data from the instance with no guarantee that the data has been written to a majority of the replica set members (i.e. may be rolled back).
   |                  | `:default` Sets the default concern, which is usually `local`
   |                  | `:linearizable` The query returns data that reflects all successful majority-acknowledged writes that completed prior to the start of the read operation.
   |                  | `:local` The query returns data from the instance with no guarantee that the data has been written to a majority of the replica set members (i.e. may be rolled back).
   |                  | `:majority` The query returns the data that has been acknowledged by a majority of the replica set members. The documents returned by the read operation are durable, even in the event of failure.
   |                  | `:snapshot` Returns majority-committed data as it appears across shards from a specific single point in time in the recent past.
   
   [Manual reference](https://www.mongodb.com/docs/manual/reference/read-concern/)"
  {:added "1.0"
   :arglists '([<read-concern> & <body>])}
  [read-concern & body]
  `(binding [*mongo-database* (.withReadConcern *mongo-database* (get-read-concern ~read-concern))]
     ~@body))

(defmacro with-write-concern
  "Set write concern of current active database.
   Reverts to earlier settings when leaving scope.

   | Parameter        | Description
   | ---              | ---
   | `:write-concern` | `optional keyword enum` Set write concern:
   |                  | `:acknowledged` Write operations that use this write concern will wait for acknowledgement. Default.
   |                  | `:journaled` Wait for the server to group commit to the journal file on disk.
   |                  | `:majority` Exceptions are raised for network issues, and server errors; waits on a majority of servers for the write operation. Usually the default option.
   |                  | `:unacknowledged` Return as soon as the message is written to the socket.
   |                  | `:w1` Wait for acknowledgement from a single member.
   |                  | `:w2` Wait for acknowledgement from two members.
   |                  | `:w3` Wait for acknowledgement from three members.
   
   [Manual reference](https://www.mongodb.com/docs/manual/reference/write-concern/)"
  {:added "1.0"
   :arglists '([<write-concern> & <body>])}
  [write-concern & body]
  `(binding [*mongo-database* (.withWriteConcern *mongo-database* (get-write-concern ~write-concern))]
     ~@body))

(defn server-status
  "Fetch information about the server.
   
   | Parameter | Description
   | ---       | ---
   | `options` | `optional keywords` Fields to be included in the result.
   |           | When providing options, fields not included will be excluded from result.
   |           | Not providing any option will yield a full result, including all fields.
   
   Options are not checked for correctness. Valid options may vary depending on the MongoDB version.
   [Manual reference](https://www.mongodb.com/docs/manual/reference/command/serverStatus/).
   
   **Returns**
   
   The status object.
   
   **Examples**
   ```clojure
   (server-status :asserts :queues)

   (server-status :assets) ; Runs fine, but will only return the minimal status information.
   ```"
  {:added "1.0"
   :arglists '([& :asserts :batchedDeletes :bucketCatalog :catalogStats :changeStreamPreImages :collectionCatalog
                :connections :defaultRWConcern :electionMetrics :extra_info :featureCompatibilityVersion :flowControl
                :globalLock :hedgingMetrics :indexBuilds :indexBulkBuilder :indexStats :internalTransactions :locks
                :logicalSessionRecordCache :mem :metrics :mirroredReads :network :opLatencies :opWorkingTime
                :opReadConcernCounters :opWriteConcernCounters :opcounters :opcountersRepl :oplogTruncation
                :planCache :queryAnalyzers :querySettings :queryStats :queues :readConcernCounters
                :readPreferenceCounters :repl :scramCache :security :shardedIndexConsistency :shardingStatistics
                :shardSplits :storageEngine :tcmalloc :tenantMigrations :trafficRecording :transactions :transportSecurity
                :twoPhaseCommitCoordinator :watchdog :wiredTiger])}
  [& options]
  (server-status-method options))

(defn run-command!
  "Run custom commands.
   
   | Parameter   | Description
   | ---         | ---
   | `command`   | `keyword` The command to be executed.
   | `parameter` | The parameter fo the command. Typically just the number 1.
   | `options`   | Key-value pairs of optional parameters specific to the command.
   
   Options are not checked for correctness.
   [Manual reference](https://www.mongodb.com/docs/manual/reference/command/)
   
   **Returns**
   
   Results vary depending on the command.
   
   **Examples**
   
   ```clojure
   (run-command! :collStats \"coll-name\" :scale 1)
   ```"
  {:added "1.0"
   :arglists '([<command> <parameter> & <options>])}
  [cmd val & {:as options}]
  (run-command-method cmd val options))

; ------------------------
; Collation
; ------------------------

(defmacro collation
  "Create collation object.
   
   | Parameter            | Description
   | ---                  | ---
   | `locale`             | `string` The two-letter ICU locale string.
   | `:alternate`         | `optional keyword enum` Should whitespace and punctuation be considered as base characters for purposes of comparison?
   |                      | `:non-ignorable` Whitespace and punctuation are considered base characters.
   |                      | `:shifted` Whitespace and punctuation are not considered base characters and are only distinguished at strength levels greater than 3.
   | `:backwards?`        | `optional boolean` Whether strings with diacritics sort from back of the string, such as with some French dictionary ordering. Default is `false`.
   | `:case-first`        | `optional keyword enum` Sort order of case differences during tertiary level comparisons.
   |                      | `:lower` Uppercase sorts before lowercase.
   |                      | `:upper` Lowercase sorts before uppercase.
   |                      | `:off` Default value. Similar to `:lower` with slight differences.
   | `:case-level?`       | `optional boolean` Flag that determines whether to include case comparison at strength level 1 or 2.
   | `:max-variable`      | `optional keyword enum` Which characters are considered ignorable when `:alternate = :shifted`? Has no effect if `:alternate = :non-ignorable`.
   |                      | `:punct` Both whitespace and punctuation are ignorable and not considered base characters.
   |                      | `:space` Whitespace is ignorable and not considered to be base characters.
   | `:normalization?`    | `optional boolean` Check if text requires normalization and to perform normalization. Default is `false`.
   | `:numeric-ordering?` | `optional boolean` Compare numeric strings as numbers or as strings. Default is `false`.
   | `:strength`          | `optional keyword enum` The level of comparison to perform.
   |                      | `:identical` Limited for specific use case of tie breaker.
   |                      | `:primary` Collation performs comparisons of the base characters only, ignoring other differences such as diacritics and case.
   |                      | `:secondary` Collation performs comparisons up to secondary differences, such as diacritics. That is, collation performs comparisons of base characters (primary differences) and diacritics (secondary differences). Differences between base characters takes precedence over secondary differences.
   |                      | `:tertiary` Collation performs comparisons up to tertiary differences, such as case and letter variants. That is, collation performs comparisons of base characters (primary differences), diacritics (secondary differences), and case and variants (tertiary differences). Differences between base characters takes precedence over secondary differences, which takes precedence over tertiary differences. Default level.
   |                      | `:quaternary` Limited for specific use case to consider punctuation when levels 1-3 ignore punctuation or for processing Japanese text.
   
   [Manual reference](https://www.mongodb.com/docs/v5.3/reference/collation/)

   **Returns**

   The collation object.

   **Examples**

   ```clojure
   (collation)
   ```"
  {:added "1.0"
   :arglists '([<locale> & :alternate [:non-ignorable :shifted] :backwards? <boolean> :case-first [:lower :off :upper]
                :case-level? <boolean> :max-variable [:punct :space] :normalization? <boolean> :numeric-ordering? <boolean>
                :strength [:identical :primary :quaternary :secondary :tertiary]])}
  ^Collation [locale & {:as options}]
  (assert-keys options #{:alternate :backwards? :case-first :case-level? :max-variable
                         :normalization? :numeric-ordering? :strength})
  `(collation-method ~locale ~options))

; ------------------------
; Collection
; ------------------------

(defn list-collections
  "List full info of all collections in database."
  {:added "1.0"}
  []
  (-> (list-collections-method)
      (it->clj)))

(defn list-collection-names
  "List keyworded names of all collections in database.
   
   **Returns**
   
   A lazy sequence of keywords."
  {:added "1.0"}
  []
  (->> (list-collections-method)
       (it->clj)
       (map :name)
       (map keyword)))

(defn collection-info
  "List full info of collection.
   
   | Parameter    | Description
   | ---          | ---
   | `collection` | `keyword/string` Collection name."
  {:added "1.0"
   :arglists '([<collection>])}
  [coll]
  (->> (list-collections)
       (filter #(= (name coll) (:name %)))
       (first)))

(defmacro create-collection!
  "Create collection.
   
   | Parameter       | Description
   | ---             | ---
   | `collection`    | `keyword/string` Collection name.
   | `:collation`    | `optional collation object` The collation of the collection.
   | `:full-change?` | `optional boolean` (>= v6.0) Change streams pass full documents. Default false.
   | `:schema`       | `optional map` The schema validation map.
   | `:validation`   | `optional map` Validation logic outside of the schema.
   | `:level`        | `optional keyword enum` Validaton level:
   |                 | `:strict` Apply validation rules to all inserts and all updates. Default value.
   |                 | `:moderate` Applies validation rules to inserts and to updates on existing valid documents.
   |                 | `:off` No validation for inserts or updates.
   
   **Returns**
   
   The collection object.
   
   **Examples**
   
   ```clojure
   ; Collection with exactly one required field `name` of type `string`:
   (create-collection! :users :schema {:name (string)})

   ; Collection where each document can have either a `name` field or an `address` field, but not both:
   (create-collection! :users :validation {:$or [{:name {:$ne nil} :address {:$exists 0}}
                                                 {:name {:$exists 0} :address {:$ne nil}}]})
   ```"
  {:added "1.0"
   :arglists '([<collection> & :collation <collation object> :full-change? <boolean> :level [:strict :moderate :off] :schema {} :validation {}])}
  [coll & {:as options}]
  (assert-keys options #{:collation :full-change? :level :schema :validation})
  `(create-collection-method (name ~coll) (collection-options ~options)))

(defmacro modify-collection!
  "Make updates to a collection.
   
   | Parameter       | Description
   | ---             | ---
   | `collection`    | `keyword/string` Collection name.
   | `:name`         | `optional keyword/string` New name.
   | `:collation`    | `optional collation object` The collation of the collection.
   | `:full-change?` | `optional boolean` (>= v6.0) Change streams pass full documents. Default false.
   | `:schema`       | `optional map` The schema validation map.
   | `:validation`   | `optional map` Validation logic outside of the schema.
   | `:level`        | `optional keyword enum` Validation level:
   |                 | `:strict` Apply validation rules to all inserts and all updates. Default value.
   |                 | `:moderate` Applies validation rules to inserts and to updates on existing valid documents.
   |                 | `:off` No validation for inserts or updates.
   | `:validate?`    | `optional boolean` Ensure that existing documents in the collection conform to the new schema or validation. Default `false`.
   
   **Returns**
   
   The collection object.
   
   **Examples**
   
   ```clojure
   (modify-collection! :coll :name :coll-2)
   ```"
  {:added "1.0"
   :arglists '([<collection> & :collation <collation object> :full-change? <boolean> :level [:strict :moderate :off] :name <new name> :schema {} :validation {}])}
  [coll & {:as options}]
  (assert-keys options #{:collation :full-change? :level :name :schema :validate? :validation})
  `(modify-collection-method ~coll ~options))

(defn drop-collection!
  {:added "1.0"
   :arglists '([<collection>])}
  [coll]
  {:pre [coll]}
  (drop-collection-method (h/get-collection coll)))

; ------------------------
; Index
; ------------------------

(defn list-indexes
  {:added "1.0"
   :arglists '([<collection>])}
  [coll]
  (-> (.listIndexes (h/get-collection coll) PersistentArrayMap)
      (it->clj)))

(defmacro create-index!
  "Create an index for a collection.
   
   | Parameter      | Description
   | ---            | ---
   | `collection`   | `keyword/string` Collection name.
   | `keys`         | `map/list(keyword/string)` A document or a list of keywords or strings.
   | `:collation`   | `optional collation object` Collation of index.
   | `:background?` | `optional boolean` Create the index in the background. Default `false`.
   | `:filter`      | `optional map` A partial-filter-expression for the index.
   | `:name`        | `optional string` A custom name for the index.
   | `:sparse?`     | `optional boolean` Don't index null values. Default `false`.
   | `:unique?`     | `optional boolean` Index values must be unique. Default `false`.
   
   **Returns**
   
   The index name.

   **Examples**
   
   ```clojure
   ; Index over field-1 in descending order, field-2 as hashed
   (create-index! :coll {:field-1 -1 :field-2 \"hashed\"})

   ; Shorthand for indexing over fields in ascending order
   (create-index! :coll [:field-1 :field-2])

   ; Only flagged documents are indexed and searchable
   (create-index! :coll [:field-1] :filter {:flag-field true})
   ```"
  {:added "1.0"
   :arglists '([<collection> <keys> & :collation <collation object> :background? <boolean> :name <string> :filter {} :sparse? <boolean> :unique? <boolean>])}
  [coll keys & {:as options}]
  (assert-keys options #{:background? :collation :filter :name :sparse? :unique?})
  `(create-index-method (h/get-collection ~coll)
                        (->bson (list->map ~keys))
                        ~options))

(defn drop-index!
  {:added "1.0"
   :arglists '([<collection> <index name>])}
  [coll index]
  (drop-index-method (h/get-collection coll)
                     index))

; ------------------------
; Fetch
; ------------------------

(defmacro fetch
  "Fetch documents from collection.
   
   | Parameter    | Description
   | ---          | ---
   | `collection` | `keyword/string` The collection.
   | `query`      | `map` A standard MongoDB query.
   | `:collation` | `optional collation object` Collation used.
   | `:limit`     | `optional integer` Number of documents to fetch.
   | `:only`      | `optional map/list` A map/list of fields to include or exclude.
   | `:skip`      | `optional integer` Number of documents to skip before fetching.
   | `:sort`      | `optional map` A MongoDB map of sorting criteria.
   
   **Returns**

   A lazy sequence of matching documents.
   
   **Examples**

   ```clojure
   ; Fetch five documents from collection :users
   (fetch :users {} :limit 5)
   ```"
  {:added "1.0"
   :arglists '([<collection>]
               [<collection> <query> & :collation <collation object> :limit <count> :only {} :skip <count> :sort {}])}
  ([coll] `(fetch ~coll {}))
  ([coll query & {:as options}]
   (assert-keys options #{:collation :limit :only :skip :sort})
   `(-> (fetch-method (h/get-collection ~coll)
                      (->bson ~query)
                      ~options)
        (it->clj))))

(defmacro fetch-one
  "Return only the first document retrieved.
   
   | Parameter    | Description
   | ---          | ---
   | `collection` | `keyword/string` The collection.
   | `query`      | `map` A standard MongoDB query.
   | `:collation` | `optional collation object` Collation used.
   | `:only`      | `optional map` A MongoDB map of fields to include or exclude.
   | `:skip`      | `optional integer` Number of documents to skip before fetching.
   | `:sort`      | `optional map` A MongoDB map of sorting criteria.
   
   **Returns**
   
   A single document or `nil`."
  {:added "1.0"
   :arglists '([<collection>]
               [<collection> <query> & :collation <collation object> :only {} :skip <count> :sort {}])}
  ([coll] `(fetch-one ~coll {}))
  ([coll query & {:as options}]
   (assert-keys options #{:collation :only :skip :sort})
   `(-> (fetch-method (h/get-collection ~coll)
                      (->bson ~query)
                      (assoc ~options :limit 1))
        (it->clj)
        (first))))

(defmacro fetch-by-id
  "Fetch a single document by its id.
   
   | Parameter    | Description
   | ---          | ---
   | `collection` | `keyword/string` The collection.
   | `id`         | `ObjectId/string` The id.
   | `:only`      | `optional map` A MongoDB map of fields to include or exclude.
   
   **Returns**
   
   A single document or `nil`."
  {:added "1.0"
   :arglists '([<collection> <id> & :only {}])}
  [coll id & {:as options}]
  (assert-keys options #{:only})
  `(-> (fetch-method (h/get-collection ~coll)
                     (->bson {:_id ~id})
                     (assoc ~options :limit 1))
       (it->clj)
       (first)))

(defn fetch-count
  "Count the number of documents returned.
   
   | Parameter    | Description
   | ---          | ---
   | `collection` | `keyword/string` The collection.
   | `query`      | `map` A standard MongoDB query.
   
   **Returns**

   Number of matching documents."
  {:added "1.0"
   :arglists '([<collection>] [<collection> <query>])}
  ([coll]       (fetch-count coll {}))
  ([coll query] (count-method (h/get-collection coll)
                              (->bson query))))

(defmacro distinct
  "Fetch distinct values from a particular field.
   
   | Parameter    | Description
   | ---          | ---
   | `collection` | `keyword/string` The collection.
   | `field`      | `keyword/string` A field in the collection.
   | `query`      | `map` A standard MongoDB query.
   | `:validate?` | `optional boolean` The field must be part of the collection schema. Default false.
   
   **Returns**

   A lazy sequence of distinct values.
   
   **Examples**

   ```clojure
   (insert! :coll-1 [{:a 1} {:a 1} {:a 2}])
   (distinct :coll-1 :a) ; Returns [1 2]
   (distinct :coll-1 :b) ; Returns []

   (create-collection! :coll-2 :schema {:a (map {:b (integer)})})
   (insert! :coll-2 [{:a {:b 1}} {:a {:b 2}}])
   (distinct :coll-2 :a.b {} :validate? true) ; Returns [1 2]
   (distinct :coll-2 :a.a {} :validate? true) ; Returns IllegalArgumentException
   ```"
  {:added "1.0"
   :arglists '([<collection> <field>]
               [<collection> <field> <query> & :validate? <boolean>])}
  ([coll field] `(distinct ~coll ~field {}))
  ([coll field query & {:as options}]
   (assert-keys options #{:validate?})
   `(-> (distinct-method ~coll
                         (name ~field)
                         (->bson ~query)
                         ~options)
        (it->clj))))

; ------------------------
; Insert
; ------------------------

(defmacro insert!
  "Insert one document or a list thereof in a collection.
   Inserting a list is atomic.
   
   | Parameter       | Description
   | ---             | ---
   | `collection`    | `keyword/string` The collection.
   | `document`      | `map` A document.
   | `document-list` | `list(map)` A list of documents.
   
   **Returns**

   The document/s with `_id` fields, either a single document or a lazy sequence.

   **Examples**

   ```clojure
   (insert! :users {:name \"Alice\"})

   (insert! :users [{:name \"Alice\"} {:name \"Bob\"}])
   ```"
  {:added "1.0"
   :arglists '([<collection> <document>]
               [<collection> <document-list>])}
  [coll docs]
  `(catch-return
     (*insert-guard* ~docs)
     (-> (h/get-collection ~coll)
         (insert-method ~docs)
         (insert-result ~docs))))

(defmacro insert-one!
  "This is identical to `insert!`, except if payload is nil, return nil instead of throwing exception.
   Use this function when the payload is expected to be a nil-able document."
  {:added "1.0"
   :arglists '([<collection> <document>])}
  [coll doc]
  `(when ~doc
     (catch-return
       (*insert-guard* ~doc)
       (-> (h/get-collection ~coll)
           (insert-method ~doc)
           (insert-result ~doc)))))

; ------------------------
; Update
; ------------------------

(defmacro update!
  "Update matching documents.
   
   | Parameter    | Description
   | ---          | ---
   | `collection` | `keyword/string` The collection.
   | `query`      | `map` A standard MongoDB query.
   | `update`     | `map` A valid update document.
   | `:collation` | `optional collation object` Collation used.
   | `:hint`      | `optional map/list` Indexing hint.
   | `:upsert?`   | `optional boolean` If no document is found, create a new one. Default is `false`.
   
   **Returns**

   ```clojure
   {:matched-count <number of matching documents>
    :modified-count <number of modified documents>}
   ```
   
   **Examples**

   ```clojure
   (update!)
   ```"
  {:added "1.0"
   :arglists '([<collection> <query> <update> & :upsert? <boolean> :collation <collation object> :hint {}])}
  [coll query update & {:as options}]
  (assert-keys options #{:collation :hint :upsert?})
  `(catch-return
     (*update-guard* ~update)
     (-> (h/get-collection ~coll)
         (update-method (->bson ~query)
                        (->bson ~update)
                        (update-options ~options))
         (update-result))))

(defmacro set!
  "Shorthand for `update!` with a single `:$set` modifier.
   
   **Examples**
  
  ```clojure
  (set! :coll {} {:a 1})
  ```
  translates to: 
  ```clojure
  (update! :coll {} {:$set {:a 1}})
  ```"
  {:added "1.0"}
  [coll query update & {:as options}]
  (assert-keys options #{:collation :hint :upsert?})
  `(catch-return
     (*update-guard* {:$set ~update})
     (-> (h/get-collection ~coll)
         (update-method (->bson ~query)
                        (->bson {:$set ~update})
                        (update-options ~options))
         (update-result))))

(defmacro update-one!
  "Update first matching document.
   
   | Parameter    | Description
   | ---          | ---
   | `collection` | `keyword/string` The collection.
   | `query`      | `map` A standard MongoDB query.
   | `update`     | `map/list` A valid update document or pipeline.
   | `:collation` | `optional collation object` Collation used.
   | `:hint`      | `optional map/list` Indexing hint.
   | `:upsert?`   | `optional boolean` If no document is found, create a new one. Default is `false`.
   
   **Returns**
   
   ```clojure
   {:matched-count <0 or 1>
    :modified-count <0 or 1>}
   ```
   
   **Examples**

   ```clojure
   (update-one!)
   ```"
  {:added "1.0"
   :arglists '([<collection> <query> <update> & :upsert? <boolean> :collation <collation object> :hint {}])}
  [coll query update & {:as options}]
  (assert-keys options #{:collation :hint :upsert?})
  `(catch-return
     (*update-guard* ~update)
     (-> (h/get-collection ~coll)
         (update-one-method (->bson ~query)
                            (->bson ~update)
                            (update-options ~options))
         (update-result))))

(defmacro update-by-id!
  {:added "1.0"}
  [coll id update & {:as options}]
  (assert-keys options #{:collation :hint :upsert?})
  `(catch-return
     (*update-guard* ~update)
     (-> (h/get-collection ~coll)
         (update-one-method (->bson {:_id ~id})
                            (->bson ~update)
                            (update-options ~options))
         (update-result))))

(defmacro set-one!
  "Shorthand for `update-one!` with a single `:$set` modifier.
   
   **Examples**
  
  ```clojure
  (set-one! :coll {} {:a 1})
  ```
  translates to: 
  ```clojure
  (update-one! :coll {} {:$set {:a 1}})
  ```"
  {:added "1.0"}
  [coll query update & {:as options}]
  (assert-keys options #{:collation :hint :upsert?})
  `(catch-return
     (*update-guard* {:$set ~update})
     (-> (h/get-collection ~coll)
         (update-one-method (->bson ~query)
                            (->bson {:$set ~update})
                            (update-options ~options))
         (update-result))))

(defmacro set-by-id!
  {:added "1.0"}
  [coll id update & {:as options}]
  (assert-keys options #{:collation :hint :upsert?})
  `(catch-return
     (*update-guard* {:$set ~update})
     (-> (h/get-collection ~coll)
         (update-one-method (->bson {:_id ~id})
                            (->bson {:$set ~update})
                            (update-options ~options))
         (update-result))))

(defmacro fetch-and-update-one!
  "Update first matching document.
   
   | Parameter      | Description
   | ---            | ---
   | `collection`   | `keyword/string` The collection.
   | `query`        | `map` A standard MongoDB query.
   | `:return-new?` | `optional boolean` Return the updated document? Default if `false`.
   | `:collation`   | `optional collation object` Collation used.
   | `:only`        | `optional map` A MongoDB map of fields to include or exclude.
   | `:hint`        | `optional map` Indexing hint.
   | `:sort`        | `optional map` A MongoDB map of sorting criteria.
   | `:upsert?`     | `optional boolean` If no document is found, create a new one. Default is `false`.

   **Returns**

   A single document or nil."
  {:added "1.0"
   :arglists '([<collection> <query> & :return-new? <boolean> :upsert? <boolean> :collation <collation object> :only {} :hint {} :sort {}])}
  [coll query update & {:as options}]
  (assert-keys options #{:collation :hint :only :return-new? :sort :upsert?})
  `(catch-return
     (*update-guard* ~update)
     (-> (h/get-collection ~coll)
         (fetch-and-update-method (->bson ~query)
                                  (->bson ~update)
                                  (fetch-and-update-options ~options)))))

(defmacro fetch-and-update-by-id!
  {:added "1.0"}
  [coll id update & {:as options}]
  (assert-keys options #{:collation :hint :only :return-new? :sort :upsert?})
  `(catch-return
     (*update-guard* ~update)
     (-> (h/get-collection ~coll)
         (fetch-and-update-method (->bson {:_id ~id})
                                  (->bson ~update)
                                  (fetch-and-update-options ~options)))))

(defmacro fetch-and-set-one!
  "Shorthand for `fetch-and-update-one!` with a single `:$set` modifier.
   
   **Examples**
  
   ```clojure
   (fetch-and-set-one! :coll {} {:a 1})
   ```
   translates to: 
   ```clojure
   (fetch-and-update-one! :coll {} {:$set {:a 1}})
   ```"
  {:added "1.0"}
  [coll query update & {:as options}]
  (assert-keys options #{:collation :hint :only :return-new? :sort :upsert?})
  `(catch-return
     (*update-guard* {:$set ~update})
     (-> (h/get-collection ~coll)
         (fetch-and-update-method (->bson ~query)
                                  (->bson {:$set ~update})
                                  (fetch-and-update-options ~options)))))

(defmacro fetch-and-set-by-id!
  {:added "1.0"}
  [coll id update & {:as options}]
  (assert-keys options #{:collation :hint :only :return-new? :sort :upsert?})
  `(catch-return
     (*update-guard* {:$set ~update})
     (-> (h/get-collection ~coll)
         (fetch-and-update-method (->bson {:_id ~id})
                                  (->bson {:$set ~update})
                                  (fetch-and-update-options ~options)))))

; ------------------------
; Replace
; ------------------------

(defmacro replace-one!
  "Replace a single document.
   
   | Parameter    | Description
   | ---          | ---
   | `collection` | `keyword/string` The collection.
   | `query`      | `map` A standard MongoDB query.
   | `document`   | `map` The new document.
   | `:collation` | `optional collation object` Collation used.
   | `:hint`      | `optional map` Indexing hint.
   | `:upsert?`   | `optional boolean` If no document is found, create a new one. Default is `false`.

   **Returns**

   ```clojure
   {:matched-count <0 or 1>
    :modified-count <0 or 1>}
   ```"
  {:added "1.0"
   :arglists '([<collection> <query> <document> & :upsert? <boolean> :collation <collation object> :hint {}])}
  [coll query doc & {:as options}]
  (assert-keys options #{:collation :hint :upsert?})
  `(catch-return
     (*replace-guard* ~doc)
     (-> (h/get-collection ~coll)
         (replace-method (->bson ~query)
                         ~doc
                         (replace-options ~options))
         (replace-result))))

(defmacro replace-by-id!
  {:added "1.0"}
  [coll id doc & {:as options}]
  (assert-keys options #{:collation :hint :upsert?})
  `(catch-return
     (*replace-guard* ~doc)
     (-> (h/get-collection ~coll)
         (replace-method (->bson {:_id ~id})
                         ~doc
                         (replace-options ~options))
         (replace-result))))

(defmacro fetch-and-replace-one!
  {:added "1.0"}
  [coll query doc & {:as options}]
  (assert-keys options #{:collation :hint :only :sort :return-new? :upsert?})
  `(catch-return
     (*replace-guard* ~doc)
     (-> (h/get-collection ~coll)
         (fetch-and-replace-method (->bson ~query)
                                   ~doc
                                   (fetch-and-replace-options ~options)))))

(defmacro fetch-and-replace-by-id!
  {:added "1.0"}
  [coll id doc & {:as options}]
  (assert-keys options #{:collation :hint :only :sort :return-new? :upsert?})
  `(catch-return
     (*replace-guard* ~doc)
     (-> (h/get-collection ~coll)
         (fetch-and-replace-method (->bson {:_id ~id})
                                   ~doc
                                   (fetch-and-replace-options ~options)))))

; ------------------------
; Delete
; ------------------------

(defmacro delete!
  "Delete matching documents.

   | Parameter    | Description
   | ---          | ---
   | `collection` | `keyword/string` The collection.
   | `query`      | `map` A standard MongoDB query.
   | `:collation` | `optional collation object` Collation used.
   | `:hint`      | `optional map` Indexing hint.

   **Returns**

   ```clojure
   {:deleted-count <number of matching documents>}
   ```"
  {:added "1.0"
   :arglists '([<collection> <query> & :collation <collation object> :hint {}])}
  [coll query & {:as options}]
  (assert-keys options #{:collation :hint})
  `(-> (h/get-collection ~coll)
       (delete-method (->bson ~query)
                      (delete-options ~options))
       (delete-result)))

(defmacro delete-one!
  "Delete first matching document.
   
   | Parameter    | Description
   | ---          | ---
   | `collection` | `keyword/string` The collection.
   | `query`      | `map` A standard MongoDB query.
   | `:collation` | `optional collation object` Collation used.
   | `:hint`      | `optional map` Indexing hint.

   **Returns**

   ```clojure
   {:deleted-count <0 or 1>}
   ```"
  {:added "1.0"
   :arglists '([<collection> <query> & :collation <collation object> :hint {}])}
  [coll query & {:as options}]
  (assert-keys options #{:collation :hint})
  `(-> (h/get-collection ~coll)
       (delete-one-method (->bson ~query)
                          (delete-options ~options))
       (delete-result)))

(defmacro delete-by-id!
  {:added "1.0"}
  [coll id & {:as options}]
  (assert-keys options #{:collation :hint})
  `(-> (delete-one-method (h/get-collection ~coll)
                          (->bson {:_id ~id})
                          (delete-options ~options))
       (delete-result)))

(defmacro fetch-and-delete-one!
  {:added "1.0"}
  [coll query & {:as options}]
  (assert-keys options #{:collation :hint})
  `(fetch-and-delete-method (h/get-collection ~coll)
                            (->bson ~query)
                            (fetch-and-delete-options ~options)))

(defmacro fetch-and-delete-by-id!
  {:added "1.0"}
  [coll id & {:as options}]
  (assert-keys options #{:collation :hint})
  `(fetch-and-delete-method (h/get-collection ~coll)
                            (->bson {:_id ~id})
                            (fetch-and-delete-options ~options)))

; ------------------------
; Transaction
; ------------------------

(defmacro transaction
  "Functionally perform a transaction.
   Encapsulated database requests are queued and then atomically executed when the function goes out of scope.

   **Returns**

   The result of the last encapsulated expression.

   **Examples**
   
   ```clojure
   (transaction
     (insert! :users {:name \"My Name\"})
     (fetch! :users))
   ```"
  {:added "1.0"
   :arglists '([])}
  [& body]
  `(binding [*mongo-session* ^ClientSession (.startSession *mongo-client*)]
     (try
       (.withTransaction *mongo-session*
                         (reify TransactionBody (execute [_this] ~@body)))
       (finally
         (.close *mongo-session*)))))

; ------------------------
; Aggregate
; ------------------------

(defn aggregate
  "MongoDB aggregation.
   
   | Parameter    | Description
   | ---          | ---
   | `collection` | `keyword/string` Collection name.
   | `pipeline`   | The request pipeline queries.

   **Returns**

   Aggregation result.

   **Examples**
   
   ```clojure
   (aggregate :users
     {:$match {:age {:$gte 20}}}
     {:$project {:_id 0
                 :name 1}})
   ```"
  {:added "1.0"
   :arglists '([<collection> & <pipeline>])}
  [coll & pipeline]
  {:pre [coll pipeline]}
  (-> (aggregate-method (h/get-collection coll)
                        (map ->bson pipeline))
      (it->clj)))

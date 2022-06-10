(ns com.timezynk.mongo
  ^{:doc "A wrapper for the com.mongodb.client Java API.
          
          Requires MongoDB version 4.4 or later."}
  (:require
   [com.timezynk.mongo.config :refer [*mongo-config* *mongo-session*]]
   [com.timezynk.mongo.methods.aggregate :refer [aggregate-method]]
   [com.timezynk.mongo.methods.collation :refer [collation-method]]
   [com.timezynk.mongo.methods.connection :refer [connection-method]]
   [com.timezynk.mongo.methods.count :refer [count-method]]
   [com.timezynk.mongo.methods.create-collection :refer [create-collection-method]]
   [com.timezynk.mongo.methods.create-index :refer [create-index-method]]
   [com.timezynk.mongo.methods.delete :refer [delete-method delete-one-method]]
   [com.timezynk.mongo.methods.drop-collection :refer [drop-collection-method]]
   [com.timezynk.mongo.methods.drop-index :refer [drop-index-method]]
   [com.timezynk.mongo.methods.fetch :refer [fetch-method]]
   [com.timezynk.mongo.methods.fetch-and-delete :refer [fetch-and-delete-method]]
   [com.timezynk.mongo.methods.fetch-and-replace :refer [fetch-and-replace-method]]
   [com.timezynk.mongo.methods.fetch-and-update :refer [fetch-and-update-method]]
   [com.timezynk.mongo.methods.get-collections :refer [get-collections-method]]
   [com.timezynk.mongo.methods.insert :refer [insert-method]]
   [com.timezynk.mongo.methods.replace :refer [replace-method]]
   [com.timezynk.mongo.methods.update :refer [update-method update-one-method]]
   [com.timezynk.mongo.utils.collection :as coll]
   [com.timezynk.mongo.utils.convert :as convert])
  (:import
  ;;  [com.mongodb MongoClientSettings]
   [com.mongodb.client ClientSession MongoClients TransactionBody]
  ;;  [com.mongodb.client.model Collation]
   ))

(defn connection ^MongoClientSettings
  "Create a connection object.
   
   | Parameter        | Description |
   | ---              | --- |
   | `uri`            | `string` Database location. |
   | `:retry-writes`  | `optional boolean` Sets whether writes should be retried if they fail due to a network error. Default is `false`. |
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
  {:arglists '([<uri> & :retry-writes <boolean> :write-concern [:acknowledged :unacknowledged :journaled :majority :w1 :w2 :w3]])}
  [^String uri & options]
  (-> (connection-method uri options)
      (.build)))

(defmacro with-mongo
  "Functionally set up or change mongodb connection. Reverts to earlier settings when leaving scope.
   
   | Parameter    | Description |
   | ---          | --- |
   | `uri`        | `string` Database location. |
   | `connection` | A connection object. |
   | `db`         | `string` Database to use. |
   | `body`       | Encapsulated program utilizing the connection. |
   
   **Returns**

   The result of the last encapsulated expression.
   
   **Examples**

   ```Clojure
   (with-mongo \"mongodb://localhost:27017\" \"my-database\"
     (insert! :users {:name \"My Name\"})
     (fetch! :users))
   ```"
  {:arglists '([<uri> <db> & <body>]
               [<connection> <db> & <body>])}
  [conn ^String db & body]
  `(let [client# (-> (if (= (type ~conn) String)
                       (connection ~conn)
                       ~conn)
                     (MongoClients/create))]
     (binding [*mongo-config* {:client client#
                               :db     (.getDatabase client# ~db)}]
       (try
         ~@body
         (finally
           (.close (:client *mongo-config*)))))))

(defmacro with-db
  "Functionally set up or change database. Reverts to earlier settings when leaving scope.
   
   | Parameter | Description |
   | ---       | --- |
   | `db`      | `string` Name of database to use. |
   | `body`    | Encapsulated program calling the database. |

   **Returns**

   The result of the last encapsulated expression.

   **Examples**
   
   ```Clojure
   (with-db \"my-database-2\"
     (insert! :users {:name \"My Name\"})
     (fetch! :users))
   ```"
  {:arglists '(<db> & <body>)}
  [db & body]
  `(binding [*mongo-config* (assoc *mongo-config*
                                   :db (.getDatabase (:client *mongo-config*)
                                                     ~db))]
     ~@body))

(defn collation ^Collation
  "Create collation.
   
   | Parameter           | Description |
   | ---                 | --- |
   | `:alternate`        | `optional enum` Should consider whitespace and punctuation as base characters for purposes of comparison. |
   |                     | `:non-ignorable`  |
   |                     | `:shifted`  |
   | `:backwards`        | `optional boolean` Whether strings with diacritics sort from back of the string, such as with some French dictionary ordering. |
   | `:case-first`       | `optional enum` Sort order of case differences during tertiary level comparisons. |
   |                     | `:lower`  |
   |                     | `:off`  |
   |                     | `:upper`  |
   | `:case-level`       | `optional boolean` Flag that determines whether to include case comparison at strength level 1 or 2. |
   | `:locale`           | `optional string` ICU locale. |
   | `:max-variable`     | `optional enum` Field that determines up to which characters are considered ignorable when alternate: \"shifted\". Has no effect if alternate: \"non-ignorable\" |
   |                     | `:punct`  |
   |                     | `:space`  |
   | `:normalization`    | `optional boolean` Determines whether to check if text require normalization and to perform normalization. |
   | `:numeric-ordering` | `optional boolean` Compare numeric strings as numbers or as strings. |
   | `:strength`         | `optional enum` The level of comparison to perform. |
   |                     | `:identical`  |
   |                     | `:primary`  |
   |                     | `:secondary`  |
   |                     | `:tertiary`  |
   |                     | `:quaternary`  |

   For more details, see the [manual page on collation](https://www.mongodb.com/docs/v5.3/reference/collation/).

   **Returns**

   The collation object.

   **Examples**

   ```Clojure
   (collation)
   ```"
  {:arglists '([& :alternate [:non-ignorable :shifted] :backwards <boolean> :case-first [:lower :off :upper] :case-level <boolean>
                :locale <string> :max-variable [:punct :space] :normalization <boolean> :numeric-ordering <boolean>
                :strength [:identical :primary :quaternary :secondary :tertiary]])}
  [& options]
  (-> (collation-method options)
      (.build)))

;; (defn set-connection!
;;   "Procedurally set up or change mongodb connection.

;;    uri - string: database location."
;;   [^String uri]
;;   (alter-var-root #'*mongo-config*
;;                   (constantly
;;                    (try
;;                      (conn/close-connection)
;;                      (finally
;;                        (assoc *mongo-config* :client (conn/set-connection uri)))))))

;; (defn set-database!
;;   "Procedurally set up or change database.

;;    db - string: name of database to use."
;;   [db]
;;   (let [db (.getDatabase ^MongoClient (:client *mongo-config*)
;;                          ^String (name db))]
;;     (alter-var-root #'*mongo-config* merge {:db db})))

(defn fetch
  "Fetch documents from collection.
   
   | Parameter    | Description |
   | ---          | --- |
   | `collection` | `keyword/string` The collection. |
   | `query`      | `map` A standard MongoDB query. |
   | `:limit`     | `optional int` Number of documents to fetch. |
   | `:only`      | `optional map` A MongoDB map of fields to include or exclude. |
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
  ([coll] (fetch coll {}))
  ([coll query & options] (-> (fetch-method (coll/get-collection coll)
                                            (convert/clj->doc query)
                                            options)
                              (convert/it->clj))))

(defn fetch-one
  "Return only the first document retrieved.
   
   | Parameter    | Description |
   | ---          | --- |
   | `collection` | `keyword/string` The collection. |
   | `query`      | `map` A standard MongoDB query. |
   
   **Returns**
   
   A single document or `nil`."
  {:arglists '([collection]
               [collection query])}
  ([coll]       (fetch-one coll {}))
  ([coll query] (-> (fetch coll query :limit 1)
                    (first))))

(defn fetch-count
  "Count the number of documents returned.
   
   | Parameter    | Description |
   | ---          | --- |
   | `collection` | `keyword/string` The collection. |
   | `query`      | `map` A standard MongoDB query. |
   
   **Returns**

   Number of matching documents."
  {:arglists '([collection]
               [collection query])}
  ([coll]       (fetch-count coll {}))
  ([coll query] (count-method (coll/get-collection coll)
                              (convert/clj->doc query))))

(defn insert!
  "Insert one document or a list thereof in a collection. Inserting a list is atomic.
   
   | Parameter       | Description |
   | ---             | --- |
   | `collection`    | `keyword/string` The collection. |
   | `document`      | `map` A document. |
   | `document-list` | `list(map)` A list of documents. |
   
   **Returns**

   The document/s with `_id` fields, either a single document or a lazy sequence.

   **Examples**

   ```Clojure
   (insert! :users {:name \"Alice\"})

   (insert! :users [{:name \"Alice\"}
                    {:name \"Bob\"}])
   ```"
  {:arglists '([collection document]
               [collection document-list])}
  [coll doc]
  (let [doc (convert/clj->doc doc)]
    (insert-method (coll/get-collection coll) doc)
    (convert/doc->clj doc)))

(defn update!
  "Update matching documents.
   
   | Parameter    | Description |
   | ---          | --- |
   | `collection` | `keyword/string` The collection. |
   | `query`      | `map` A standard MongoDB query. |
   | `update`     | `map` A valid update document. Must use `$set` or `$push`, throws exception otherwise. |
   | `:upsert?`   | `optional boolean` If no document is found, create a new one. Default is `false`. |

   **Returns**

   ```Clojure
   {:matched-count <number of matching documents>
    :modified-count <number of modified documents>}
   ```
   
   **Examples**

   ```Clojure
   (update!)
   ```"
  {:arglists '([collection query update & :upsert? b])}
  [coll query update & options]
  (let [result (update-method (coll/get-collection coll)
                              (convert/clj->doc query)
                              (convert/clj->doc update)
                              options)]
    {:matched-count  (.getMatchedCount result)
     :modified-count (.getModifiedCount result)}))

(defn update-one!
  "Update first matching document.
   
   | Parameter    | Description |
   | ---          | --- |
   | `collection` | `keyword/string` The collection. |
   | `query`      | `map` A standard MongoDB query. |
   | `update`     | `map` A valid update document. Must use `$set` or `$push`, throws exception otherwise. |
   | `:upsert?`   | `optional boolean` If no document is found, create a new one. Default is `false`. |
   
   **Returns**
   
   ```Clojure
   {:matched-count <0 or 1>
    :modified-count <0 or 1>}
   ```
   
   **Examples**

   ```Clojure
   (update-one!)
   ```"
  {:arglists '([collection query update & :upsert? b])}
  [coll query update & options]
  (let [result (update-one-method (coll/get-collection coll)
                                  (convert/clj->doc query)
                                  (convert/clj->doc update)
                                  options)]
    {:matched-count  (.getMatchedCount result)
     :modified-count (.getModifiedCount result)}))

(defn replace-one!
  "Replace a single document.
   
   | Parameter    | Description |
   | ---          | --- |
   | `collection` | `keyword/string` The collection. |
   | `query`      | `map` A standard MongoDB query. |
   | `document`   | `map` The new document. |
   | `:upsert?`   | `optional boolean` If no document is found, create a new one. Default is `false`. |

   **Returns**

   ```Clojure
   {:matched-count <0 or 1>
    :modified-count <0 or 1>}
   ```"
  {:arglists '([collection query document & :upsert? b])}
  [coll query doc & options]
  (let [result (replace-method (coll/get-collection coll)
                               (convert/clj->doc query)
                               (convert/clj->doc doc)
                               options)]
    {:matched-count  (.getMatchedCount result)
     :modified-count (.getModifiedCount result)}))

(defn delete!
  "Delete matching documents.

   | Parameter    | Description |
   | ---          | --- |
   | `collection` | `keyword/string` The collection. |
   | `query`      | `map` A standard MongoDB query. |
   
   **Returns**

   ```Clojure
   {:deleted-count <number of matching documents>}
   ```"
  {:arglists '([collection query])}
  [coll query & options]
  (let [result (delete-method (coll/get-collection coll)
                              (convert/clj->doc query)
                              options)]
    {:deleted-count (.getDeletedCount result)}))

(defn delete-one!
  "Delete first matching document.
   
   | Parameter    | Description |
   | ---          | --- |
   | `collection` | `keyword/string` The collection. |
   | `query`      | `map` A standard MongoDB query. |
   
   **Returns**

   ```Clojure
   {:deleted-count <0 or 1>}
   ```"
  {:arglists '([collection query])}
  [coll query & options]
  (let [result (delete-one-method (coll/get-collection coll)
                                  (convert/clj->doc query)
                                  options)]
    {:deleted-count (.getDeletedCount result)}))

(defn fetch-and-update!
  "Update first matching document.
   
   | Parameter     | Description |
   | ---           | --- |
   | `collection`  | `keyword/string` The collection. |
   | `query`       | `map` A standard MongoDB query. |
   | `return-new?` | `optional boolean` Return the updated document? Default if `false`. |
   | `:upsert?`    | `optional boolean` If no document is found, create a new one. Default is `false`. |
   
   **Returns**

   A single document or nil."
  {:arglists '([collection query & :return-new? b :upsert? b])}
  [coll query update & options]
  (-> (fetch-and-update-method (coll/get-collection coll)
                               (convert/clj->doc query)
                               (convert/clj->doc update)
                               options)
      (convert/doc->clj)))

; TODO: test
(defn fetch-and-replace! [coll query doc & options]
  (-> (fetch-and-replace-method (coll/get-collection coll)
                                (convert/clj->doc query)
                                (convert/clj->doc doc)
                                options)
      (convert/doc->clj)))

; TODO: test
(defn fetch-and-delete! [coll query]
  (-> (fetch-and-delete-method (coll/get-collection coll)
                               (convert/clj->doc query))
      (convert/doc->clj)))

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
  `(binding [*mongo-session* ^ClientSession (.startSession (:client *mongo-config*))]
     (let [txn-body# (reify TransactionBody
                       (execute [_this]
                         ~@body))]
       (try
         (.withTransaction *mongo-session* txn-body#)
         (finally
           (.close *mongo-session*))))))

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
  {:arglists '([collection & pipeline])}
  [coll & pipeline]
  (-> (aggregate-method (coll/get-collection coll)
                        (convert/clj->doc pipeline))
      (convert/it->clj)))

; TODO: test
(defn list-indexes [coll]
  (-> (.listIndexes (coll/get-collection coll))
      (convert/it->clj)))

(defn create-index!
  "Create an index for a collection.
   
   | Parameter                    | Description |
   | ---                          | --- |
   | `collection`                 | `keyword/string` Collection name. |
   | `keys`                       | `map/list(keyword/string)` A document or a list of keywords or strings. |
   | `:background`                | `optional boolean` Create the index in the background. Default `false`. |
   | `:name`                      | `optional string` A custom name for the index. |
   | `:partial-filter-expression` | `optional map` A filter expression for the index. |
   | `:sparse`                    | `optional boolean` Allow null values. Default `false`. |
   | `:unique`                    | `optional boolean` Index values must be unique. Default `false`. |
   
   **Returns**
   
   The index name.

   **Examples**
   
   ```Clojure
   (create-index!)
   ```"
  {:arglists '([<collection> <keys> & :background <boolean> :name <string> :partial-filter-expression {} :sparse <boolean> :unique <boolean>])}
  [coll keys & options]
  (create-index-method (coll/get-collection coll)
                       (if (map? keys)
                         (convert/clj->doc keys)
                         (convert/list->doc keys))
                       options))

(defn drop-index! [coll index]
  (drop-index-method (coll/get-collection coll)
                     index))

(defn get-collections []
  (-> (:db *mongo-config*)
      (get-collections-method)
      (convert/it->clj)))

(defn create-collection!
  "Create collection.
   
   | Parameter     | Description |
   | ---           | --- |
   | 'name`        | `keyword/string` Collection name. |
   | `:collation`  | `optional collation object` The collation of the collection. |
   | `:schema`     | `optional map` The schema validation map. |
   | `:validation` | `optional map` Validation logic outside of the schema. |
   | `:level`      | `optional enum` Validaton level: |
   |               | `:moderate` Applies validation rules to inserts and to updates on existing valid documents. |
   |               | `:off` No validation for inserts or updates. |
   |               | `:strict` Apply validation rules to all inserts and all updates. Default value. |
   
   **Returns**
   
   The collection object
   
   **Examples**
   
   ```Clojure
   (create-collection!)
   ```"
  {:arglists '([<name> & :collation <collation object> :level <integer> :schema {} :validation {}])}
  [name & options]
  (create-collection-method name options))

(defn drop-collection! [coll]
  (drop-collection-method (coll/get-collection coll)))

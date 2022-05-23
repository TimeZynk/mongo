(ns com.timezynk.mongo
  ^{:doc "A wrapper for the com.mongodb.client Java API.
          
          Requires MongoDB version 4.4 or later."}
  (:require
   [com.timezynk.mongo.config :refer [*mongo-config* *mongo-session*]]
   [com.timezynk.mongo.methods.aggregate :refer [aggregate-method]]
   [com.timezynk.mongo.methods.count :refer [count-method]]
   [com.timezynk.mongo.methods.create-coll :refer [create-coll-method]]
   [com.timezynk.mongo.methods.create-index :refer [create-index-method]]
   [com.timezynk.mongo.methods.delete :refer [delete-method delete-one-method]]
   [com.timezynk.mongo.methods.drop-coll :refer [drop-coll-method]]
   [com.timezynk.mongo.methods.drop-index :refer [drop-index-method]]
   [com.timezynk.mongo.methods.fetch :refer [fetch-method]]
   [com.timezynk.mongo.methods.fetch-and-delete :refer [fetch-and-delete-method]]
   [com.timezynk.mongo.methods.fetch-and-replace :refer [fetch-and-replace-method]]
   [com.timezynk.mongo.methods.fetch-and-update :refer [fetch-and-update-method]]
   [com.timezynk.mongo.methods.get-collections :refer [get-collections-method]]
   [com.timezynk.mongo.methods.insert :refer [insert-method]]
   [com.timezynk.mongo.methods.replace :refer [replace-method]]
   [com.timezynk.mongo.methods.update :refer [update-method update-one-method]]
   [com.timezynk.mongo.utils.coll :as coll]
   [com.timezynk.mongo.utils.convert :as convert])
  (:import
   [com.mongodb ConnectionString MongoClientSettings WriteConcern]
   [com.mongodb.client
    ClientSession MongoClient MongoClients
    MongoDatabase TransactionBody]))

(defn set-connection ^MongoClient [^String uri]
  (-> (MongoClientSettings/builder)
      (.applyConnectionString (ConnectionString. uri))
      (.retryWrites false)
      (.writeConcern WriteConcern/ACKNOWLEDGED)
      (.build)
      (MongoClients/create)))

(defn close-connection []
  (.close (:client *mongo-config*)))

(defmacro with-mongo
  "Functionally set up or change mongodb connection. Reverts to earlier settings when leaving scope.
   uri  - string: database location.
   db   - string: database to use.
   body - encapsulated program utilizing the connection."
  [^String uri ^String db & body]
  `(let [client# (set-connection ~uri)]
     (binding [*mongo-config* {:client client#
                               :db     (.getDatabase client# ~db)}]
       (try
         ~@body
         (finally
           (close-connection))))))

(defmacro with-db
  "Functionally set up or change database. Reverts to earlier settings when leaving scope.
   db   - string: name of database to use.
   body - encapsulated program calling the database."
  [db & body]
  `(binding [*mongo-config* (assoc *mongo-config*
                                   :db (.getDatabase (:client *mongo-config*)
                                                     ~db))]
     ~@body))

(defn set-connection!
  "Procedurally set up or change mongodb connection.
   uri - string: database location."
  [^String uri]
  (alter-var-root #'*mongo-config*
                  (constantly
                   (try
                     (close-connection)
                     (finally
                       (assoc *mongo-config* :client (set-connection uri)))))))

(defn set-database!
  "Procedurally set up or change database.
   db - string: name of database to use."
  [db]
  (let [db (.getDatabase ^MongoClient (:client *mongo-config*)
                         ^String (name db))]
    (alter-var-root #'*mongo-config* merge {:db db})))

(defn- apply-options [result {:keys [limit only skip sort]}]
  (cond-> result
    limit (.limit limit)
    only  (.projection (convert/clj->doc only))
    skip  (.skip skip)
    sort  (.sort (convert/clj->doc sort))))

(defn fetch
  "Fetch documents from collection.
   coll    - keyword/string: The collection.
   query   - map: A standard MongoDB query.
   options - Optional parameters:
     limit - int: Number of documents to fetch.
     only  - map: A MongoDB map of fields to include or exclude.
     skip  - int: Number of documents to skip before fetching.
     sort  - map: A MongoDB map of sorting criteria."
  ([coll]                 (fetch coll {}))
  ([coll query & options] (-> (fetch-method (coll/get-coll coll)
                                            (convert/clj->doc query))
                              (apply-options options)
                              (convert/it->clj))))

(defn fetch-one
  "Return only the first document retrieved.
   coll  - keyword/string: The collection.
   query - map: A standard MongoDB query."
  ([coll]       (fetch-one coll {}))
  ([coll query] (-> (fetch coll query :limit 1)
                    (first))))

(defn fetch-count
  "Count the number of documents returned.
   coll  - keyword/string: The collection.
   query - map: A standard MongoDB query."
  ([coll]       (fetch-count coll {}))
  ([coll query] (count-method (coll/get-coll coll)
                              (convert/clj->doc query))))

(defn insert!
  "Add one document or a list thereof to a collection.
   coll - keyword/string: The collection.
   doc  - map/list(map): A document or a list of documents."
  [coll doc]
  (let [doc (convert/clj->doc doc)]
    (insert-method (coll/get-coll coll) doc)
    (convert/doc->clj doc)))

(defn update!
  "Update matching documents.
   coll    - keyword/string: The collection.
   query   - map: A standard MongoDB query.
   update  - map: A valid update document. Must use $set or $push.
   options - Optional parameters:
     upsert? - boolean: If no document is found, create a new one. Default is don't create."
  [coll query update & options]
  (let [result (update-method (coll/get-coll coll)
                              (convert/clj->doc query)
                              (convert/clj->doc update)
                              options)]
    {:matched-count  (.getMatchedCount result)
     :modified-count (.getModifiedCount result)}))

(defn update-one!
  "Update first matching document.
   coll    - keyword/string: The collection.
   query   - map: A standard MongoDB query.
   update  - map: A valid update document. Must use $set or $push.
   options - Optional parameters:
     upsert? - boolean: If no document is found, create a new one. Default is don't create."
  [coll query update & options]
  (let [result (update-one-method (coll/get-coll coll)
                                  (convert/clj->doc query)
                                  (convert/clj->doc update)
                                  options)]
    {:matched-count  (.getMatchedCount result)
     :modified-count (.getModifiedCount result)}))

(defn replace-one!
  "Replace a single document.
   coll  - keyword/string: The collection.
   query - map: A standard MongoDB query.
   doc   - map: The new document.
   Optional parameters:
     upsert? - boolean: If no document is found, create a new one. Default is don't create."
  [coll query doc & options]
  (let [result (replace-method (coll/get-coll coll)
                               (convert/clj->doc query)
                               (convert/clj->doc doc)
                               options)]
    {:matched-count  (.getMatchedCount result)
     :modified-count (.getModifiedCount result)}))

(defn delete!
  "Delete matching documents.
   coll  - keyword/string: The collection.
   query - map: A standard MongoDB query.
   Optional parameters:
     None yet."
  [coll query & options]
  (let [result (delete-method (coll/get-coll coll)
                              (convert/clj->doc query)
                              options)]
    {:deleted-count (.getDeletedCount result)}))

(defn delete-one!
  "Delete first matching document.
   coll  - keyword/string: The collection.
   query - map: A standard MongoDB query.
   Optional parameters:
     None yet."
  [coll query & options]
  (let [result (delete-one-method (coll/get-coll coll)
                                  (convert/clj->doc query)
                                  options)]
    {:deleted-count (.getDeletedCount result)}))

(defn fetch-and-update! [coll query update & options]
  (-> (fetch-and-update-method (coll/get-coll coll)
                               (convert/clj->doc query)
                               (convert/clj->doc update)
                               options)
      (convert/doc->clj)))

; TODO: test
(defn fetch-and-replace! [coll query doc & options]
  (-> (fetch-and-replace-method (coll/get-coll coll)
                                (convert/clj->doc query)
                                (convert/clj->doc doc)
                                options)
      (convert/doc->clj)))

; TODO: test
(defn fetch-and-delete! [coll query]
  (-> (fetch-and-delete-method (coll/get-coll coll)
                               (convert/clj->doc query))
      (convert/doc->clj)))

(defmacro transaction
  "Functionally perform a transaction. Encapsulated database requests are queued and then
   atomically executed when the function goes out of scope."
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
   coll     - keyword/string: Collection name.
   pipeline - list(map): A list containing the request pipeline documents."
  [coll pipeline]
  (-> (aggregate-method (coll/get-coll coll)
                        (convert/clj->doc pipeline))
      (convert/it->clj)))

; TODO: test
(defn list-indexes [coll]
  (-> (.listIndexes (coll/get-coll coll))
      (convert/it->clj)))

(defn create-index!
  "Create an index for a collection.
   coll - keyword/string: Collection name.
   keys - map/list(keyword/string): A document or a list of keywords or strings.
   Optional parameters:
     background                - boolean: Create the index in the background.
     name                      - string: A custom name for the index.
     partial-filter-expression - map: A filter expression for the index.
     sparse                    - boolean: Allow null values.
     unique                    - boolean: Index values must be unique."
  [coll keys & options]
  (create-index-method (coll/get-coll coll)
                       (if (map? keys)
                         (convert/clj->doc keys)
                         (convert/list->doc keys))
                       options))

(defn drop-index! [coll index]
  (drop-index-method (coll/get-coll coll)
                     index))

(defn get-collections []
  (-> (:db *mongo-config*)
      (get-collections-method)
      (convert/it->clj)))

(defn create-collection! [name]
  (create-coll-method name))

(defn drop-collection! [coll]
  (drop-coll-method (coll/get-coll coll)))

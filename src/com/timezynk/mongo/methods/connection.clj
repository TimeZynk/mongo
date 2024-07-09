(ns ^:no-doc com.timezynk.mongo.methods.connection
  (:import [com.mongodb ConnectionString MongoClientSettings ReadConcern WriteConcern]
           [com.mongodb.client MongoClients]))

(defn get-read-concern [read-concern]
  (case read-concern
    :available    ReadConcern/AVAILABLE
    :default      ReadConcern/DEFAULT
    :linearizable ReadConcern/LINEARIZABLE
    :local        ReadConcern/LOCAL
    :majority     ReadConcern/MAJORITY
    :snapshot     ReadConcern/SNAPSHOT))

(defn get-write-concern [write-concern]
  (case write-concern
    :acknowledged   WriteConcern/ACKNOWLEDGED
    :journaled      WriteConcern/JOURNALED
    :majority       WriteConcern/MAJORITY
    :unacknowledged WriteConcern/UNACKNOWLEDGED
    :w1             WriteConcern/W1
    :w2             WriteConcern/W2
    :w3             WriteConcern/W3))

(defn connection-method [uri {:keys [retry-reads? retry-writes? read-concern write-concern]}]
  (let [conn   (ConnectionString. uri)
        client (-> (cond-> (-> (MongoClientSettings/builder)
                               (.applyConnectionString conn))
                     retry-reads?  (.retryReads retry-reads?)
                     retry-writes? (.retryWrites retry-writes?)
                     read-concern  (.readConcern (get-read-concern read-concern))
                     write-concern (.writeConcern (get-write-concern write-concern)))
                   (.build)
                   (MongoClients/create))]
    {:client   client
     :database (.getDatabase client (.getDatabase conn))}))

(ns ^:no-doc com.timezynk.mongo.methods.connection
  (:import [com.mongodb ConnectionString MongoClientSettings WriteConcern]
           [com.mongodb.client MongoClients]))

(defn connection-method [uri {:keys [retry-writes? write-concern]}]
  (let [conn (ConnectionString. uri)
        client (-> (MongoClientSettings/builder)
                   (.applyConnectionString conn)
                   (.retryWrites           (true? retry-writes?)) ; Must be false for transactions.
                   (.writeConcern          (case (or write-concern :acknowledged)
                                             :acknowledged   (WriteConcern/ACKNOWLEDGED)
                                             :unacknowledged (WriteConcern/UNACKNOWLEDGED)
                                             :journaled      (WriteConcern/JOURNALED)
                                             :majority       (WriteConcern/MAJORITY)
                                             :w1             (WriteConcern/W1)
                                             :w2             (WriteConcern/W2)
                                             :w3             (WriteConcern/W3)))
                   (.build)
                   (MongoClients/create))]
    {:client   client
     :database (.getDatabase client (.getDatabase conn))}))

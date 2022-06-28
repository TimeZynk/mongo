(ns ^:no-doc com.timezynk.mongo.methods.connection
  (:import [com.mongodb ConnectionString MongoClientSettings WriteConcern]))

(defn connection-method [uri {:keys [retry-writes? write-concern]}]
  (-> (MongoClientSettings/builder)
      (.applyConnectionString (ConnectionString. uri))
      (.retryWrites           (true? retry-writes?)) ; Must be false for transactions.
      (.writeConcern          (case (or write-concern :acknowledged)
                                :acknowledged   (WriteConcern/ACKNOWLEDGED)
                                :unacknowledged (WriteConcern/UNACKNOWLEDGED)
                                :journaled      (WriteConcern/JOURNALED)
                                :majority       (WriteConcern/MAJORITY)
                                :w1             (WriteConcern/W1)
                                :w2             (WriteConcern/W2)
                                :w3             (WriteConcern/W3)))
      (.build)))

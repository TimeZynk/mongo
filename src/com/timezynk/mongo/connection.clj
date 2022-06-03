(ns ^:no-doc com.timezynk.mongo.connection
  (:require
   [com.timezynk.mongo.config :refer [*default-connection-options*]])
  (:import
   [com.mongodb ConnectionString MongoClientSettings WriteConcern]
   [com.mongodb.client MongoClientSettings]))

(defn create-connection-settings ^MongoClientSettings [^String uri {:keys [retry-writes write-concern]}]
  (-> (MongoClientSettings/builder)
      (.applyConnectionString (ConnectionString. uri))
      (.retryWrites           (or retry-writes
                                  (:retry-writes *default-connection-options*)))
      (.writeConcern          (case write-concern
                                :acknowledged   (WriteConcern/ACKNOWLEDGED)
                                :unacknowledged (WriteConcern/UNACKNOWLEDGED)
                                :journaled      (WriteConcern/JOURNALED)
                                :majority       (WriteConcern/MAJORITY)
                                :w1             (WriteConcern/W1)
                                :w2             (WriteConcern/W2)
                                :w3             (WriteConcern/W3)
                                (:write-concern *default-connection-options*)))
      (.build)))

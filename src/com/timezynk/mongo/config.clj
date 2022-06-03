(ns ^:no-doc com.timezynk.mongo.config
  (:import
   [com.mongodb WriteConcern]))

(def ^:dynamic *mongo-config* {})
(def ^:dynamic *mongo-session* nil)

(def ^:const *default-connection-options*
  {:retry-writes false ; Default is true, but must be false for transactions
   :write-concern WriteConcern/ACKNOWLEDGED})

(def ^:const *default-query-options* {})

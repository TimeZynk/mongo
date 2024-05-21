(ns com.timezynk.mongo.methods.connection-pool-stats
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-database* *mongo-session*]])
  (:import [clojure.lang PersistentArrayMap]))

(defmulti connection-pool-method
  (fn [] (some? *mongo-session*)))

(defmethod connection-pool-method true []
  (.runCommand *mongo-database*
               *mongo-session*
               (->bson {:connPoolStats 1})
               PersistentArrayMap))

(defmethod connection-pool-method false []
  (.runCommand *mongo-database*
               (->bson {:connPoolStats 1})
               PersistentArrayMap))

(ns ^:no-doc com.timezynk.mongo.methods.list-databases
  (:require
   [com.timezynk.mongo.config :refer [*mongo-client* *mongo-session*]]))

(defmulti list-databases-method
  (fn []
    (some? *mongo-session*)))

(defmethod list-databases-method true []
  (.listDatabases *mongo-client* *mongo-session*))

(defmethod list-databases-method false []
  (.listDatabases *mongo-client*))

(ns ^:no-doc com.timezynk.mongo.methods.list-databases
  (:require
   [com.timezynk.mongo.config :refer [*mongo-client* *mongo-session*]]))

(defmulti list-databases-method
  (fn []
    {:session (some? *mongo-session*)}))

(defmethod list-databases-method {:session true} []
  (.listDatabases *mongo-client* *mongo-session*))

(defmethod list-databases-method {:session false} []
  (.listDatabases *mongo-client*))

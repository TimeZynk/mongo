(ns ^:no-doc com.timezynk.mongo.methods.list-collections
  (:require
   [com.timezynk.mongo.config :refer [*mongo-database* *mongo-session*]])
  (:import [clojure.lang PersistentArrayMap]))

(defmulti list-collections-method
  (fn []
    (some? *mongo-session*)))

(defmethod list-collections-method true []
  (.listCollections *mongo-database* *mongo-session* PersistentArrayMap))

(defmethod list-collections-method false []
  (.listCollections *mongo-database* PersistentArrayMap))

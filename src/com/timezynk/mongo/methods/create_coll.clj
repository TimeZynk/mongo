(ns ^:no-doc com.timezynk.mongo.methods.create-coll
  (:require
   [com.timezynk.mongo.config :refer [*mongo-config* *mongo-session*]]))

(defmulti create-coll-method
  (fn [_name]
    {:session (some? *mongo-session*)}))

(defmethod create-coll-method {:session true} [name]
  (.createCollection (:db *mongo-config*) *mongo-session* name))

(defmethod create-coll-method {:session false} [name]
  (.createCollection (:db *mongo-config*) name))

(ns com.timezynk.mongo.methods.insert
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]]))

(defmulti insert-method
  (fn [_coll doc]
    {:session (some? *mongo-session*)
     :many    (sequential? doc)}))

(defmethod insert-method {:session true :many true} [coll doc]
  (.insertMany coll *mongo-session* doc))

(defmethod insert-method {:session false :many true} [coll doc]
  (.insertMany coll doc))

(defmethod insert-method {:session true :many false} [coll doc]
  (.insertOne coll *mongo-session* doc))

(defmethod insert-method {:session false :many false} [coll doc]
  (.insertOne coll doc))

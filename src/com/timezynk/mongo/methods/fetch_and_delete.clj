(ns com.timezynk.mongo.methods.fetch-and-delete
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]])
  (:import [org.bson Document]))

(defmulti fetch-and-delete-method ^Document
  (fn [_coll _query]
    {:session (some? *mongo-session*)}))

(defmethod fetch-and-delete-method {:session true} [coll query]
  (.findOneAndDelete coll *mongo-session* query))

(defmethod fetch-and-delete-method {:session false} [coll query]
  (.findOneAndDelete coll query))

(ns ^:no-doc com.timezynk.mongo.methods.fetch
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]])
  (:import [org.bson Document]))

(defmulti fetch-method ^Document
  (fn [_coll _query]
    {:session (some? *mongo-session*)}))

(defmethod fetch-method {:session true} [coll query]
  (.find coll *mongo-session* query))

(defmethod fetch-method {:session false} [coll query]
  (.find coll query))

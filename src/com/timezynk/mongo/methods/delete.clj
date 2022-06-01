(ns ^:no-doc com.timezynk.mongo.methods.delete
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]])
  (:import [com.mongodb.client.result UpdateResult]))

(defmulti delete-method ^UpdateResult
  (fn [_coll _query _options]
    {:session (some? *mongo-session*)}))

(defmethod delete-method {:session true} [coll query _options]
  (.deleteMany coll *mongo-session* query))

(defmethod delete-method {:session false} [coll query _options]
  (.deleteMany coll query))

(defmulti delete-one-method ^UpdateResult
  (fn [_coll _query _options]
    {:session (some? *mongo-session*)}))

(defmethod delete-one-method {:session true} [coll query _options]
  (.deleteOne coll *mongo-session* query))

(defmethod delete-one-method {:session false} [coll query _options]
  (.deleteOne coll query))

(ns ^:no-doc com.timezynk.mongo.methods.get-collections
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]]))

(defmulti get-collections-method
  (fn [_db]
    {:session (some? *mongo-session*)}))

(defmethod get-collections-method {:session true} [db]
  (.listCollectionNames db *mongo-session*))

(defmethod get-collections-method {:session false} [db]
  (.listCollectionNames db))

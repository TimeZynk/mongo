(ns ^:no-doc com.timezynk.mongo.methods.list-collections
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]]))

(defmulti list-collections-method
  (fn [_db]
    {:session (some? *mongo-session*)}))

(defmethod list-collections-method {:session true} [db]
  (.listCollections db *mongo-session*))

(defmethod list-collections-method {:session false} [db]
  (.listCollections db))

(defmulti list-collection-names-method
  (fn [_db]
    {:session (some? *mongo-session*)}))

(defmethod list-collection-names-method {:session true} [db]
  (.listCollectionNames db *mongo-session*))

(defmethod list-collection-names-method {:session false} [db]
  (.listCollectionNames db))

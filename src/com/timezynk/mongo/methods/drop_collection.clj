(ns ^:no-doc com.timezynk.mongo.methods.drop-collection
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]]))

(defmulti drop-collection-method
  (fn [_coll]
    {:session (some? *mongo-session*)}))

(defmethod drop-collection-method {:session true} [coll]
  (.drop coll *mongo-session*))

(defmethod drop-collection-method {:session false} [coll]
  (.drop coll))

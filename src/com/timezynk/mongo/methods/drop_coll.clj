(ns com.timezynk.mongo.methods.drop-coll
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]]))

(defmulti drop-coll-method
  (fn [_coll]
    {:session (some? *mongo-session*)}))

(defmethod drop-coll-method {:session true} [coll]
  (.drop coll *mongo-session*))

(defmethod drop-coll-method {:session false} [coll]
  (.drop coll))

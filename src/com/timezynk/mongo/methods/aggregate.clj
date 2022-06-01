(ns ^:no-doc com.timezynk.mongo.methods.aggregate
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]]))

(defmulti aggregate-method
  (fn [_coll _pipeline]
    {:session (some? *mongo-session*)}))

(defmethod aggregate-method {:session true} [coll pipeline]
  (.aggregate coll *mongo-session* pipeline))

(defmethod aggregate-method {:session false} [coll pipeline]
  (.aggregate coll pipeline))

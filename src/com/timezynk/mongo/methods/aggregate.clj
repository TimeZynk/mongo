(ns ^:no-doc com.timezynk.mongo.methods.aggregate
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]]))

(defmulti aggregate-method
  (fn [_coll _pipeline]
    (some? *mongo-session*)))

(defmethod aggregate-method true [coll pipeline]
  (.aggregate coll *mongo-session* pipeline))

(defmethod aggregate-method false [coll pipeline]
  (.aggregate coll pipeline))

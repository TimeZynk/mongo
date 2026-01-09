(ns ^:no-doc com.timezynk.mongo.methods.drop-collection
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]]))

(defmulti drop-collection-method
  (fn [_coll]
    (some? *mongo-session*)))

(defmethod drop-collection-method true [coll]
  (.drop coll *mongo-session*))

(defmethod drop-collection-method false [coll]
  (.drop coll))

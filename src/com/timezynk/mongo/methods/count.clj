(ns ^:no-doc com.timezynk.mongo.methods.count
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-session*]]))

(defmulti count-method
  (fn [_coll _query]
    (some? *mongo-session*)))

(defmethod count-method true [coll query]
  (.countDocuments coll *mongo-session* (->bson query)))

(defmethod count-method false [coll query]
  (.countDocuments coll (->bson query)))

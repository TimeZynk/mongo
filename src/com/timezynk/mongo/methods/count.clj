(ns ^:no-doc com.timezynk.mongo.methods.count
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.helpers :as h]))

(defmulti count-method
  (fn [_coll _query]
    (some? *mongo-session*)))

(defmethod count-method true [coll query]
  (.countDocuments (h/get-collection coll)
                   *mongo-session*
                   (->bson query)))

(defmethod count-method false [coll query]
  (.countDocuments (h/get-collection coll)
                   (->bson query)))

(ns ^:no-doc com.timezynk.mongo.methods.aggregate
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.convert :refer [it->clj]]
   [com.timezynk.mongo.helpers :as h]))

(defmulti aggregate-method
  (fn [_coll _pipeline]
    (some? *mongo-session*)))

(defmethod aggregate-method true [coll pipeline]
  (-> (.aggregate (h/get-collection coll)
                  *mongo-session*
                  (map ->bson pipeline))
      (it->clj)))

(defmethod aggregate-method false [coll pipeline]
  (-> (.aggregate (h/get-collection coll)
                  (map ->bson pipeline))
      (it->clj)))

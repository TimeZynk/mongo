(ns ^:no-doc com.timezynk.mongo.methods.drop-index
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.convert :refer [list->map]]
   [com.timezynk.mongo.helpers :as h])
  (:import [clojure.lang PersistentArrayMap PersistentVector]))

(defprotocol DropIndex
  "Convert index to appropriate format."
  (->index [v]))

(extend-protocol DropIndex
  PersistentVector
  (->index [v]
    (->bson (list->map v)))

  PersistentArrayMap
  (->index [v]
    (->bson v))

  String
  (->index [v] v))

(defmulti drop-index-method
  (fn [_coll _index]
    (some? *mongo-session*)))

(defmethod drop-index-method true [coll index]
  (.dropIndex (h/get-collection coll)
              *mongo-session*
              (->index index)))

(defmethod drop-index-method false [coll index]
  (.dropIndex (h/get-collection coll)
              (->index index)))

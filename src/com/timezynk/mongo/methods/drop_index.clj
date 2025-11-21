(ns ^:no-doc com.timezynk.mongo.methods.drop-index
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.convert :refer [list->map]]
   [com.timezynk.mongo.helpers :as h]))

(defmulti drop-index-method
  (fn [_coll index]
    {:session (some? *mongo-session*)
     :coll    (coll? index)}))

(defmethod drop-index-method {:session true :coll true} [coll index]
  (.dropIndex (h/get-collection coll)
              *mongo-session*
              (->bson (list->map index))))

(defmethod drop-index-method {:session true :coll false} [coll index]
  (.dropIndex (h/get-collection coll)
              *mongo-session*
              index))

(defmethod drop-index-method {:session false :coll true} [coll index]
  (.dropIndex (h/get-collection coll)
              (->bson (list->map index))))

(defmethod drop-index-method {:session false :coll false} [coll index]
  (.dropIndex (h/get-collection coll)
              index))

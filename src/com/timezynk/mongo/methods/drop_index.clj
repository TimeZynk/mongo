(ns ^:no-doc com.timezynk.mongo.methods.drop-index
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-session*]]))

(defmulti drop-index-method
  (fn [_coll index]
    {:session (some? *mongo-session*)
     :map     (map? index)}))

(defmethod drop-index-method {:session true :map true} [coll index]
  (.dropIndex coll *mongo-session* (->bson index)))

(defmethod drop-index-method {:session true :map false} [coll index]
  (.dropIndex coll *mongo-session* index))

(defmethod drop-index-method {:session false :map true} [coll index]
  (.dropIndex coll (->bson index)))

(defmethod drop-index-method {:session false :map false} [coll index]
  (.dropIndex coll index))

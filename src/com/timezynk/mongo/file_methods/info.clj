(ns ^:no-doc com.timezynk.mongo.file-methods.info
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.convert :refer [list->map]])
  (:import [com.mongodb.client.gridfs GridFSBucket]
           [org.bson.conversions Bson]))

(defn- with-options [result {:keys [collation limit skip sort]}]
  (cond-> result
    collation (.collation collation)
    limit     (.limit limit)
    skip      (.skip skip)
    sort      (.sort (->bson (list->map sort)))))

(defmulti info-method
  (fn [^GridFSBucket _bucket ^Bson _query options]
    {:session (some? *mongo-session*)
     :options (coll? options)}))

(defmethod info-method {:session true :options true} [bucket query options]
  (-> (.find bucket *mongo-session* query)
      (with-options options)))

(defmethod info-method {:session true :options false} [bucket query _options]
  (.find bucket *mongo-session* query))

(defmethod info-method {:session false :options true} [bucket query options]
  (-> (.find bucket query)
      (with-options options)))

(defmethod info-method {:session false :options false} [bucket query _options]
  (.find bucket query))

(ns ^:no-doc com.timezynk.mongo.methods.fetch-and-update
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.convert :refer [list->map]]
   [com.timezynk.mongo.padding :refer [*update-padding*]])
  (:import [org.bson Document]
           [com.mongodb.client.model FindOneAndUpdateOptions ReturnDocument]))

(defn get-options [obj {:keys [return-new? upsert? collation only hint sort]}]
  (cond-> obj
    return-new? (.returnDocument ReturnDocument/AFTER)
    upsert?     (.upsert true)
    collation   (.collation collation)
    only        (.projection (->bson (list->map only)))
    hint        (.hint (->bson (list->map hint)))
    sort        (.sort (->bson (list->map sort)))))

(defn- fetch-and-update-options ^FindOneAndUpdateOptions [options]
  (get-options (FindOneAndUpdateOptions.) options))

(defmulti fetch-and-update-method ^Document
  (fn [_coll _query _update _options]
    (some? *mongo-session*)))

(defmethod fetch-and-update-method true [coll query update options]
  (.findOneAndUpdate coll
                     *mongo-session*
                     (->bson query)
                     (->bson (*update-padding* update))
                     (fetch-and-update-options options)))

(defmethod fetch-and-update-method false [coll query update options]
  (.findOneAndUpdate coll
                     (->bson query)
                     (->bson (*update-padding* update))
                     (fetch-and-update-options options)))

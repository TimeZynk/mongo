(ns ^:no-doc com.timezynk.mongo.methods.fetch-and-replace
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.convert :refer [list->map]])
  (:import [org.bson Document]
           [com.mongodb.client.model FindOneAndReplaceOptions ReturnDocument]))

(defn fetch-and-replace-options ^FindOneAndReplaceOptions [{:keys [return-new? upsert? collation only hint sort]}]
  (cond-> (FindOneAndReplaceOptions.)
    return-new? (.returnDocument ReturnDocument/AFTER)
    upsert?     (.upsert true)
    collation   (.collation collation)
    only        (.projection (->bson (list->map only)))
    hint        (.hint (->bson (list->map hint)))
    sort        (.sort (->bson (list->map sort)))))

(defmulti fetch-and-replace-method ^Document
  (fn [_coll _query _doc _options]
    (some? *mongo-session*)))

(defmethod fetch-and-replace-method true [coll query doc options]
  (.findOneAndReplace coll *mongo-session* query doc options))

(defmethod fetch-and-replace-method false [coll query doc options]
  (.findOneAndReplace coll query doc options))

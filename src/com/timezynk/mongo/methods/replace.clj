(ns ^:no-doc com.timezynk.mongo.methods.replace
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.convert :refer [list->map]])
  (:import [com.mongodb.client.model ReplaceOptions]
           [com.mongodb.client.result UpdateResult]))

(defn replace-options ^ReplaceOptions [{:keys [upsert? collation hint]}]
  (cond-> (ReplaceOptions.)
    upsert?   (.upsert true)
    collation (.collation collation)
    hint      (.hint (->bson (list->map hint)))))

(defmulti replace-method ^UpdateResult
  (fn [_coll _query _doc _options]
    (some? *mongo-session*)))

(defmethod replace-method true [coll query doc options]
  (.replaceOne coll *mongo-session* query doc options))

(defmethod replace-method false [coll query doc options]
  (.replaceOne coll query doc options))

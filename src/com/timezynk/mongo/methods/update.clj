(ns ^:no-doc com.timezynk.mongo.methods.update
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.convert :refer [list->map]])
  (:import [com.mongodb.client.model UpdateOptions]
           [com.mongodb.client.result UpdateResult]))

(defn update-options ^UpdateOptions [{:keys [upsert? collation hint]}]
  (cond-> (UpdateOptions.)
    upsert?   (.upsert true)
    collation (.collation collation)
    hint      (.hint (->bson (list->map hint)))))

(defmulti update-method ^UpdateResult
  (fn [_coll _query _update _options]
    (some? *mongo-session*)))

(defmethod update-method true [coll query update options]
  (.updateMany coll *mongo-session* query update options))

(defmethod update-method false [coll query update options]
  (.updateMany coll query update options))

(defmulti update-one-method ^UpdateResult
  (fn [_coll _query _update _options]
    (some? *mongo-session*)))

(defmethod update-one-method true [coll query update options]
  (.updateOne coll *mongo-session* query update options))

(defmethod update-one-method false [coll query update options]
  (.updateOne coll query update options))

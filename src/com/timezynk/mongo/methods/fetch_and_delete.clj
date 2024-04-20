(ns ^:no-doc com.timezynk.mongo.methods.fetch-and-delete
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.convert :refer [list->map]])
  (:import [org.bson Document]
           [com.mongodb.client.model FindOneAndDeleteOptions]))

(defn fetch-and-delete-options ^FindOneAndDeleteOptions [{:keys [collation only hint sort]}]
  (cond-> (FindOneAndDeleteOptions.)
    collation (.collation collation)
    only      (.projection (->bson (list->map only)))
    hint      (.hint (->bson (list->map hint)))
    sort      (.sort (->bson (list->map sort)))))

(defmulti fetch-and-delete-method ^Document
  (fn [_coll _query _options]
    (some? *mongo-session*)))

(defmethod fetch-and-delete-method true [coll query options]
  (.findOneAndDelete coll *mongo-session* query options))

(defmethod fetch-and-delete-method false [coll query options]
  (.findOneAndDelete coll query options))

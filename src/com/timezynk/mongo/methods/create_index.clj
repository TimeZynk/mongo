(ns ^:no-doc com.timezynk.mongo.methods.create-index
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-session*]])
  (:import [com.mongodb.client.model IndexOptions]))

(defn- create-options ^IndexOptions
  [{:keys [collation background? name filter sparse? unique?]}]
  (cond-> (IndexOptions.)
    collation   (.collation collation)
    background? (.background background?)
    name        (.name name)
    filter      (.partialFilterExpression (->bson filter))
    sparse?     (.sparse sparse?)
    unique?     (.unique unique?)))

(defmulti create-index-method
  (fn [_coll _keys _options]
    (some? *mongo-session*)))

(defmethod create-index-method true [coll keys options]
  (.createIndex coll *mongo-session* keys (create-options options)))

(defmethod create-index-method false [coll keys options]
  (.createIndex coll keys (create-options options)))

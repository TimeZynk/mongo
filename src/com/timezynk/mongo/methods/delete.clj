(ns ^:no-doc com.timezynk.mongo.methods.delete
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.convert :refer [list->map]])
  (:import [com.mongodb.client.model DeleteOptions]
           [com.mongodb.client.result UpdateResult]))

(defn delete-options ^DeleteOptions [{:keys [collation hint]}]
  (cond-> (DeleteOptions.)
    collation (.collation collation)
    hint      (.hint (->bson (list->map hint)))))

(defmulti delete-method ^UpdateResult
  (fn [_coll _query _options]
    {:session (some? *mongo-session*)}))

(defmethod delete-method {:session true} [coll query options]
  (.deleteMany coll *mongo-session* query options))

(defmethod delete-method {:session false} [coll query options]
  (.deleteMany coll query options))

(defmulti delete-one-method ^UpdateResult
  (fn [_coll _query _options]
    {:session (some? *mongo-session*)}))

(defmethod delete-one-method {:session true} [coll query options]
  (.deleteOne coll *mongo-session* query options))

(defmethod delete-one-method {:session false} [coll query options]
  (.deleteOne coll query options))

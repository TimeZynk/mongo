(ns ^:no-doc com.timezynk.mongo.methods.update
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]])
  (:import [com.mongodb.client.model UpdateOptions]
           [com.mongodb.client.result UpdateResult]))

(defn- create-options ^UpdateOptions [{:keys [upsert?]}]
  (-> (UpdateOptions.)
      (.upsert (some? upsert?))))

(defmulti update-method ^UpdateResult
  (fn [_coll _query _update _options]
    {:session (some? *mongo-session*)}))

(defmethod update-method {:session true} [coll query update options]
  (.updateMany coll *mongo-session* query update (create-options options)))

(defmethod update-method {:session false} [coll query update options]
  (.updateMany coll query update (create-options options)))

(defmulti update-one-method ^UpdateResult
  (fn [_coll _query _update _options]
    {:session (some? *mongo-session*)}))

(defmethod update-one-method {:session true} [coll query update options]
  (.updateOne coll *mongo-session* query update (create-options options)))

(defmethod update-one-method {:session false} [coll query update options]
  (.updateOne coll query update (create-options options)))

(ns ^:no-doc com.timezynk.mongo.methods.fetch-and-update
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]])
  (:import [org.bson Document]
           [com.mongodb.client.model FindOneAndUpdateOptions ReturnDocument]))

(defn fetch-and-update-options ^FindOneAndUpdateOptions [{:keys [return-new? upsert?]}]
  (cond-> (FindOneAndUpdateOptions.)
    return-new?
    (.returnDocument ReturnDocument/AFTER)
    upsert?
    (.upsert true)))

(defmulti fetch-and-update-method ^Document
  (fn [_coll _query _update _options]
    {:session (some? *mongo-session*)}))

(defmethod fetch-and-update-method {:session true} [coll query update options]
  (.findOneAndUpdate coll *mongo-session* query update options))

(defmethod fetch-and-update-method {:session false} [coll query update options]
  (.findOneAndUpdate coll query update options))

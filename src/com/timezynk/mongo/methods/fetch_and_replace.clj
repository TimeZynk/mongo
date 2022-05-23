(ns com.timezynk.mongo.methods.fetch-and-replace
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]])
  (:import [org.bson Document]
           [com.mongodb.client.model FindOneAndReplaceOptions ReturnDocument]))

(defn- create-options ^FindOneAndReplaceOptions [{:keys [return-new? upsert?]}]
  (cond-> (FindOneAndReplaceOptions.)
    return-new?
    (.returnDocument ReturnDocument/AFTER)
    upsert?
    (.upsert true)))

(defmulti fetch-and-replace-method ^Document
  (fn [_coll _query _doc _options]
    {:session (some? *mongo-session*)}))

(defmethod fetch-and-replace-method {:session true} [coll query doc options]
  (.findOneAndReplace coll *mongo-session* query doc (create-options options)))

(defmethod fetch-and-replace-method {:session false} [coll query doc options]
  (.findOneAndReplace coll query doc (create-options options)))

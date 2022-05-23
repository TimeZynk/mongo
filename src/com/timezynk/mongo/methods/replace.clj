(ns com.timezynk.mongo.methods.replace
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]])
  (:import [com.mongodb.client.model ReplaceOptions]
           [com.mongodb.client.result UpdateResult]))

(defn- create-options ^ReplaceOptions [{:keys [upsert?]}]
  (-> (ReplaceOptions.)
      (.upsert (some? upsert?))))

(defmulti replace-method ^UpdateResult
  (fn [_coll _query _doc _options]
    {:session (some? *mongo-session*)}))

(defmethod replace-method {:session true} [coll query doc options]
  (.replaceOne coll *mongo-session* query doc (create-options options)))

(defmethod replace-method {:session false} [coll query doc options]
  (.replaceOne coll query doc (create-options options)))

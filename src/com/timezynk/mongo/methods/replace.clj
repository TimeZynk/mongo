(ns ^:no-doc com.timezynk.mongo.methods.replace
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.convert-types :refer [clj->doc list->doc]])
  (:import [com.mongodb.client.model ReplaceOptions]
           [com.mongodb.client.result UpdateResult]))

(defn replace-options ^ReplaceOptions [{:keys [upsert? collation hint]}]
  (cond-> (ReplaceOptions.)
    upsert?   (.upsert true)
    collation (.collation collation)
    hint      (.hint (if (map? hint)
                       (clj->doc hint)
                       (list->doc hint)))))

(defmulti replace-method ^UpdateResult
  (fn [_coll _query _doc _options]
    {:session (some? *mongo-session*)}))

(defmethod replace-method {:session true} [coll query doc options]
  (.replaceOne coll *mongo-session* query doc options))

(defmethod replace-method {:session false} [coll query doc options]
  (.replaceOne coll query doc options))

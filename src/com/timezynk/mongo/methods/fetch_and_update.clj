(ns ^:no-doc com.timezynk.mongo.methods.fetch-and-update
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.convert-types :refer [clj->doc list->doc]])
  (:import [org.bson Document]
           [com.mongodb.client.model FindOneAndUpdateOptions ReturnDocument]))

(defn fetch-and-update-options ^FindOneAndUpdateOptions [{:keys [return-new? upsert? collation only hint sort]}]
  (cond-> (FindOneAndUpdateOptions.)
    return-new? (.returnDocument ReturnDocument/AFTER)
    upsert?     (.upsert true)
    collation   (.collation collation)
    only        (.projection (if (map? only)
                               (clj->doc only)
                               (list->doc only)))
    hint        (.hint (if (map? hint)
                         (clj->doc hint)
                         (list->doc hint)))
    sort        (.sort (if (map? sort)
                         (clj->doc sort)
                         (list->doc sort)))))

(defmulti fetch-and-update-method ^Document
  (fn [_coll _query _update _options]
    {:session (some? *mongo-session*)}))

(defmethod fetch-and-update-method {:session true} [coll query update options]
  (.findOneAndUpdate coll *mongo-session* query update options))

(defmethod fetch-and-update-method {:session false} [coll query update options]
  (.findOneAndUpdate coll query update options))

(ns ^:no-doc com.timezynk.mongo.methods.fetch-and-delete
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.convert-types :refer [clj->doc list->doc]])
  (:import [org.bson Document]
           [com.mongodb.client.model FindOneAndDeleteOptions]))

(defn fetch-and-delete-options ^FindOneAndDeleteOptions [{:keys [collation only hint sort]}]
  (cond-> (FindOneAndDeleteOptions.)
    collation (.collation collation)
    only      (.projection (if (map? only)
                             (clj->doc only)
                             (list->doc only)))
    hint      (.hint (if (map? hint)
                       (clj->doc hint)
                       (list->doc hint)))
    sort      (.sort (if (map? sort)
                       (clj->doc sort)
                       (list->doc sort)))))

(defmulti fetch-and-delete-method ^Document
  (fn [_coll _query _options]
    {:session (some? *mongo-session*)}))

(defmethod fetch-and-delete-method {:session true} [coll query options]
  (.findOneAndDelete coll *mongo-session* query options))

(defmethod fetch-and-delete-method {:session false} [coll query options]
  (.findOneAndDelete coll query options))

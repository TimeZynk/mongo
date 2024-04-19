(ns ^:no-doc com.timezynk.mongo.methods.update
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.convert-types :refer [clj->doc list->doc]])
  (:import [com.mongodb.client.model UpdateOptions]
           [com.mongodb.client.result UpdateResult]))

(defn update-options ^UpdateOptions [{:keys [upsert? collation hint]}]
  (cond-> (UpdateOptions.)
    upsert?   (.upsert true)
    collation (.collation collation)
    hint      (.hint (if (map? hint)
                       (clj->doc hint)
                       (list->doc hint)))))

(defmulti update-method ^UpdateResult
  (fn [_coll _query _update _options]
    {:session (some? *mongo-session*)}))

(defmethod update-method {:session true} [coll query update options]
  (.updateMany coll *mongo-session* query update options))

(defmethod update-method {:session false} [coll query update options]
  (.updateMany coll query update options))

(defmulti update-one-method ^UpdateResult
  (fn [_coll _query _update _options]
    {:session (some? *mongo-session*)}))

(defmethod update-one-method {:session true} [coll query update options]
  (.updateOne coll *mongo-session* query update options))

(defmethod update-one-method {:session false} [coll query update options]
  (.updateOne coll query update options))

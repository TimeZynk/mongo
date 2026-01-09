(ns ^:no-doc com.timezynk.mongo.methods.update
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.convert :refer [list->map]]
   [com.timezynk.mongo.hooks :refer [*read-hook*]]
   [com.timezynk.mongo.padding :refer [*update-padding*]])
  (:import [com.mongodb.client.model UpdateOptions]
           [com.mongodb.client.result
            UpdateResult
            UpdateResult$AcknowledgedUpdateResult
            UpdateResult$UnacknowledgedUpdateResult]))

(defprotocol UpdRes
  (update-result [result]))

(extend-protocol UpdRes
  UpdateResult$AcknowledgedUpdateResult
  (update-result [result]
    (merge {:matched-count  (.getMatchedCount result)
            :modified-count (.getModifiedCount result)
            :acknowledged   true}
           (when-let [v (.getUpsertedId result)]
             (-> {:_id (.getValue v)}
                 (*read-hook*)))))

  UpdateResult$UnacknowledgedUpdateResult
  (update-result [_result]
    {:acknowledged false}))

(defn get-options [obj {:keys [upsert? collation hint]}]
  (cond-> obj
    upsert?   (.upsert true)
    collation (.collation collation)
    hint      (.hint (->bson (list->map hint)))))

(defn- update-options ^UpdateOptions [options]
  (get-options (UpdateOptions.) options))

(defmulti update-method ^UpdateResult
  (fn [_coll _query _update _options]
    (some? *mongo-session*)))

(defmethod update-method true [coll query update options]
  (-> (.updateMany coll
                   *mongo-session*
                   (->bson query)
                   (->bson (*update-padding* update))
                   (update-options options))
      (update-result)))

(defmethod update-method false [coll query update options]
  (-> (.updateMany coll
                   (->bson query)
                   (->bson (*update-padding* update))
                   (update-options options))

      (update-result)))

(defmulti update-one-method ^UpdateResult
  (fn [_coll _query _update _options]
    (some? *mongo-session*)))

(defmethod update-one-method true [coll query update options]
  (-> (.updateOne coll
                  *mongo-session*
                  (->bson query)
                  (->bson (*update-padding* update))
                  (update-options options))
      (update-result)))

(defmethod update-one-method false [coll query update options]
  (-> (.updateOne coll
                  (->bson query)
                  (->bson (*update-padding* update))
                  (update-options options))
      (update-result)))

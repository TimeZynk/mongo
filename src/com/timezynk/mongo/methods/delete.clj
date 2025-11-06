(ns ^:no-doc com.timezynk.mongo.methods.delete
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.convert :refer [list->map]]
   [com.timezynk.mongo.helpers :as h])
  (:import [com.mongodb.client.model DeleteOptions]
           [com.mongodb.client.result
            DeleteResult
            DeleteResult$AcknowledgedDeleteResult
            DeleteResult$UnacknowledgedDeleteResult]))

(defprotocol DelRes
  (delete-result [result]))

(extend-protocol DelRes
  DeleteResult$AcknowledgedDeleteResult
  (delete-result [result]
    {:deleted-count (.getDeletedCount result)
     :acknowledged  true})

  DeleteResult$UnacknowledgedDeleteResult
  (delete-result [_result]
    {:acknowledged false}))

(defn- delete-options ^DeleteOptions [{:keys [collation hint]}]
  (cond-> (DeleteOptions.)
    collation (.collation collation)
    hint      (.hint (->bson (list->map hint)))))

(defmulti delete-method ^DeleteResult
  (fn [_coll _query _options]
    (some? *mongo-session*)))

(defmethod delete-method true [coll query options]
  (-> (.deleteMany (h/get-collection coll)
                   *mongo-session*
                   (->bson query)
                   (delete-options options))
      (delete-result)))

(defmethod delete-method false [coll query options]
  (-> (.deleteMany (h/get-collection coll)
                   (->bson query)
                   (delete-options options))
      (delete-result)))

(defmulti delete-one-method ^DeleteResult
  (fn [_coll _query _options]
    (some? *mongo-session*)))

(defmethod delete-one-method true [coll query options]
  (-> (.deleteOne (h/get-collection coll)
                  *mongo-session*
                  (->bson query)
                  (delete-options options))
      (delete-result)))

(defmethod delete-one-method false [coll query options]
  (-> (.deleteOne (h/get-collection coll)
                  (->bson query)
                  (delete-options options))
      (delete-result)))

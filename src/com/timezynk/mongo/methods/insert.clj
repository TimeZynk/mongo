(ns ^:no-doc com.timezynk.mongo.methods.insert
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.helpers :as h]
   [com.timezynk.mongo.hooks :refer [*read-hook*]]
   [com.timezynk.mongo.padding :refer [*insert-padding*]])
  (:import [com.mongodb.client.result
            InsertManyResult$AcknowledgedInsertManyResult
            InsertManyResult$UnacknowledgedInsertManyResult
            InsertOneResult$AcknowledgedInsertOneResult
            InsertOneResult$UnacknowledgedInsertOneResult]))

(defprotocol InsertResult
  (insert-result [result docs]))

(extend-protocol InsertResult
  InsertManyResult$AcknowledgedInsertManyResult
  (insert-result [result docs]
    (let [ids (.getInsertedIds result)]
      (map-indexed (fn [i d]
                     (merge d
                            (-> {:_id (-> (get ids (int i))
                                          (.getValue))}
                                (*read-hook*))))
                   docs)))

  InsertManyResult$UnacknowledgedInsertManyResult
  (insert-result [_result docs]
    docs)

  InsertOneResult$AcknowledgedInsertOneResult
  (insert-result [result doc]
    (merge doc
           (-> {:_id (-> (.getInsertedId result)
                         (.getValue))}
               (*read-hook*))))

  InsertOneResult$UnacknowledgedInsertOneResult
  (insert-result [_result doc]
    doc))

(defmulti insert-padding
  (fn [docs]
    (sequential? docs)))

(defmethod insert-padding true [docs]
  (map *insert-padding* docs))

(defmethod insert-padding false [doc]
  (*insert-padding* doc))

(defmulti insert-method
  (fn [_coll docs]
    {:session (some? *mongo-session*)
     :many    (sequential? docs)}))

(defmethod insert-method {:session true :many true} [coll docs]
  (.insertMany (h/get-collection coll)
               *mongo-session*
               docs))

(defmethod insert-method {:session true :many false} [coll doc]
  (.insertOne (h/get-collection coll)
              *mongo-session*
              doc))

(defmethod insert-method {:session false :many true} [coll docs]
  (.insertMany (h/get-collection coll)
               docs))

(defmethod insert-method {:session false :many false} [coll doc]
  (.insertOne (h/get-collection coll)
              doc))

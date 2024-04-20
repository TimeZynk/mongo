(ns ^:no-doc com.timezynk.mongo.methods.insert
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]])
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
    (map-indexed (fn [i d]
                   (assoc d :_id (-> (.getInsertedIds result)
                                     (get (int i))
                                     (.getValue))))
                 docs))

  InsertManyResult$UnacknowledgedInsertManyResult
  (insert-result [_result docs]
    docs)

  InsertOneResult$AcknowledgedInsertOneResult
  (insert-result [result doc]
    (assoc doc :_id (-> (.getInsertedId result)
                        (.getValue))))

  InsertOneResult$UnacknowledgedInsertOneResult
  (insert-result [_result doc]
    doc))

(defmulti insert-method
  (fn [_coll doc]
    {:session (some? *mongo-session*)
     :many    (sequential? doc)}))

(defmethod insert-method {:session true :many true} [coll docs]
  (-> (.insertMany coll *mongo-session* docs)
      (insert-result docs)))

(defmethod insert-method {:session false :many true} [coll docs]
  (-> (.insertMany coll docs)
      (insert-result docs)))

(defmethod insert-method {:session true :many false} [coll doc]
  (-> (.insertOne coll *mongo-session* doc)
      (insert-result doc)))

(defmethod insert-method {:session false :many false} [coll doc]
  (-> (.insertOne coll doc)
      (insert-result doc)))

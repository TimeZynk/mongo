(ns ^:no-doc com.timezynk.mongo.methods.insert
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]])
  (:import [com.mongodb WriteConcern]
           [com.mongodb.client.result InsertManyResult InsertOneResult]))

(defn insert-options [coll {:keys [write-concern]}]
  (cond-> coll
    write-concern (.withWriteConcern (case write-concern
                                       :acknowledged   WriteConcern/ACKNOWLEDGED
                                       :unacknowledged WriteConcern/UNACKNOWLEDGED
                                       :journaled      WriteConcern/JOURNALED
                                       :majority       WriteConcern/MAJORITY
                                       :w1             WriteConcern/W1
                                       :w2             WriteConcern/W2
                                       :w3             WriteConcern/W3))))

(defmulti insert-method
  (fn [_coll doc]
    {:session (some? *mongo-session*)
     :many    (sequential? doc)}))

(defn insert-ids [docs ^InsertManyResult result]
  (map-indexed (fn [i d]
                 (assoc d :_id (-> (.getInsertedIds result)
                                   (get (int i))
                                   (.getValue))))
               docs))

(defmethod insert-method {:session true :many true} [coll docs]
  (->> (.insertMany coll *mongo-session* docs)
       (insert-ids docs)))

(defmethod insert-method {:session false :many true} [coll docs]
  (->> (.insertMany coll docs)
       (insert-ids docs)))

(defn insert-id [doc ^InsertOneResult result]
  (assoc doc :_id (-> (.getInsertedId result)
                      (.getValue))))

(defmethod insert-method {:session true :many false} [coll doc]
  (->> (.insertOne coll *mongo-session* doc)
       (insert-id doc)))

(defmethod insert-method {:session false :many false} [coll doc]
  (->> (.insertOne coll doc)
       (insert-id doc)))

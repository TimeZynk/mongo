(ns ^:no-doc com.timezynk.mongo.methods.insert
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]])
  (:import [com.mongodb WriteConcern]))

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

(defmethod insert-method {:session true :many true} [coll doc]
  (.insertMany coll *mongo-session* doc))

(defmethod insert-method {:session false :many true} [coll doc]
  (.insertMany coll doc))

(defmethod insert-method {:session true :many false} [coll doc]
  (.insertOne coll *mongo-session* doc))

(defmethod insert-method {:session false :many false} [coll doc]
  (.insertOne coll doc))

(ns ^:no-doc com.timezynk.mongo.methods.count
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]]))

(defmulti count-method
  (fn [_coll _query]
    {:session (some? *mongo-session*)}))

(defmethod count-method {:session true} [coll query]
  (.countDocuments coll *mongo-session* query))

(defmethod count-method {:session false} [coll query]
  (.countDocuments coll query))

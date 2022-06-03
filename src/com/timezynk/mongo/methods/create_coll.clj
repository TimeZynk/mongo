(ns ^:no-doc com.timezynk.mongo.methods.create-coll
  (:require
   [com.timezynk.mongo.config :refer [*mongo-config* *mongo-session*]])
    (:import [com.mongodb.client.model CreateCollectionOptions]))

(defn- with-options [{:keys [collation]}]
  (cond-> (CreateCollectionOptions.)
    collation (.collation collation)))

(defmulti create-coll-method
  (fn [_name options]
    {:session (some? *mongo-session*)
     :options (seq? options)}))

(defmethod create-coll-method {:session true :options false} [name _options]
  (.createCollection (:db *mongo-config*) *mongo-session* name))

(defmethod create-coll-method {:session true :options true} [name options]
  (.createCollection (:db *mongo-config*) *mongo-session* name (with-options options)))

(defmethod create-coll-method {:session false :options false} [name _options]
  (.createCollection (:db *mongo-config*) name))

(defmethod create-coll-method {:session false :options true} [name options]
  (.createCollection (:db *mongo-config*) name (with-options options)))

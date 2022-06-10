(ns ^:no-doc com.timezynk.mongo.methods.fetch
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.utils.convert :as convert])
  (:import [org.bson Document]))

(defn- with-options [result {:keys [collation limit only skip sort]}]
  (cond-> result
    collation (.collation collation)
    limit     (.limit limit)
    only      (.projection (convert/clj->doc only))
    skip      (.skip skip)
    sort      (.sort (convert/clj->doc sort))))

(defmulti fetch-method ^Document
  (fn [_coll _query options]
    {:session (some? *mongo-session*)
     :options (seq? options)}))

(defmethod fetch-method {:session true :options false} [coll query _options]
  (.find coll *mongo-session* query))

(defmethod fetch-method {:session true :options true} [coll query options]
  (-> (.find coll *mongo-session* query)
      (with-options options)))

(defmethod fetch-method {:session false :options false} [coll query _options]
  (.find coll query))

(defmethod fetch-method {:session false :options true} [coll query options]
  (-> (.find coll query)
      (with-options options)))

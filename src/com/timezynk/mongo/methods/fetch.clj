(ns ^:no-doc com.timezynk.mongo.methods.fetch
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.convert :refer [list->map]]))

(defn- with-options [result {:keys [collation limit only skip sort]}]
  (cond-> result
    collation (.collation collation)
    limit     (.limit limit)
    only      (.projection (->bson (list->map only)))
    skip      (.skip skip)
    sort      (.sort (->bson (list->map sort)))))

(defmulti fetch-method
  (fn [_coll _query options]
    {:session (some? *mongo-session*)
     :options (coll? options)}))

(defmethod fetch-method {:session true :options true} [coll query options]
  (-> (.find coll *mongo-session* query)
      (with-options options)))

(defmethod fetch-method {:session true :options false} [coll query _options]
  (.find coll *mongo-session* query))

(defmethod fetch-method {:session false :options true} [coll query options]
  (-> (.find coll query)
      (with-options options)))

(defmethod fetch-method {:session false :options false} [coll query _options]
  (.find coll query))

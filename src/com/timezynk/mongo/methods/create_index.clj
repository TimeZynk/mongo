(ns ^:no-doc com.timezynk.mongo.methods.create-index
  (:require
  ;;  [clojure.tools.logging :as log]
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.utils.convert :as convert])
  (:import [com.mongodb.client.model IndexOptions]))

(defn- create-options ^IndexOptions
  [{:keys [background name partial-filter-expression sparse unique]}]
  (cond-> (IndexOptions.)
    background                (.background background)
    name                      (.name name)
    partial-filter-expression (.partialFilterExpression
                               (convert/clj->doc partial-filter-expression))
    sparse                    (.sparse sparse)
    unique                    (.unique unique)))

(defmulti create-index-method
  (fn [_coll _keys _options]
    {:session (some? *mongo-session*)}))

(defmethod create-index-method {:session true} [coll keys options]
  (.createIndex coll *mongo-session* keys (create-options options)))

(defmethod create-index-method {:session false} [coll keys options]
  (.createIndex coll keys (create-options options)))

(ns ^:no-doc com.timezynk.mongo.watch-methods.config
  (:require
   [clojure.set :as set]
   [com.timezynk.mongo.config :refer [*mongo-client*]]))

(defonce ^:private watch-ids
  (atom #{}))

(defn add-watch-id
  "Create an entry from the current connection, operation type, and function.

   **Returns**

   `true` if an entry was created.
   `false` id the entry already existed."
  [op-type watch-fn]
  (let [[old new] (swap-vals! watch-ids
                              conj
                              [(System/identityHashCode *mongo-client*)
                               op-type
                               (System/identityHashCode watch-fn)])]
    (not= (count old)
          (count new))))

(defn close-watch-ids
  "Remove all ids for this connection."
  []
  (let [client-id (System/identityHashCode *mongo-client*)]
    (swap! watch-ids
           (partial set/select (fn [[k _ _]]
                                 (not= k client-id))))))

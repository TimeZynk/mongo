(ns ^:no-doc com.timezynk.mongo.watch-methods.update
  (:require
   [clojure.core.async :as async]
   [com.timezynk.mongo.watch-methods.common :as w :refer [UPDATE]]
   [com.timezynk.mongo.watch-methods.config :as config])
  (:import [com.mongodb.client.model.changestream ChangeStreamDocument]
           [java.lang IllegalStateException]))

(defn on-update-method [coll update-fn {:keys [full?] :as options}]
  (w/check-full coll full? UPDATE)
  (when (config/add-watch-id UPDATE update-fn)
    (let [cursor (w/get-cursor coll UPDATE options)]
      (async/thread
        (try
          (loop []
            (let [event ^ChangeStreamDocument (.next cursor)
                  time  (w/get-time event)
                  coll  (w/get-collection event)
                  delta (w/get-delta event)]
              (if-let [doc-before (and full?
                                       (.getFullDocumentBeforeChange event))]
                (update-fn coll
                           time
                           delta
                           doc-before)
                (update-fn coll
                           time
                           delta))
              (recur)))
          (catch IllegalStateException e
            (w/handle-exception e UPDATE))
          (finally
            (.close cursor)))))))

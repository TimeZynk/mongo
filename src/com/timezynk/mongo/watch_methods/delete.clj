(ns ^:no-doc com.timezynk.mongo.watch-methods.delete
  (:require
   [clojure.core.async :as async]
   [com.timezynk.mongo.watch-methods.common :as w :refer [DELETE]]
   [com.timezynk.mongo.watch-methods.config :as config])
  (:import [com.mongodb.client.model.changestream ChangeStreamDocument]
           [java.lang IllegalStateException]))

(defn on-delete-method [coll delete-fn {:keys [full?] :as options}]
  (w/check-full coll full? DELETE)
  (when (config/add-watch-id DELETE delete-fn)
    (let [cursor (w/get-cursor coll DELETE options)]
      (async/thread
        (try
          (loop []
            (let [event ^ChangeStreamDocument (.next cursor)
                  time  (w/get-time event)
                  coll  (w/get-collection event)]
              (if-let [doc-before (and full?
                                       (.getFullDocumentBeforeChange event))]
                (delete-fn coll
                           time
                           doc-before)
                (delete-fn coll
                           time
                           (w/get-id event)))
              (recur)))
          (catch IllegalStateException e
            (w/handle-exception e DELETE))
          (finally
            (.close cursor)))))))

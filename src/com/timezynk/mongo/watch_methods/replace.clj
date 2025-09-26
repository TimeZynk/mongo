(ns ^:no-doc com.timezynk.mongo.watch-methods.replace
  (:require
   [clojure.core.async :as async]
   [com.timezynk.mongo.watch-methods.common :as w :refer [REPLACE]]
   [com.timezynk.mongo.watch-methods.config :as config])
  (:import [com.mongodb.client.model.changestream ChangeStreamDocument]
           [java.lang IllegalStateException]))

(defn on-replace-method [coll replace-fn {:keys [full?] :as options}]
  (w/check-full coll full? REPLACE)
  (when (config/add-watch-id REPLACE replace-fn)
    (let [cursor (w/get-cursor coll REPLACE options)]
      (async/thread
        (try
          (loop []
            (let [event ^ChangeStreamDocument (.next cursor)
                  time  (w/get-time event)
                  coll  (w/get-collection event)
                  doc   (.getFullDocument event)]
              (if-let [doc-before (and full?
                                       (.getFullDocumentBeforeChange event))]
                (replace-fn coll
                            time
                            doc
                            doc-before)
                (replace-fn coll
                            time
                            doc))
              (recur)))
          (catch IllegalStateException e
            (w/handle-exception e REPLACE))
          (finally
            (.close cursor)))))))

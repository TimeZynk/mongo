(ns ^:no-doc com.timezynk.mongo.watch-methods.replace
  (:require
   [clojure.core.async :as async]
   [com.timezynk.mongo.watch-methods.common :as w :refer [REPLACE]])
  (:import [com.mongodb.client.model.changestream ChangeStreamDocument]
           [java.lang IllegalStateException]))

(defn on-replace-method [coll replace-fn {:keys [full?] :as options}]
  (w/check-full coll full? REPLACE)
  (let [cursor (w/get-cursor coll REPLACE options)]
    (async/thread
      (try
        (while true
          (let [event ^ChangeStreamDocument (.next cursor)
                time  (w/get-time event)
                coll  (-> (.getNamespace event)
                          (.getCollectionName)
                          (keyword))
                doc   (.getFullDocument event)]
            (if-let [doc-before (and full?
                                     (.getFullDocumentBeforeChange event))]
              (replace-fn coll
                          time
                          doc
                          doc-before)
              (replace-fn coll
                          time
                          doc))))
        (catch IllegalStateException e
          (w/handle-exception e REPLACE))
        (finally
          (.close cursor))))))

(ns ^:no-doc com.timezynk.mongo.watch-methods.insert
  (:require
   [clojure.core.async :as async]
   [com.timezynk.mongo.watch-methods.common :as w :refer [INSERT]])
  (:import [com.mongodb.client.model.changestream ChangeStreamDocument]
           [java.lang IllegalStateException]))

(defn on-insert-method [coll insert-fn options]
  (let [cursor (w/get-cursor coll INSERT options)]
    (async/thread
      (try
        (while true
          (let [event ^ChangeStreamDocument (.next cursor)
                time  (w/get-time event)
                coll  (-> (.getNamespace event)
                          (.getCollectionName)
                          (keyword))
                doc   (.getFullDocument event)]
            (insert-fn coll time doc)))
        (catch IllegalStateException e
          (w/handle-exception e INSERT))
        (finally
          (.close cursor))))))

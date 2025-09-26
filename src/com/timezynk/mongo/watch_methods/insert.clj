(ns ^:no-doc com.timezynk.mongo.watch-methods.insert
  (:require
   [clojure.core.async :as async]
   [com.timezynk.mongo.watch-methods.common :as w :refer [INSERT]]
   [com.timezynk.mongo.watch-methods.config :as config])
  (:import [com.mongodb.client.model.changestream ChangeStreamDocument]
           [java.lang IllegalStateException]))

(defn on-insert-method [coll insert-fn options]
  (when (config/add-watch-id INSERT insert-fn)
    (let [cursor (w/get-cursor coll INSERT options)]
      (async/thread
        (try
          (loop []
            (let [event ^ChangeStreamDocument (.next cursor)
                  time  (w/get-time event)
                  coll  (w/get-collection event)
                  doc   (.getFullDocument event)]
              (insert-fn coll
                         time
                         doc)
              (recur)))
          (catch IllegalStateException e
            (w/handle-exception e INSERT))
          (finally
            (.close cursor)))))))

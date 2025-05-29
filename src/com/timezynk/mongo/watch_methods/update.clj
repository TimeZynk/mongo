(ns ^:no-doc com.timezynk.mongo.watch-methods.update
  (:require
   [clojure.core.async :as async]
   [com.timezynk.mongo.convert :as convert]
   [com.timezynk.mongo.watch-methods.common :as w :refer [UPDATE]])
  (:import [com.mongodb.client.model.changestream ChangeStreamDocument UpdateDescription]
           [java.lang IllegalStateException]
           [org.bson BsonDocument]))

(defn on-update-method [coll update-fn {:keys [full?] :as options}]
  (w/check-full coll full? UPDATE)
  (let [cursor (w/get-cursor coll UPDATE options)]
    (async/thread
      (try
        (while true
          (let [event ^ChangeStreamDocument (.next cursor)
                time  (w/get-time event)
                coll  (-> (.getNamespace event)
                          (.getCollectionName)
                          (keyword))
                delta (merge (w/get-id event)
                             (-> ^UpdateDescription (.getUpdateDescription event)
                                 ^BsonDocument (.getUpdatedFields)
                                 (convert/decode-bson-document)))]
            (if-let [doc-before (and full?
                                     (.getFullDocumentBeforeChange event))]
              (update-fn coll
                         time
                         delta
                         doc-before)
              (update-fn coll
                         time
                         delta))))
        (catch IllegalStateException e
          (w/handle-exception e UPDATE))
        (finally
          (.close cursor))))))

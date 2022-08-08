(ns com.timezynk.mongo.watch
  (:require
   [clojure.core.async :as async]
   [clojure.tools.logging :as log]
   [com.timezynk.mongo.utils.collection :as coll]
   [com.timezynk.mongo.utils.convert :as convert])
  (:import [com.mongodb.client MongoCollection MongoCursor MongoIterable]
           [com.mongodb.client.model.changestream ChangeStreamDocument]
           [com.mongodb MongoException]
           [java.util Date]
           [org.bson BsonTimestamp Document]))

(defn insert
  "Listen for an `insert!` event."
  [coll insert-fn]
  (let [cursor (->> ^MongoCollection (coll/get-collection coll)
                    ^MongoIterable (.watch)
                    ^MongoCursor (.iterator))
        chan   (async/chan (async/sliding-buffer 1))
        inc    (let [i (iterate inc 0)]
                 (fn [] (next i)))]
    (async/thread
      (try
        (while (.hasNext cursor)
          (let [event ^ChangeStreamDocument (.next cursor)
                time  (-> ^BsonTimestamp (.getClusterTime event)
                          ^Long (.getTime)
                          (* 1000)
                          (Date.))
                doc   (-> ^Document (.getFullDocument event)
                          (convert/doc->clj))]
            (insert-fn time doc))
          (log/spy "AAAAAAA")
          ;; (log/spy (inc))
          #_(async/put! chan (inc)))
        (.close cursor)
        (catch MongoException e
          (if (= (.getMessage e) "state should be: open")
            (log/info "Cursor closed, insert watch terminated")
            (throw (MongoException. e))))))
    chan))

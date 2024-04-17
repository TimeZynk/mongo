(ns ^:no-doc com.timezynk.mongo.methods.watch
  (:require
   [clojure.core.async :as async]
   [clojure.tools.logging :as log]
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.helpers :as h])
  (:import [com.mongodb.client MongoCollection MongoCursor MongoIterable]
           [com.mongodb.client.model.changestream ChangeStreamDocument UpdateDescription]
           [com.mongodb MongoException]
           [java.util Date]
           [org.bson BsonDocument BsonTimestamp Document]))

(defmulti watch-method ^MongoIterable
  (fn [^MongoCollection _coll ^String _op-type]
    {:session (some? *mongo-session*)}))

(defmethod watch-method {:session true} [coll op-type]
  (.watch coll *mongo-session* (->bson [{:$match {:operationType op-type}}])))

(defmethod watch-method {:session false} [coll op-type]
  (.watch coll (->bson [{:$match {:operationType op-type}}])))

(defn- get-cursor [coll op-type]
  (-> ^MongoCollection (h/get-collection coll)
      ^MongoIterable (watch-method op-type)
      ^MongoCursor (.iterator)))

(defn- get-time [event]
  (-> ^BsonTimestamp (.getClusterTime event)
      ^Long (.getTime)
      (* 1000)
      (Date.)))

(defn- get-id [event]
  (-> ^BsonDocument (.getDocumentKey event)
      (Document.)))

(defn- handle-exception [e text]
  (if (= (.getMessage e) "state should be: open")
    (log/info (str "Cursor closed, " text " watch terminated"))
    (throw (MongoException. e))))

(defn insert-method [coll insert-fn]
  (let [cursor (get-cursor coll "insert")]
    (async/thread
      (try
        (while (.hasNext cursor)
          (let [event ^ChangeStreamDocument (.next cursor)
                time  (get-time event)
                doc   (-> ^Document (.getFullDocument event))]
            (insert-fn time doc)))
        (.close cursor)
        (catch MongoException e
          (handle-exception e "insert"))))))

(defn update-method [coll update-fn]
  (let [cursor (get-cursor coll "update")]
    (async/thread
      (try
        (while (.hasNext cursor)
          (let [event  ^ChangeStreamDocument (.next cursor)
                time   (get-time event)
                id     (get-id event)
                update (-> ^UpdateDescription (.getUpdateDescription event)
                           ^BsonDocument (.getUpdatedFields)
                           (Document.))]
            (update-fn time (merge id update))))
        (.close cursor)
        (catch MongoException e
          (handle-exception e "update"))))))

(defn delete-method [coll delete-fn]
  (let [cursor (get-cursor coll "delete")]
    (async/thread
      (try
        (while (.hasNext cursor)
          (let [event ^ChangeStreamDocument (.next cursor)
                time  (get-time event)
                id    (get-id event)]
            (delete-fn time id)))
        (.close cursor)
        (catch MongoException e
          (handle-exception e "delete"))))))

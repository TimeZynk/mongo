(ns ^:no-doc com.timezynk.mongo.watch-methods.common
  (:refer-clojure :exclude [filter])
  (:require
   [clojure.tools.logging :as log]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-database* *mongo-session*]]
   [com.timezynk.mongo.convert :as convert]
   [com.timezynk.mongo.helpers :as h])
  (:import [clojure.lang PersistentArrayMap]
           [com.mongodb MongoChangeStreamException]
           [com.mongodb.client ChangeStreamIterable MongoCollection MongoCursor MongoIterable]
           [com.mongodb.client.model.changestream FullDocument FullDocumentBeforeChange]
           [java.lang IllegalStateException]
           [org.bson BsonDateTime BsonDocument]
           [org.bson.conversions Bson]))

(def ^:const DELETE  "delete")
(def ^:const INSERT  "insert")
(def ^:const REPLACE "replace")
(def ^:const UPDATE  "update")

(defn check-full [coll full? op-type]
  (when (and coll
             full?
             (not (get-in (m/collection-info coll)
                          [:options :changeStreamPreAndPostImages :enabled])))
    (throw (MongoChangeStreamException. (str "Create " op-type " change stream failed: Collection not set for passing full documents")))))

(defn build-pipeline [op-type filter prefix]
  [^Bson (->bson {:$match (merge {:operationType op-type}
                                 (when filter
                                   (update-keys filter
                                                #(->> (name %)
                                                      (str prefix)
                                                      (keyword)))))})])

(defn- pipeline [op-type {:keys [filter]}]
  (case op-type
    "delete"  (build-pipeline op-type nil nil)
    "insert"  (build-pipeline op-type filter "fullDocument.")
    "replace" (build-pipeline op-type filter "fullDocument.")
    "update"  (build-pipeline op-type filter "updateDescription.updatedFields.")))

(defmulti watch-method ^MongoIterable
  (fn [_obj ^String _op-type _options]
    (some? *mongo-session*)))

(defmethod watch-method true [obj op-type options]
  (.watch obj
          *mongo-session*
          (pipeline op-type options)
          PersistentArrayMap))

(defmethod watch-method false [obj op-type options]
  (.watch obj
          (pipeline op-type options)
          PersistentArrayMap))

(defn get-cursor [coll op-type {:keys [collation full?] :as options}]
  (cond-> (if coll
            ^MongoCollection (h/get-collection coll)
            *mongo-database*)
    true      ^ChangeStreamIterable (watch-method op-type options)
    collation (.collation collation)
    full?     (.fullDocument             (if coll
                                           FullDocument/REQUIRED
                                           FullDocument/WHEN_AVAILABLE))
    full?     (.fullDocumentBeforeChange (if coll
                                           FullDocumentBeforeChange/REQUIRED
                                           FullDocumentBeforeChange/WHEN_AVAILABLE))
    true      ^MongoCursor (.iterator)))

(defn get-time [event]
  (-> ^BsonDateTime (.getWallTime event)
      (convert/decode-bson-value)))

(defn get-id [event]
  (-> ^BsonDocument (.getDocumentKey event)
      (convert/decode-bson-document)))

(defn handle-exception [e text]
  (if (= (.getMessage e)
         "state should be: open")
    (log/info "Cursor closed," text "watch terminated")
    (throw (IllegalStateException. e))))

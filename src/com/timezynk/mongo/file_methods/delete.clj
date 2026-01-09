(ns ^:no-doc com.timezynk.mongo.file-methods.delete
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.helpers :refer [file-hooks]]
   [com.timezynk.mongo.hooks :refer [ignore-hooks]]
   [com.timezynk.mongo.methods.fetch :refer [fetch-method]])
  (:import [com.mongodb.client.gridfs GridFSBucket]
           [com.mongodb.client.gridfs.model GridFSFile]
           [org.bson.types ObjectId]))

(defmulti ^:private delete
  (fn [_bucket _id]
    (some? *mongo-session*)))

(defmethod delete true [bucket id]
  (.delete bucket *mongo-session* id))

(defmethod delete false [bucket id]
  (.delete bucket id))

(defn delete-method [^GridFSBucket bucket database-file]
  {:pre [(= (type database-file) String)]}
  (let [files (ignore-hooks
                (fetch-method bucket
                              (->bson {:filename database-file})
                              nil))]
    (doseq [^GridFSFile f files]
      (->> (.getObjectId f)
           (delete bucket)))
    {:deleted-count (count files)}))

(defn delete-by-id-method [^GridFSBucket bucket id]
  {:pre [(= (type id) ObjectId)]}
  (delete bucket id)
  {:deleted-count 1})

(defn delete-by-query-method [^GridFSBucket bucket query options]
  (let [files (file-hooks
                (fetch-method bucket
                              (->bson query)
                              options))]
    (doseq [^GridFSFile f files]
      (->> (.getObjectId f)
           (delete bucket)))
    {:deleted-count (count files)}))

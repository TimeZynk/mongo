(ns ^:no-doc com.timezynk.mongo.file-methods.rename
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.methods.fetch :refer [fetch-method]]
   [com.timezynk.mongo.hooks :refer [ignore-hooks]])
  (:import [com.mongodb.client.gridfs GridFSBucket]
           [com.mongodb.client.gridfs.model GridFSFile]
           [org.bson.types ObjectId]))

(defmulti ^:private rename
  (fn [_bucket _id _filename]
    (some? *mongo-session*)))

(defmethod rename true [bucket id filename]
  (.rename bucket *mongo-session* id filename))

(defmethod rename false [bucket id filename]
  (.rename bucket id filename))

(defn rename-method [^GridFSBucket bucket database-file filename]
  {:pre [(= (type database-file) String)]}
  (let [files (ignore-hooks
                (fetch-method bucket
                              (->bson {:filename database-file})
                              nil))]
    (doseq [^GridFSFile f files]
      (rename bucket (.getObjectId f) filename))))

(defn rename-by-id-method [^GridFSBucket bucket id filename]
  {:pre [(= (type id) ObjectId)]}
  (rename bucket id filename))

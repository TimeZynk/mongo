(ns ^:no-doc com.timezynk.mongo.file-methods.download-stream
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.file-methods.download :refer [download-options]])
  (:import [com.mongodb.client.gridfs GridFSBucket GridFSDownloadStream]
           [org.bson.types ObjectId]))

(defmulti stream
  (fn [^GridFSBucket _bucket _file options]
    {:session (some? *mongo-session*)
     :options (coll? options)}))

(defmethod stream {:session true :options true} [bucket file options]
  (.openDownloadStream bucket *mongo-session* file (download-options options)))

(defmethod stream {:session true :options false} [bucket file _options]
  (.openDownloadStream bucket *mongo-session* file))

(defmethod stream {:session false :options true} [bucket file options]
  (.openDownloadStream bucket file (download-options options)))

(defmethod stream {:session false :options false} [bucket file _options]
  (.openDownloadStream bucket file))

(defn download-stream-method ^GridFSDownloadStream [^GridFSBucket bucket database-file options]
  {:pre [(= (type database-file) String)]}
  (stream bucket database-file options))

(defn download-stream-by-id-method ^GridFSDownloadStream [^GridFSBucket bucket id]
  {:pre [(= (type id) ObjectId)]}
  (stream bucket id nil))

(ns ^:no-doc com.timezynk.mongo.files.download
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]])
  (:import [com.mongodb.client.gridfs GridFSBucket]
           [com.mongodb.client.gridfs.model GridFSDownloadOptions]
           [java.io OutputStream]))

(defn download-options [{:keys [revision]}]
  (-> (GridFSDownloadOptions.)
      (.revision revision)))

(defmulti download-file-method
  (fn [^GridFSBucket _bucket ^String _file ^OutputStream _stream options]
    {:session (some? *mongo-session*)
     :options (coll? options)}))

(defmethod download-file-method {:session true :options true} [bucket file stream options]
  (.downloadToStream bucket *mongo-session* file stream (download-options options)))

(defmethod download-file-method {:session true :options false} [bucket file stream _options]
  (.downloadToStream bucket *mongo-session* file stream))

(defmethod download-file-method {:session false :options true} [bucket file stream options]
  (.downloadToStream bucket file stream (download-options options)))

(defmethod download-file-method {:session false :options false} [bucket file stream _options]
  (.downloadToStream bucket file stream))

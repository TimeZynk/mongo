(ns ^:no-doc com.timezynk.mongo.file-methods.download
  (:require
   [clojure.java.io :as io]
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.helpers :refer [by-id]]
   [com.timezynk.mongo.methods.fetch :refer [fetch-method]])
  (:import [com.mongodb.client.gridfs GridFSBucket]
           [com.mongodb.client.gridfs.model GridFSDownloadOptions]
           [java.io ByteArrayOutputStream OutputStream]
           [org.bson.types ObjectId]))

(defn download-options [{:keys [revision]}]
  (-> (GridFSDownloadOptions.)
      (.revision revision)))

(defmulti download-stream
  (fn [^GridFSBucket _bucket _file ^OutputStream _stream options]
    {:session (some? *mongo-session*)
     :options (coll? options)}))

(defmethod download-stream {:session true :options true} [bucket file stream options]
  (.downloadToStream bucket *mongo-session* file stream (download-options options)))

(defmethod download-stream {:session true :options false} [bucket file stream _options]
  (.downloadToStream bucket *mongo-session* file stream))

(defmethod download-stream {:session false :options true} [bucket file stream options]
  (.downloadToStream bucket file stream (download-options options)))

(defmethod download-stream {:session false :options false} [bucket file stream _options]
  (.downloadToStream bucket file stream))

(defmulti download-method
  (fn [^GridFSBucket _bucket _database-file output _options]
    (type output)))

(defmethod download-method String [^GridFSBucket bucket database-file output-file options]
  {:pre [(= (type database-file) String)]}
  (with-open [stream (io/output-stream output-file)]
    (download-stream bucket database-file stream options)))

(defmethod download-method OutputStream [^GridFSBucket bucket database-file output-stream options]
  {:pre [(= (type database-file) String)]}
  (download-stream bucket database-file output-stream options))

(defmethod download-method nil [^GridFSBucket bucket database-file _output options]
  {:pre [(= (type database-file) String)]}
  (with-open [stream (io/output-stream database-file)]
    (download-stream bucket database-file stream options)))

(defmulti download-by-id-method
  (fn [^GridFSBucket _bucket _id output]
    (type output)))

(defmethod download-by-id-method String [^GridFSBucket bucket id output-file]
  {:pre [(= (type id) ObjectId)]}
  (with-open [stream (io/output-stream output-file)]
    (download-stream bucket id stream nil)))

(defmethod download-by-id-method OutputStream [^GridFSBucket bucket id output-stream]
  {:pre [(= (type id) ObjectId)]}
  (download-stream bucket id output-stream nil))

(defmethod download-by-id-method nil [^GridFSBucket bucket id _output]
  {:pre [(= (type id) ObjectId)]}
  (let [filename (-> (by-id
                       (fetch-method bucket {:_id id} nil))
                     (first)
                     (.getFilename))]
    (with-open [stream (io/output-stream filename)]
      (download-stream bucket id stream nil))))

(defn download-by-query-method [^GridFSBucket bucket query options]
  (let [filenames (->> (fetch-method bucket query options)
                       (map #(.getFilename %))
                       (set))]
    (doseq [filename filenames]
      (with-open [stream (io/output-stream filename)]
        (download-stream bucket filename stream nil)))))

(defn download-array-method [^GridFSBucket bucket database-file options]
  {:pre [(= (type database-file) String)]}
  (with-open [stream (ByteArrayOutputStream.)]
    (download-stream bucket database-file stream options)
    (.toByteArray stream)))

(defn download-array-by-id-method [^GridFSBucket bucket id]
  {:pre [(= (type id) ObjectId)]}
  (with-open [stream (ByteArrayOutputStream.)]
    (download-stream bucket id stream nil)
    (.toByteArray stream)))

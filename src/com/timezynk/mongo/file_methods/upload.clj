(ns ^:no-doc com.timezynk.mongo.file-methods.upload
  (:require
   [clojure.java.io :as io]
   [com.timezynk.mongo.codecs.byte-array :as codec]
   [com.timezynk.mongo.config :refer [*mongo-database* *mongo-session*]]
   [com.timezynk.mongo.file-methods.delete :refer [delete-by-query-method]]
   [com.timezynk.mongo.hooks :refer [ignore-hooks]])
  (:import [com.mongodb.client.gridfs GridFSBucket]
           [com.mongodb.client.gridfs.model GridFSUploadOptions]
           [java.io InputStream]
           [org.bson BsonDocumentWrapper]
           [org.bson.codecs DocumentCodec DecoderContext]))

(defn- upload-options [{:keys [chunk-size metadata]}]
  (cond-> (GridFSUploadOptions.)
    chunk-size (.chunkSizeBytes chunk-size)
    metadata   (.metadata (.decode (DocumentCodec.)
                                   (-> (BsonDocumentWrapper/asBsonDocument metadata
                                                                           (.getCodecRegistry *mongo-database*))
                                       (.asBsonReader))
                                   (.build (DecoderContext/builder))))))

(defmulti upload
  (fn [^GridFSBucket _bucket ^String _file ^InputStream _stream options]
    {:session (some? *mongo-session*)
     :options (coll? options)}))

(defmethod upload {:session true :options true} [bucket file stream options]
  (.uploadFromStream bucket *mongo-session* file stream (upload-options options)))

(defmethod upload {:session true :options false} [bucket file stream _options]
  (.uploadFromStream bucket *mongo-session* file stream))

(defmethod upload {:session false :options true} [bucket file stream options]
  (.uploadFromStream bucket file stream (upload-options options)))

(defmethod upload {:session false :options false} [bucket file stream _options]
  (.uploadFromStream bucket file stream))

(defmulti ->stream
  type)

(defmethod ->stream String [input-file]
  (io/input-stream input-file))

(defmethod ->stream (codec/type-byte-array) [input-array]
  (io/input-stream input-array))

(defmethod ->stream InputStream [input-stream]
  input-stream)

(defn upload-method [^GridFSBucket bucket input database-file options]
  {:pre [(= (type database-file) String)]}
  (with-open [stream (->stream input)]
    (let [id (upload bucket database-file stream options)]
      (when (:prune? options)
        (ignore-hooks
          (delete-by-query-method bucket
                                  {:filename database-file}
                                  {:sort {:uploadDate -1}
                                   :skip 1})))
      id)))

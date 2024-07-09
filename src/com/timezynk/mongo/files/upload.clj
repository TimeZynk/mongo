(ns ^:no-doc com.timezynk.mongo.files.upload
  (:require
   [com.timezynk.mongo.config :refer [*mongo-database* *mongo-session*]])
  (:import [com.mongodb.client.gridfs GridFSBucket]
           [com.mongodb.client.gridfs.model GridFSUploadOptions]
           [java.io InputStream]
           [org.bson BsonDocumentWrapper]
           [org.bson.codecs DocumentCodec DecoderContext]))

(defn upload-options [{:keys [metadata]}]
  (cond-> (GridFSUploadOptions.)
    metadata (.metadata (.decode (DocumentCodec.)
                                 (-> (BsonDocumentWrapper/asBsonDocument metadata
                                                                         (.getCodecRegistry *mongo-database*))
                                     (.asBsonReader))
                                 (.build (DecoderContext/builder))))))

(defmulti upload-file-method
  (fn [^GridFSBucket _bucket ^String _file ^InputStream _stream options]
    {:session (some? *mongo-session*)
     :options (coll? options)}))

(defmethod upload-file-method {:session true :options true} [bucket file stream options]
  (.uploadFromStream bucket *mongo-session* file stream (upload-options options)))

(defmethod upload-file-method {:session true :options false} [bucket file stream _options]
  (.uploadFromStream bucket *mongo-session* file stream))

(defmethod upload-file-method {:session false :options true} [bucket file stream options]
  (.uploadFromStream bucket file stream (upload-options options)))

(defmethod upload-file-method {:session false :options false} [bucket file stream _options]
  (.uploadFromStream bucket file stream))

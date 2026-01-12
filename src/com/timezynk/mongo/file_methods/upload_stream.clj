(ns ^:no-doc com.timezynk.mongo.file-methods.upload-stream
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.file-methods.upload :refer [upload-options]])
  (:import [com.mongodb.client.gridfs GridFSBucket GridFSUploadStream]
           [org.bson BsonObjectId]
           [org.bson.types ObjectId]))

(defmulti stream
  (fn [^GridFSBucket _bucket ^String _file options]
    {:session (some? *mongo-session*)
     :options (coll? options)}))

(defmethod stream {:session true :options true} [bucket file {:keys [_id] :as options}]
  (if _id
    (.openUploadStream bucket *mongo-session* (BsonObjectId. _id) file (upload-options options))
    (.openUploadStream bucket *mongo-session* file (upload-options options))))

(defmethod stream {:session true :options false} [bucket file _options]
  (.openUploadStream bucket *mongo-session* file))

(defmethod stream {:session false :options true} [bucket file {:keys [_id] :as options}]
  (if _id
    (.openUploadStream bucket (BsonObjectId. _id) file (upload-options options))
    (.openUploadStream bucket file (upload-options options))))

(defmethod stream {:session false :options false} [bucket file _options]
  (.openUploadStream bucket file))

(defn upload-stream-method ^GridFSUploadStream [^GridFSBucket bucket database-file {:keys [_id] :as options}]
  {:pre [(= (type database-file) String)
         (or (nil? _id)
             (= (type _id) ObjectId))]}
  (stream bucket database-file options))

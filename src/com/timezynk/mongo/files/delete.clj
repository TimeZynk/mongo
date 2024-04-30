(ns ^:no-doc com.timezynk.mongo.files.delete
  (:require
   [com.timezynk.mongo.config :refer [*mongo-session*]])
  (:import [com.mongodb.client.gridfs GridFSBucket]
           [org.bson.types ObjectId]))

(defmulti delete-file-method
  (fn [^GridFSBucket _bucket ^ObjectId _id]
    (some? *mongo-session*)))

(defmethod delete-file-method true [bucket id]
  (.delete bucket *mongo-session* id))

(defmethod delete-file-method false [bucket id]
  (.delete bucket id))

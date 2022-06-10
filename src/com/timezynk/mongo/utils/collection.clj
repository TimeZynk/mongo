(ns ^:no-doc com.timezynk.mongo.utils.collection
  (:require
   [com.timezynk.mongo.config :refer [*mongo-config*]])
  (:import [com.mongodb.client MongoCollection]))

(defn get-collection ^MongoCollection [coll]
  (.getCollection (:db *mongo-config*)
                  (name coll)))

(ns ^:no-doc com.timezynk.mongo.utils.collection
  (:require
   [com.timezynk.mongo.config :refer [*mongo-database*]])
  (:import [com.mongodb.client MongoCollection]))

(defn get-collection ^MongoCollection [coll]
  (.getCollection *mongo-database*
                  (name coll)))

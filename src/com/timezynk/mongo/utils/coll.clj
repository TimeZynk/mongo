(ns ^:no-doc com.timezynk.mongo.utils.coll
  (:require
   [com.timezynk.mongo.config :refer [*mongo-config*]])
  (:import [com.mongodb.client MongoCollection]))

(defn get-coll ^MongoCollection [coll]
  (.getCollection (:db *mongo-config*)
                  (name coll)))

(ns ^:no-doc com.timezynk.mongo.helpers
  (:require
   [clojure.set :refer [rename-keys]]
   [com.timezynk.mongo.codecs.collection :refer [collection-provider]]
   [com.timezynk.mongo.codecs.distinct :refer [distinct-provider]]
   [com.timezynk.mongo.codecs.map :refer [map-provider]]
   [com.timezynk.mongo.config :refer [*mongo-database*]]
   [com.timezynk.mongo.hooks :refer [with-hooks]])
  (:import [clojure.lang PersistentArrayMap]
           [com.mongodb.client MongoCollection]
           [com.mongodb.client.gridfs GridFSBuckets]
           [org.bson.codecs.configuration CodecRegistries]))

(defn codec-registry [database codecs]
  (.withCodecRegistry database
                      (CodecRegistries/fromRegistries
                       [(CodecRegistries/fromProviders [(collection-provider)
                                                        (distinct-provider)
                                                        (map-provider)])
                        (CodecRegistries/fromCodecs codecs)])))

(defmacro get-collection ^MongoCollection [coll]
  `(.getCollection *mongo-database*
                   (name ~coll)
                   PersistentArrayMap))

(defmacro get-filebucket [bucket]
  `(if ~bucket
     (GridFSBuckets/create *mongo-database* (name ~bucket))
     (GridFSBuckets/create *mongo-database*)))

(defmacro by-id [& body]
  `(with-hooks {:write identity}
     ~@body))

(defmacro file-hooks [& body]
  `(with-hooks {:write #(rename-keys % {:chunk-size  :chunkSize
                                        :upload-date :uploadDate})}
     ~@body))

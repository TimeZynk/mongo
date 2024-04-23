(ns ^:no-doc com.timezynk.mongo.codecs.map
  (:require
   [com.timezynk.mongo.codecs.keyword :refer [keyword->str]]
   [com.timezynk.mongo.hooks :refer [*read-hook* *write-hook*]]
   [com.timezynk.mongo.config :refer [*mongo-types*]])
  (:import [clojure.lang PersistentArrayMap]
           [org.bson BsonType]
           [org.bson.codecs Codec]
           [org.bson.codecs.configuration CodecProvider]))

(defn map-codec [registry]
  (reify Codec
    (decode [_this reader decoder-context]
      (.readStartDocument reader)
      (loop [m {}]
        (if (= BsonType/END_OF_DOCUMENT
               (.readBsonType reader))
          (do (.readEndDocument reader)
              (*read-hook* m))
          (recur (assoc m
                        (keyword (.readName reader))
                        (let [bson-type (.getCurrentBsonType reader)]
                          (if (= bson-type BsonType/NULL)
                            (.readNull reader)
                            (.decodeWithChildContext decoder-context
                                                     (->> (get *mongo-types* bson-type)
                                                          (.get registry))
                                                     reader))))))))

    (encode [_this writer m encoder-context]
      (.writeStartDocument writer)
      (doseq [[k v] (*write-hook* m)]
        (.writeName writer (keyword->str k))
        (if (nil? v)
          (.writeNull writer)
          (.encodeWithChildContext encoder-context
                                   (->> (type v)
                                        (.get registry))
                                   writer
                                   v)))
      (.writeEndDocument writer))

    (getEncoderClass [_this]
      PersistentArrayMap)))

(defn map-provider []
  (reify CodecProvider
    (get [_this clazz registry]
      (when (= clazz PersistentArrayMap)
        (map-codec registry)))))

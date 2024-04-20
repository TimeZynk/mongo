(ns ^:no-doc com.timezynk.mongo.codecs.collection
  (:require
   [com.timezynk.mongo.config :refer [*mongo-types*]])
  (:import [clojure.lang LazySeq PersistentHashSet PersistentVector]
           [org.bson BsonType]
           [org.bson.codecs Codec]
           [org.bson.codecs.configuration CodecProvider]))

(defn collection-codec [registry]
  (reify Codec
    (decode [_this reader decoder-context]
      (.readStartArray reader)
      (loop [v []]
        (if (= BsonType/END_OF_DOCUMENT
               (.readBsonType reader))
          (do (.readEndArray reader)
              v)
          (recur (conj v
                       (let [bson-type (.getCurrentBsonType reader)]
                         (if (= bson-type BsonType/NULL)
                           (.readNull reader)
                           (.decodeWithChildContext decoder-context
                                                    (.get registry
                                                          (get *mongo-types* bson-type))
                                                    reader))))))))

    (encode [_this writer v encoder-context]
      (.writeStartArray writer)
      (doseq [l v]
        (if (nil? l)
          (.writeNull writer)
          (.encodeWithChildContext encoder-context
                                   (.get registry
                                         (type l))
                                   writer
                                   l)))
      (.writeEndArray writer))

    (getEncoderClass [_this]
      PersistentVector)))

(defn collection-provider []
  (reify CodecProvider
    (get [_this clazz registry]
      (when (contains? #{LazySeq PersistentHashSet PersistentVector} clazz)
        (collection-codec registry)))))

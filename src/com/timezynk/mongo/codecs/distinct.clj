(ns ^:no-doc com.timezynk.mongo.codecs.distinct
  (:require
   [com.timezynk.mongo.config :refer [*mongo-types*]])
  (:import [org.bson.codecs Codec]
           [org.bson.codecs.configuration CodecProvider]))

(deftype DistinctCodec [registry]
  Codec
  (decode [_this reader decoder-context]
    (.decode (->> (.getCurrentBsonType reader)
                  (get *mongo-types*)
                  (.get registry))
             reader
             decoder-context))

  (encode [_this writer value encoder-context]
    (.encode (->> (type value)
                  (.get registry))
             writer
             value
             encoder-context))

  (getEncoderClass [_this]
    DistinctCodec))

(defn distinct-provider []
  (reify CodecProvider
    (get [_this clazz registry]
      (when (= DistinctCodec clazz)
        (->DistinctCodec registry)))))

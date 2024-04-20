(ns ^:no-doc com.timezynk.mongo.codecs.int64
  (:import [org.bson.codecs Codec]))

(defn int64-codec []
  (reify Codec
    (decode [_this reader _decoder-context]
      (.readInt64 reader))

    (encode [_this writer value _encoder-context]
      (.writeInt64 writer value))

    (getEncoderClass [_this]
      Long)))

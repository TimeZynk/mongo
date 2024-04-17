(ns ^:no-doc com.timezynk.mongo.codecs.int32
  (:import [org.bson.codecs Codec]))

(defn int32-codec []
  (reify Codec
    (decode [_this reader _decoder-context]
      (long (.readInt32 reader)))

    (encode [_this writer value _encoder-context]
      (.writeInt64 writer (long value)))

    (getEncoderClass [_this]
      Integer)))

(ns ^:no-doc com.timezynk.mongo.codecs.string
  (:import [org.bson.codecs Codec]))

(defn string-codec []
  (reify Codec
    (decode [_this reader _decoder-context]
      (.readString reader))

    (encode [_this writer value _encoder-context]
      (.writeString writer value))

    (getEncoderClass [_this]
      String)))

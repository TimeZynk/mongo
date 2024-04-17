(ns ^:no-doc com.timezynk.mongo.codecs.double
  (:import [org.bson.codecs Codec]))

(defn double-codec []
  (reify Codec
    (decode [_this reader _decoder-context]
      (.readDouble reader))

    (encode [_this writer value _encoder-context]
      (.writeDouble writer value))

    (getEncoderClass [_this]
      Double)))

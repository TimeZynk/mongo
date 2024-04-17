(ns ^:no-doc com.timezynk.mongo.codecs.boolean
  (:import [org.bson.codecs Codec]))

(defn boolean-codec []
  (reify Codec
    (decode [_this reader _decoder-context]
      (.readBoolean reader))

    (encode [_this writer value _encoder-context]
      (.writeBoolean writer value))

    (getEncoderClass [_this]
      Boolean)))

(ns ^:no-doc com.timezynk.mongo.codecs.undefined
  (:import [org.bson BsonUndefined]
           [org.bson.codecs Codec]))

(defn undefined-codec []
  (reify Codec
    (decode [_this reader _decoder-context]
      (.readUndefined reader))

    (encode [_this writer _value _encoder-context]
      (.writeNull writer))

    (getEncoderClass [_this]
      BsonUndefined)))

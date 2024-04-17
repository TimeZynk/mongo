(ns ^:no-doc com.timezynk.mongo.codecs.timestamp
  (:import [org.bson.types BSONTimestamp]
           [org.bson.codecs Codec]))

(defn timestamp-codec []
  (reify Codec
    (decode [_this reader _decoder-context]
            (.readTimestamp reader))

    (encode [_this writer value _encoder-context]
      (.writeTimestamp writer value))

    (getEncoderClass [_this]
      BSONTimestamp)))

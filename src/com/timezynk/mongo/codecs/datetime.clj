(ns ^:no-doc com.timezynk.mongo.codecs.datetime
  (:import [java.util Date]
           [org.bson.codecs Codec]))

(defn datetime-codec []
  (reify Codec
    (decode [_this reader _decoder-context]
      (Date. (.readDateTime reader)))

    (encode [_this writer value _encoder-context]
      (.writeDateTime writer (.getTime ^Date value)))

    (getEncoderClass [_this]
      Date)))

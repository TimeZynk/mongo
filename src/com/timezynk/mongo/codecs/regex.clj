(ns ^:no-doc com.timezynk.mongo.codecs.regex
  (:import [java.util.regex Pattern]
           [org.bson BsonRegularExpression]
           [org.bson.codecs Codec]))

(defn regex-codec []
  (reify Codec
    (decode [_this reader _decoder-context]
      (->> (.readRegularExpression reader)
           (.getPattern)
           (Pattern/compile)))

    (encode [_this writer value _encoder-context]
      (->> (.pattern value)
           (BsonRegularExpression.)
           (.writeRegularExpression writer)))

    (getEncoderClass [_this]
      Pattern)))

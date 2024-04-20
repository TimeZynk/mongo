(ns ^:no-doc com.timezynk.mongo.codecs.regex
  (:import [java.util.regex Pattern]
           [org.bson BsonRegularExpression]
           [org.bson.codecs Codec]))

(defn regex-codec []
  (reify Codec
    (decode [_this reader _decoder-context]
      (.readString (Pattern/compile (.getPattern reader))))

    (encode [_this writer value _encoder-context]
      (.writeRegularExpression writer (BsonRegularExpression. (.pattern value))))

    (getEncoderClass [_this]
      Pattern)))

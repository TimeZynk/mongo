(ns ^:no-doc com.timezynk.mongo.codecs.decimal
  (:import [org.bson.codecs Codec]
           [org.bson.types Decimal128]))

(defn decimal-codec []
  (reify Codec
    (decode [_this reader _decoder-context]
      (.bigDecimalValue (.readDecimal128 reader)))

    (encode [_this writer value _encoder-context]
      (.writeDecimal128 writer (Decimal128. value)))

    (getEncoderClass [_this]
      BigDecimal)))

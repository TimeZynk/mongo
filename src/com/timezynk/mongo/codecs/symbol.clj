(ns ^:no-doc com.timezynk.mongo.codecs.symbol
  (:import [clojure.lang Symbol]
           [org.bson.codecs Codec]))

(defn symbol-codec []
  (reify Codec
    (decode [_this reader _decoder-context]
      (symbol (.readSymbol reader)))

    (encode [_this writer value _encoder-context]
      (.writeSymbol writer (str value)))

    (getEncoderClass [_this]
      Symbol)))

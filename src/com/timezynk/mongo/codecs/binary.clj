(ns ^:no-doc com.timezynk.mongo.codecs.binary
  (:import [org.bson.codecs Codec]
           [org.bson.types Binary]))

(defn binary-codec []
  (reify Codec
    (decode [_this reader _decoder-context]
      (let [bin (.readBinaryData reader)]
        (case (.getType bin)
          4 (.asUuid bin)
          (.getData bin))))

    (encode [_this writer value _encoder-context]
      (.writeBinaryData writer value))

    (getEncoderClass [_this]
      Binary)))

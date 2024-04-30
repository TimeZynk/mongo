(ns ^:no-doc com.timezynk.mongo.codecs.uuid
  (:import [java.util UUID]
           [org.bson BsonBinary]
           [org.bson.codecs Codec]))

(defn uuid-codec []
  (reify Codec
    ; Not used
    (decode [_this reader _decoder-context]
      (-> (.readBinaryData reader)
          (.asUuid)))

    (encode [_this writer value _encoder-context]
      (->> (BsonBinary. value)
           (.writeBinaryData writer)))

    (getEncoderClass [_this]
      UUID)))

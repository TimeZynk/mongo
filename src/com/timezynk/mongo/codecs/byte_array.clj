(ns ^:no-doc com.timezynk.mongo.codecs.byte-array
  (:import [org.bson BsonBinary]
           [org.bson.codecs Codec]))

(defmacro type-byte-array []
  (type (byte-array [])))

(defn byte-array-codec []
  (reify Codec
    ; Not used
    (decode [_this reader _decoder-context]
      (-> (.readBinaryData reader)
          (.getData)))

    (encode [_this writer value _encoder-context]
      (->> (BsonBinary. value)
           (.writeBinaryData writer)))

    (getEncoderClass [_this]
      (type-byte-array))))

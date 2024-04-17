(ns ^:no-doc com.timezynk.mongo.codecs.object-id
  (:import [org.bson.codecs Codec]
           [org.bson.types ObjectId]))

(defn object-id-codec []
  (reify Codec
    (decode [_this reader _decoder-context]
      (.readObjectId reader))

    (encode [_this writer value _encoder-context]
      (.writeObjectId writer value))

    (getEncoderClass [_this]
      ObjectId)))

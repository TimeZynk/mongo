(ns ^:no-doc com.timezynk.mongo.codecs.keyword
  (:import [clojure.lang Keyword]
           [org.bson.codecs Codec]))

(defn keyword->str [k]
  (let [v-ns (namespace k)]
    (str v-ns (when v-ns "/") (name k))))

(defn keyword-codec []
  (reify Codec
    (decode [_this reader _decoder-context]
      (keyword (.readString reader)))

    (encode [_this writer value _encoder-context]
      (->> (keyword->str value)
           (.writeString writer)))

    (getEncoderClass [_this]
      Keyword)))

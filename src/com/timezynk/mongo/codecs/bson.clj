(ns ^:no-doc com.timezynk.mongo.codecs.bson
  (:import [org.bson BsonDocumentWrapper]
           [org.bson.conversions Bson]))

(defn ->bson [query]
  (reify Bson
    (toBsonDocument [_this _clazz registry]
      (->> (type query)
           (.get registry)
           (BsonDocumentWrapper. query)))))

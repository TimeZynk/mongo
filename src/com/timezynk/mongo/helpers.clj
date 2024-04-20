(ns ^:no-doc com.timezynk.mongo.helpers
  (:require
   [com.timezynk.mongo.codecs.collection :refer [collection-provider]]
   [com.timezynk.mongo.codecs.map :refer [map-provider]]
   [com.timezynk.mongo.config :refer [*mongo-database*]]
   )
  (:import [com.mongodb.client MongoCollection]
           [com.mongodb.client.result UpdateResult$AcknowledgedUpdateResult UpdateResult$UnacknowledgedUpdateResult]
           [org.bson.codecs.configuration CodecRegistries]))

(defn codec-registry [database codecs]
  (.withCodecRegistry database
                      (CodecRegistries/fromRegistries
                       [(CodecRegistries/fromProviders [(collection-provider)
                                                        (map-provider)])
                        (CodecRegistries/fromCodecs codecs)])))

(defmacro get-collection ^MongoCollection [coll]
  `(.getCollection *mongo-database*
                   (name ~coll)
                   clojure.lang.PersistentArrayMap))

#_(defprotocol ToObjectId
  (->object-id [v]))

#_(extend-protocol ToObjectId
  String
  (->object-id [s]
    (ObjectId. s))

  ObjectId
  (->object-id [o] o))

(defprotocol UpdateResult
  (update-result [result]))

(extend-protocol UpdateResult
  UpdateResult$AcknowledgedUpdateResult
  (update-result [result]
    {:matched-count  (.getMatchedCount result)
     :modified-count (.getModifiedCount result)
     :_id            (when-let [v (.getUpsertedId result)]
                       (.getValue v))
     :acknowledged   true})

  UpdateResult$UnacknowledgedUpdateResult
  (update-result [_result]
    {:acknowledged   false}))

(ns ^:no-doc com.timezynk.mongo.helpers
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.codecs.collection :refer [collection-provider]]
   [com.timezynk.mongo.codecs.map :refer [map-provider]]
   [com.timezynk.mongo.config :refer [*mongo-database*]]
   [com.timezynk.mongo.convert :refer [it->clj]]
   [com.timezynk.mongo.guards :refer [*insert-guard* *update-guard* catch-return]]
   [com.timezynk.mongo.methods.fetch :refer [fetch-method]]
   [com.timezynk.mongo.methods.fetch-and-update :refer [fetch-and-update-method fetch-and-update-options]]
   [com.timezynk.mongo.methods.insert :refer [insert-method insert-options]]
   [com.timezynk.mongo.methods.update :refer [update-method update-one-method update-options]])
  (:import [com.mongodb WriteConcern]
           [com.mongodb.client MongoCollection]
           [com.mongodb.client.result UpdateResult$AcknowledgedUpdateResult UpdateResult$UnacknowledgedUpdateResult]
           [org.bson.codecs.configuration CodecRegistries]
           [org.bson.types ObjectId]))

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

(defprotocol ToObjectId
  (->object-id [v]))

(extend-protocol ToObjectId
  String
  (->object-id [s]
    (ObjectId. s))

  ObjectId
  (->object-id [o] o))

(defmacro do-fetch [coll query options]
  `{:pre [~coll ~query]}
  `(-> (fetch-method (get-collection ~coll)
                     (->bson ~query)
                     ~options)
       (it->clj)))

(defmacro do-insert [coll docs options]
  `{:pre [~coll]}
  `(catch-return
    (*insert-guard* ~docs)
    (-> (get-collection ~coll)
        (insert-options ~options)
        (insert-method ~docs))))

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

(defn ^:no-doc do-update [coll query update {:keys [write-concern] :as options}]
  {:pre [coll query]}
  (catch-return
   (*update-guard* update)
   (-> (update-method (cond-> (get-collection coll)
                        write-concern
                        (.withWriteConcern (case write-concern
                                             :acknowledged   WriteConcern/ACKNOWLEDGED
                                             :unacknowledged WriteConcern/UNACKNOWLEDGED
                                             :journaled      WriteConcern/JOURNALED
                                             :majority       WriteConcern/MAJORITY
                                             :w1             WriteConcern/W1
                                             :w2             WriteConcern/W2
                                             :w3             WriteConcern/W3)))
                      (->bson query)
                      (->bson update)
                      (update-options options))
       (update-result))))

(defn ^:no-doc do-update-one [coll query update {:keys [write-concern] :as options}]
  {:pre [coll query]}
  (catch-return
    (*update-guard* update)
    (-> (update-one-method (cond-> (get-collection coll)
                             write-concern
                             (.withWriteConcern (case write-concern
                                                  :acknowledged   WriteConcern/ACKNOWLEDGED
                                                  :unacknowledged WriteConcern/UNACKNOWLEDGED
                                                  :journaled      WriteConcern/JOURNALED
                                                  :majority       WriteConcern/MAJORITY
                                                  :w1             WriteConcern/W1
                                                  :w2             WriteConcern/W2
                                                  :w3             WriteConcern/W3)))
                           (->bson query)
                           (->bson update)
                           (update-options options))
        (update-result))))

(defmacro do-fetch-and-update-one [coll query update options]
  `{:pre [~coll ~query]}
  `(catch-return
    (*update-guard* ~update)
    (-> (fetch-and-update-method (get-collection ~coll)
                                 (->bson ~query)
                                 (->bson ~update)
                                 (fetch-and-update-options ~options)))))

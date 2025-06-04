(ns ^:no-doc com.timezynk.mongo.methods.create-collection
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-database* *mongo-session*]]
   [com.timezynk.mongo.schema :refer [convert-schema]])
  (:import [com.mongodb.client.model ChangeStreamPreAndPostImagesOptions
            CreateCollectionOptions ValidationAction ValidationLevel ValidationOptions]))

(defn collection-options [{:keys [collation full-change? level schema validation]}]
  (-> (cond-> (CreateCollectionOptions.)
        collation    (.collation collation)
        full-change? (.changeStreamPreAndPostImagesOptions (ChangeStreamPreAndPostImagesOptions. full-change?)))
      (.validationOptions (-> (cond-> (ValidationOptions.)
                                (or (seq schema) validation)
                                (.validator (->bson (merge (convert-schema schema)
                                                           validation))))
                              (.validationLevel (case (or level :strict)
                                                  :moderate ValidationLevel/MODERATE
                                                  :off      ValidationLevel/OFF
                                                  :strict   ValidationLevel/STRICT))
                              (.validationAction ValidationAction/ERROR)))))

(defmulti create-collection-method
  (fn [_name _options]
    (some? *mongo-session*)))

(defmethod create-collection-method true [coll options]
  (.createCollection *mongo-database* *mongo-session* coll options))

(defmethod create-collection-method false [coll options]
  (.createCollection *mongo-database* coll options))

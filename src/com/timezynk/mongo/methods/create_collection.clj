(ns ^:no-doc com.timezynk.mongo.methods.create-collection
  (:require
   [com.timezynk.mongo.utils.convert :as convert]
   [com.timezynk.mongo.config :refer [*mongo-database* *mongo-session*]]
   [com.timezynk.mongo.schema :refer [convert-schema]])
  (:import
   [com.mongodb.client.model CreateCollectionOptions ValidationAction ValidationLevel ValidationOptions]))

(defn- with-options [{:keys [collation level schema validation]}]
  (cond-> (CreateCollectionOptions.)
    collation              (.collation collation)
    (or schema validation) (.validationOptions
                            (-> (ValidationOptions.)
                                (.validator (convert/clj->doc (merge (when schema
                                                                       (convert-schema schema))
                                                                     (when validation
                                                                       validation))))
                                (.validationLevel (case (or level :strict)
                                                    :moderate ValidationLevel/MODERATE
                                                    :off      ValidationLevel/OFF
                                                    :strict   ValidationLevel/STRICT))
                                (.validationAction ValidationAction/ERROR)))))

(defmulti create-collection-method
  (fn [_name options]
    {:session (some? *mongo-session*)
     :options (seq? options)}))

(defmethod create-collection-method {:session true :options false} [coll _options]
  (.createCollection *mongo-database* *mongo-session* coll))

(defmethod create-collection-method {:session true :options true} [coll options]
  (.createCollection *mongo-database* *mongo-session* coll (with-options options)))

(defmethod create-collection-method {:session false :options false} [coll _options]
  (.createCollection *mongo-database* coll))

(defmethod create-collection-method {:session false :options true} [coll options]
  (.createCollection *mongo-database* coll (with-options options)))

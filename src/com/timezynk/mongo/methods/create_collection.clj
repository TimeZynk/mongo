(ns ^:no-doc com.timezynk.mongo.methods.create-collection
  (:require
   [com.timezynk.mongo.utils.convert :as convert]
   [com.timezynk.mongo.config :refer [*mongo-config* *mongo-session*]])
  (:import [com.mongodb.client.model CreateCollectionOptions ValidationLevel ValidationOptions]))

(defn- with-options [{:keys [collation level schema validation]}]
  (cond-> (CreateCollectionOptions.)
    collation              (.collation collation)
    (or schema validation) (.validationOptions
                            (-> (ValidationOptions.)
                                (.validator
                                 (convert/clj->doc (merge (when schema {:$jsonSchema schema})
                                                          (when validation validation))))
                                (.validationLevel
                                 (case (or level :strict)
                                   :moderate ValidationLevel/MODERATE
                                   :off      ValidationLevel/OFF
                                   :strict   ValidationLevel/STRICT))))))

(defmulti create-collection-method
  (fn [_name options]
    {:session (some? *mongo-session*)
     :options (seq? options)}))

(defmethod create-collection-method {:session true :options false} [name _options]
  (.createCollection (:db *mongo-config*) *mongo-session* name))

(defmethod create-collection-method {:session true :options true} [name options]
  (.createCollection (:db *mongo-config*) *mongo-session* name (with-options options)))

(defmethod create-collection-method {:session false :options false} [name _options]
  (.createCollection (:db *mongo-config*) name))

(defmethod create-collection-method {:session false :options true} [name options]
  (.createCollection (:db *mongo-config*) name (with-options options)))

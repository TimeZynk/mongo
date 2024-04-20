(ns ^:no-doc com.timezynk.mongo.methods.modify-collection
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-database* *mongo-session*]]
   [com.timezynk.mongo.helpers :as h]
   [com.timezynk.mongo.methods.fetch :refer [fetch-method]]
   [com.timezynk.mongo.methods.list-collections :refer [list-collections-method]]
   [com.timezynk.mongo.schema :refer [convert-schema]])
  (:import [clojure.lang PersistentArrayMap]
           [com.mongodb MongoClientException MongoNamespace]))

(defn- set-name [name]
  (MongoNamespace. (.getName *mongo-database*)
                   (clojure.core/name name)))

(defmulti rename-collection-method
  (fn [_coll _name] (some? *mongo-session*)))

(defmethod rename-collection-method true [coll name]
  (.renameCollection (h/get-collection coll) *mongo-session* (set-name name))
  name)

(defmethod rename-collection-method false [coll name]
  (.renameCollection (h/get-collection coll) (set-name name))
  name)

(defn- set-validation [coll schema validation validate?]
  (let [schema (convert-schema schema)]
    (when validate?
      (when (and schema
                 (first (fetch-method coll (->bson {:$nor [schema]}) [:limit 1])))
        (throw (MongoClientException. "Existing documents failed new schema validation")))
      (when (and validation
                 (first (fetch-method coll (->bson {:$nor [validation]}) [:limit 1])))
        (throw (MongoClientException. "Existing documents failed new custom validation"))))
    (let [validator  (as-> (list-collections-method) v
                       (filter #(= (name coll) (:name %)) v)
                       (first v)
                       (get-in v [:options :validator]))
          schema     (or schema
                         (select-keys validator [:$jsonSchema]))
          validation (or validation
                         (dissoc validator :$jsonSchema))]
      (->bson {:collMod   (name coll)
               :validator (merge schema validation)}))))

(defmulti set-validation-method
  (fn [_coll _schema _validation _validate?]
    (some? *mongo-session*)))

(defmethod set-validation-method true [coll schema validation validate?]
  (.runCommand *mongo-database* *mongo-session* (set-validation coll schema validation validate?) PersistentArrayMap)
  coll)

(defmethod set-validation-method false [coll schema validation validate?]
  (.runCommand *mongo-database* (set-validation coll schema validation validate?) PersistentArrayMap)
  coll)

(defn modify-collection-method [coll {:keys [name schema validation validate?]}]
  (cond-> coll
    name                   (rename-collection-method name)
    (or schema validation) (set-validation-method schema validation validate?)))

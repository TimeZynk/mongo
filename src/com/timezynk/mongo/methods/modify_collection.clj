(ns ^:no-doc com.timezynk.mongo.methods.modify-collection
  (:require
   [com.timezynk.mongo.config :refer [*mongo-database* *mongo-session*]]
   [com.timezynk.mongo.convert-types :refer [clj->doc it->clj]]
   [com.timezynk.mongo.helpers :as h]
   [com.timezynk.mongo.methods.list-collections :refer [list-collections-method]]
   [com.timezynk.mongo.schema :refer [convert-schema]])
  (:import [com.mongodb MongoClientException MongoNamespace]
           [org.bson Document]))

(defn- set-name [name]
  (MongoNamespace. (.getName *mongo-database*)
                   (clojure.core/name name)))

(defmulti rename-collection-method
  (fn [_coll _name] (some? *mongo-session*)))

(defmethod rename-collection-method true [coll name]
  (.renameCollection (h/get-collection coll) *mongo-session* (set-name name)))

(defmethod rename-collection-method false [coll name]
  (.renameCollection (h/get-collection coll) (set-name name)))

(defn- set-validation [coll schema validation validate?]
  (let [schema (convert-schema schema)]
    (when validate?
      (when (and schema
                 (first (h/do-fetch coll {:$nor [schema]} [:limit 1])))
        (throw (MongoClientException. "Existing documents failed new schema validation")))
      (when (and validation
                 (first (h/do-fetch coll {:$nor [validation]} [:limit 1])))
        (throw (MongoClientException. "Existing documents failed new custom validation"))))
    (let [validator  (as-> (list-collections-method *mongo-database*) v
                       (it->clj v)
                       (filter #(= (name coll) (:name %)) v)
                       (first v)
                       (get-in v [:options :validator]))
          schema     (or schema
                         (select-keys validator [:$jsonSchema]))
          validation (or validation
                         (dissoc validator :$jsonSchema))]
      (-> (Document.)
          (.append "collMod"   (name coll))
          (.append "validator" (clj->doc (merge schema validation)))))))

(defmulti set-validation-method
  (fn [_coll _schema _validation _validate?] (some? *mongo-session*)))

(defmethod set-validation-method true [coll schema validation validate?]
  (.runCommand *mongo-database* *mongo-session* (set-validation coll schema validation validate?)))

(defmethod set-validation-method false [coll schema validation validate?]
  (.runCommand *mongo-database* (set-validation coll schema validation validate?)))

(defn modify-collection-method [coll {:keys [name schema validation validate?]}]
  (cond-> coll
    name                   (rename-collection-method name)
    (or schema validation) (set-validation-method schema validation validate?)))

(ns com.timezynk.mongo.schema
  (:require
   [clojure.tools.logging :as log]))

(def ^:no-doc ^:const ID        "objectId")
(def ^:no-doc ^:const STRING    "string")
(def ^:no-doc ^:const NUMBER    "decimal")
(def ^:no-doc ^:const INTEGER   "long")
(def ^:no-doc ^:const BOOLEAN   "bool")
(def ^:no-doc ^:const DATE      "date")
(def ^:no-doc ^:const TIMESTAMP "timestamp")
(def ^:no-doc ^:const MAP       "object")
(def ^:no-doc ^:const ARRAY     "array")
(def ^:no-doc ^:const TYPES [ID STRING NUMBER INTEGER BOOLEAN DATE TIMESTAMP MAP ARRAY])

(defn- ^:no-doc set-required [{:keys [optional?]}]
  {:required (not optional?)})

(defn- ^:no-doc set-enum [{:keys [in]}]
  (when in
    {:enum in}))

(defn- ^:no-doc set-regex [{:keys [regex]}]
  (when regex
    {:pattern regex}))

(defn- ^:no-doc set-min-length [{:keys [min-length]}]
  (when min-length
    {:minLength min-length}))

(defn- ^:no-doc set-max-length [{:keys [max-length]}]
  (when max-length
    {:maxLength max-length}))

(defn- ^:no-doc set-min [{:keys [min]}]
  (when min
    {:minimum min}))

(defn- ^:no-doc set-max [{:keys [max]}]
  (when max
    {:maximum max}))

(defn id
  "Schema type `ObjectId`.
   
   | Parameter    | Description |
   | ---          | --- |
   | `:optional?` | `optional boolean` Is the field optional? |

   **Examples**
   
   ```Clojure
   (create-collection! :users :schema {:id (id :optional? true)})
   ```"
  {:arglists '([& :optional? <boolean>])}
  [& options]
  (merge {:bsonType ID}
         (set-required options)))

(defn string
  "Schema type `string`.
   
   | Parameter     | Description |
   | ---           | --- |
   | `:optional?`  | `optional boolean` Is the field optional? |
   | `:in`         | `optional seq` A list of acceptable values. |
   | `:regex`      | `optional string` A regular expression that values must pass. |
   | `:min-length` | `optional integer` Minimum length of the string. |
   | `:max-length` | `optional integer` Maximum length of the string. |

   **Examples**
   
   ```Clojure
   (create-collection! :users :schema {:name (string :optional? true)})
   ```"
  {:arglists '([& :optional? <boolean> :in [<array of accepted values>] :regex <regular expression>]
               :min-length <minimum length> :max-length <maximum length>)}
  [& options]
  (merge {:bsonType STRING}
         (set-required options)
         (set-enum options)
         (set-regex options)
         (set-min-length options)
         (set-max-length options)))

(defn number
  [& options]
  (merge {:bsonType NUMBER}
         (set-required options)
         (set-min options)
         (set-max options)))

(defn integer
  [& options]
  (merge {:bsonType INTEGER}
         (set-required options)
         (set-min options)
         (set-max options)))


(defn any
  [& options]
  (merge {:bsonType TYPES}
         (set-required options)))

(defn ^:no-doc convert-schema [schema]
  (let [required (reduce-kv (fn [l k v]
                              (if (:required v)
                                (cons (name k) l)
                                l))
                            [] schema)
        schema   (->> schema
                      (map (fn [[k v]]
                             [k (dissoc v :required)]))
                      (into {}))
        properties (merge {:_id  {:bsonType ID}}
                          schema)]
    {:$jsonSchema {:bsonType "object"
                   :required required
                   :properties properties
                   :additionalProperties false}}))

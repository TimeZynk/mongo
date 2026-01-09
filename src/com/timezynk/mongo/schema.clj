(ns com.timezynk.mongo.schema
  "Functions for defining a collection schema."
  (:refer-clojure :exclude [boolean map])
  (:require
   [clojure.core.reducers :as r]
   [com.timezynk.mongo.assert :refer [assert-keys]]
   [com.timezynk.mongo.hooks :refer [*write-hook*]])
  ;; (:import [clojure.lang PersistentArrayMap PersistentVector])
  )

(def ^:no-doc ^:const ARRAY     "array")
(def ^:no-doc ^:const BOOLEAN   "bool")
(def ^:no-doc ^:const DATETIME  "date")
(def ^:no-doc ^:const ID        "objectId")
(def ^:no-doc ^:const INTEGER   "long")
(def ^:no-doc ^:const MAP       "object")
(def ^:no-doc ^:const NULL      "null")
(def ^:no-doc ^:const NUMBER    "double")
(def ^:no-doc ^:const STRING    "string")
(def ^:no-doc ^:const TIMESTAMP "long")
; timestamp excluded, same as integer:
(def ^:no-doc ^:const TYPES [ARRAY BOOLEAN DATETIME ID INTEGER MAP NUMBER STRING])

(defn ^:no-doc set-required [{:keys [optional?]}]
  {:optional optional?})

(defn ^:no-doc set-description [{:keys [description]}]
  (when description
    {:description description}))

(defn ^:no-doc set-enum [{:keys [in]}]
  (when in
    {:enum in}))

(defn ^:no-doc set-regex [{:keys [regex]}]
  (when regex
    {:pattern regex}))

(defn ^:no-doc set-min-length [{:keys [min-length]}]
  (when min-length
    {:minLength min-length}))

(defn ^:no-doc set-max-length [{:keys [max-length]}]
  (when max-length
    {:maxLength max-length}))

(defn ^:no-doc set-min [{:keys [min]}]
  (when min
    {:minimum min}))

(defn ^:no-doc set-max [{:keys [max]}]
  (when max
    {:maximum max}))

;; (defprotocol ^:no-doc WalkHook
;;   (walk-hook [m]))

;; (extend-protocol WalkHook
;;   PersistentArrayMap
;;   (walk-hook [m]
;;     (->> (*write-hook* m)
;;          (r/map (fn [[k v]]
;;                   [k (walk-hook v)]))
;;          (into {})))

;;   PersistentVector
;;   (walk-hook [v]
;;     (mapv walk-hook v))

;;   Object
;;   (walk-hook [o] o)

;;   nil
;;   (walk-hook [_] nil))

(defn ^:no-doc convert-schema* [schema]
  (let [properties (->> schema
                        (r/map (fn [[k v]]
                                 (let [bson-type (:bsonType v)]
                                   [k (cond-> v
                                        (:optional v) (assoc :bsonType (if (sequential? bson-type)
                                                                         (conj bson-type NULL)
                                                                         [bson-type NULL]))
                                        true          (dissoc :optional))])))
                        (into {}))
        required (->> (*write-hook* schema)
                      (filter (fn [[_k v]]
                                (not (:optional v))))
                      (clojure.core/map (fn [[k _v]] k)))]
    (merge {:bsonType MAP
            :properties properties
            :additionalProperties false}
           (when (seq required)
             {:required required}))))

(defn ^:no-doc convert-schema [schema]
  (if (seq schema)
    (let [{:keys [required properties]} (convert-schema* schema)]
      {:$jsonSchema (merge {:bsonType MAP
                            :properties (merge {:_id {:bsonType ID}}
                                               properties)
                            :additionalProperties false}
                           (when required
                             {:required required}))})
    schema))

(defmacro id
  "Field must be an `ObjectId`.

   | Parameter      | Description
   | ---            | ---
   | `:optional?`   | `optional boolean` Is the field optional?
   | `:description` | `optional string` Description for validation errors.

   **Examples**

   ```clojure
   ; field-1 must be an ObjectId and can be excluded:
   (create-collection! :coll :schema {:field-1 (id :optional? true)})
   ```"
  {:added "1.0"
   :arglists '([& :optional? <boolean> :description <string>])}
  [& {:as options}]
  (assert-keys options #{:optional? :description})
  `(merge {:bsonType ID}
          (set-required ~options)
          (set-description ~options)))

(defmacro string
  "Field must be a `string`.

   | Parameter      | Description
   | ---            | ---
   | `:optional?`   | `optional boolean` Is the field optional?
   | `:description` | `optional string` Description for validation errors.
   | `:in`          | `optional seq` A collection of acceptable values.
   | `:regex`       | `optional string` A regular expression that values must pass.
   | `:min-length`  | `optional integer` Minimum length of the string.
   | `:max-length`  | `optional integer` Maximum length of the string.

   **Examples**

   ```clojure
   ; field-1 must be a string and must be included:
   (create-collection! :coll :schema {:field-1 (string)})
   ```"
  {:added "1.0"
   :arglists '([& :optional? <boolean> :description <string> :in [<array of accepted values>]
                :regex <regular expression> :min-length <minimum length> :max-length <maximum length>])}
  [& {:as options}]
  (assert-keys options #{:optional? :description :in :regex :min-length :max-length})
  `(merge {:bsonType STRING}
          (set-required ~options)
          (set-description ~options)
          (set-enum ~options)
          (set-regex ~options)
          (set-min-length ~options)
          (set-max-length ~options)))

(defmacro number
  "Field must be an 8-byte `double`.

   | Parameter      | Description
   | ---            | ---
   | `:optional?`   | `optional boolean` Is the field optional?
   | `:description` | `optional string` Description for validation errors.
   | `:in`          | `optional seq` A collection of acceptable values.
   | `:min`         | `optional integer` Minimum value.
   | `:max`         | `optional integer` Maximum value.

   **Examples**

   ```clojure
   ; field-1 must be a number and must be included:
   (create-collection! :coll :schema {:field-1 (number)})
   ```"
  {:added "1.0"
   :arglists '([& :optional? <boolean> :description <string> :in [<array of accepted values>]
                :min <minimum> :max <maximum>])}
  [& {:as options}]
  (assert-keys options #{:optional? :description :in :min :max})
  `(merge {:bsonType NUMBER}
          (set-required ~options)
          (set-description ~options)
          (set-enum ~options)
          (set-min ~options)
          (set-max ~options)))

(defmacro integer
  "Field must be an 8-byte `long integer`.

   | Parameter      | Description
   | ---            | ---
   | `:optional?`   | `optional boolean` Is the field optional?
   | `:description` | `optional string` Description for validation errors.
   | `:in`          | `optional seq` A collection of acceptable values.
   | `:min`         | `optional integer` Minimum value.
   | `:max`         | `optional integer` Maximum value.

   **Examples**

   ```clojure
   ; field-1 must be an integer and must be included:
   (create-collection! :coll :schema {:field-1 (integer)})
   ```"
  {:added "1.0"
   :arglists '([& :optional? <boolean> :description <string> :in [<array of accepted values>]
                :min <minimum> :max <maximum>])}
  [& {:as options}]
  (assert-keys options #{:optional? :description :in :min :max})
  `(merge {:bsonType INTEGER}
          (set-required ~options)
          (set-description ~options)
          (set-enum ~options)
          (set-min ~options)
          (set-max ~options)))

(defmacro timestamp
  "Field must be a `timestamp`, i.e. a positive `long integer`.

   | Parameter      | Description
   | ---            | ---
   | `:optional?`   | `optional boolean` Is the field optional?
   | `:description` | `optional string` Description for validation errors.

   **Examples**

   ```clojure
   ; field-1 must be an integer and must be included:
   (create-collection! :coll :schema {:field-1 (timestamp)})
   ```"
  {:added "1.0"
   :arglists '([& :optional? <boolean> :description <string>])}
  [& {:as options}]
  (assert-keys options #{:optional? :description})
  `(merge {:bsonType TIMESTAMP}
          (set-required ~options)
          (set-description ~options)
          (set-min {:min 0})))

(defmacro boolean
  "Field must be a `boolean`.

   | Parameter      | Description
   | ---            | ---
   | `:optional?`   | `optional boolean` Is the field optional?
   | `:description` | `optional string` Description for validation errors.

   **Examples**

   ```clojure
   ; field-1 must be a boolean and must be included:
   (create-collection! :coll :schema {:field-1 (boolean)})
   ```"
  {:added "1.0"
   :arglists '([& :optional? <boolean> :description <string>])}
  [& {:as options}]
  (assert-keys options #{:optional? :description})
  `(merge {:bsonType BOOLEAN}
          (set-required ~options)
          (set-description ~options)))

(defmacro date-time
  "Field must be a valid date-time object, default `java.util.Date`.

   | Parameter      | Description
   | ---            | ---
   | `:optional?`   | `optional boolean` Is the field optional?
   | `:description` | `optional string` Description for validation errors.

   **Examples**

   ```clojure
   ; Set field-1 to current date and time:
   (create-collection! :coll :schema {:field-1 (date-time)})
   ```"
  {:added "1.0"
   :arglists '([& :optional? <boolean> :description <string>])}
  [& {:as options}]
  (assert-keys options #{:optional? :description})
  `(merge {:bsonType DATETIME}
          (set-required ~options)
          (set-description ~options)))

(defmacro map
  "Field must be a `map`.

   | Parameter      | Description
   | ---            | ---
   | `:optional?`   | `optional boolean` Is the field optional?
   | `:description` | `optional string` Description for validation errors.

   **Examples**

   ```clojure
   ; Set field-1 to map containing two strings:
   (create-collection! :coll :schema {:field-1 (map {:str-1 (string)
                                                     :str-2 (string)})})
   ```"
  {:added "1.0"
   :arglists '([& :optional? <boolean> :description <string>])}
  [schema & {:as options}]
  (assert-keys options #{:optional? :description})
  `(merge (convert-schema* ~schema)
          (set-required ~options)
          (set-description ~options)))

(defmacro array
  "Field must be an `array`.

   | Parameter      | Description
   | ---            | ---
   | `schema`       | `fn` A call to a schema function.
   | `:optional?`   | `optional boolean` Is the field optional?
   | `:description` | `optional string` Description for validation errors.
   | `:min-length`  | `optional integer` Minimum length of the array.
   | `:max-length`  | `optional integer` Maximum length of the array.
   | `:unique?`     | `optional boolean` Whether array values must be unique.

   **Examples**

   ```clojure
   ; field-1 must be an array of strings and contain at least one element:
   (create-collection! :coll :schema {:field-1 (array (string) :min-length 1)})
   ```"
  {:added "1.0"
   :arglists '([<schema> & :optional? <boolean> :description <string>
                :min-length <minimum length> :max-length <maximum length> :unique? <boolean>])}
  [schema & {:keys [min-length max-length unique?] :as options}]
  (assert-keys options #{:optional? :description :min-length :max-length :unique?})
  `(merge {:bsonType ARRAY
           :items (dissoc ~schema :optional)}
          (set-required ~options)
          (set-description ~options)
          (when ~min-length
            {:minItems ~min-length})
          (when ~max-length
            {:maxItems ~max-length})
          (when ~unique?
            {:uniqueItems ~unique?})))

(defmacro any
  "Field can be of any type.

   | Parameter     | Description
   | ---           | ---
   | `:optional?`  | `optional boolean` Is the field optional?

   **Examples**

   ```clojure
   (create-collection! :coll :schema {:field-1 (any)})
   ```"
  {:added "1.0"
   :arglists '([& :optional? <boolean>])}
  [& {:as options}]
  (assert-keys options #{:optional?})
  `(merge {:bsonType TYPES}
          (set-required ~options)))

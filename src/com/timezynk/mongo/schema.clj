(ns com.timezynk.mongo.schema
  "Functions for defining a collection schema."
  (:refer-clojure :exclude [boolean map]))

(def ^:no-doc ^:const ID       "objectId")
(def ^:no-doc ^:const STRING   "string")
(def ^:no-doc ^:const NUMBER   "double")
(def ^:no-doc ^:const INTEGER  "long")
(def ^:no-doc ^:const BOOLEAN  "bool")
(def ^:no-doc ^:const DATETIME "date")
(def ^:no-doc ^:const MAP      "object")
(def ^:no-doc ^:const ARRAY    "array")
(def ^:no-doc ^:const TYPES [ID STRING NUMBER INTEGER BOOLEAN DATETIME MAP ARRAY])

(defn- ^:no-doc set-required [{:keys [optional?]}]
  {:optional optional?})

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

(defn- ^:no-doc convert-schema* [schema]
  (let [properties (->> (clojure.core/map (fn [[k v]] [k (dissoc v :optional)]) schema)
                        (into {}))
        required (->> (filter (fn [[_k v]] (not (:optional v))) schema)
                      (clojure.core/map (fn [[k _v]] (name k))))]
    (merge {:bsonType MAP
            :properties properties
            :additionalProperties false}
           (when (seq required)
             {:required required}))))

(defn ^:no-doc convert-schema [schema]
  (let [{:keys [required properties]} (convert-schema* schema)]
    {:$jsonSchema (merge {:bsonType MAP
                          :properties (merge {:_id {:bsonType ID}}
                                             properties)
                          :additionalProperties false}
                         (when required
                           {:required required}))}))

(defn id
  "Field must be an `ObjectId`.
   
   | Parameter    | Description |
   | ---          | --- |
   | `:optional?` | `optional boolean` Is the field optional? |

   **Examples**
   
   ```Clojure
   ; field-1 must be an ObjectId and can be excluded:
   (create-collection! :coll :schema {:field-1 (id :optional? true)})
   ```"
  {:arglists '([& :optional? <boolean>])}
  [& options]
  (merge {:bsonType ID}
         (set-required options)))

(defn string
  "Field must be a `string`.
   
   | Parameter     | Description |
   | ---           | --- |
   | `:optional?`  | `optional boolean` Is the field optional? |
   | `:in`         | `optional seq` A collection of acceptable values. |
   | `:regex`      | `optional string` A regular expression that values must pass. |
   | `:min-length` | `optional integer` Minimum length of the string. |
   | `:max-length` | `optional integer` Maximum length of the string. |

   **Examples**
   
   ```Clojure
   ; field-1 must be a string and must be included:
   (create-collection! :coll :schema {:field-1 (string)})
   ```"
  {:arglists '([& :optional? <boolean> :in [<array of accepted values>] :regex <regular expression>
                :min-length <minimum length> :max-length <maximum length>])}
  [& options]
  (merge {:bsonType STRING}
         (set-required options)
         (set-enum options)
         (set-regex options)
         (set-min-length options)
         (set-max-length options)))

(defn number
  "Field must be an 8-byte `double`.
   
   | Parameter    | Description |
   | ---          | --- |
   | `:optional?` | `optional boolean` Is the field optional? |
   | `:in`        | `optional seq` A collection of acceptable values. |
   | `:min`       | `optional integer` Minimum value. |
   | `:max`       | `optional integer` Maximum value. |

   **Examples**
   
   ```Clojure
   ; field-1 must be a number and must be included:
   (create-collection! :coll :schema {:field-1 (number)})
   ```"
  {:arglists '([& :optional? <boolean> :in [<array of accepted values>] :min <minimum> :max <maximum>])}
  [& options]
  (merge {:bsonType NUMBER}
         (set-required options)
         (set-enum options)
         (set-min options)
         (set-max options)))

(defn integer
  "Field must be an 8-byte `long integer`.
   
   | Parameter    | Description |
   | ---          | --- |
   | `:optional?` | `optional boolean` Is the field optional? |
   | `:in`        | `optional seq` A collection of acceptable values. |
   | `:min`       | `optional integer` Minimum value. |
   | `:max`       | `optional integer` Maximum value. |

   **Examples**
   
   ```Clojure
   ; field-1 must be an integer and must be included:
   (create-collection! :coll :schema {:field-1 (integer)})
   ```"
  {:arglists '([& :optional? <boolean> :in [<array of accepted values>] :min <minimum> :max <maximum>])}
  [& options]
  (merge {:bsonType INTEGER}
         (set-required options)
         (set-enum options)
         (set-min options)
         (set-max options)))

(defn boolean
  "Field must be a `boolean`.
   
   | Parameter    | Description |
   | ---          | --- |
   | `:optional?` | `optional boolean` Is the field optional? |

   **Examples**
   
   ```Clojure
   ; field-1 must be a boolean and must be included:
   (create-collection! :coll :schema {:field-1 (boolean)})
   ```"
  {:arglists '([& :optional? <boolean>])}
  [& options]
  (merge {:bsonType BOOLEAN}
         (set-required options)))

(defn date-time
  "Field must be a `java.util.Date` object.
   
   | Parameter    | Description |
   | ---          | --- |
   | `:optional?` | `optional boolean` Is the field optional? |

   **Examples**
   
   ```Clojure
   ; Set field-1 to current date and time:
   (create-collection! :coll :schema {:field-1 (date-time)})
   ```"
  {:arglists '([& :optional? <boolean>])}
  [& options]
  (merge {:bsonType DATETIME}
         (set-required options)))

(defn map
  "Field must be a `map`.
   
   | Parameter    | Description |
   | ---          | --- |
   | `:optional?` | `optional boolean` Is the field optional? |

   **Examples**
   
   ```Clojure
   ; Set field-1 to map containing two strings:
   (create-collection! :coll :schema {:field-1 (map {:str-1 (string)
                                                     :str-2 (string)})})
   ```"
  {:arglists '([& :optional? <boolean>])}
  [schema & options]
  (merge (convert-schema* schema)
         (set-required options)))

(defn array
  "Field must be an `array`.
   
   | Parameter     | Description |
   | ---           | --- |
   | `schema`      | `fn` A call to a schema function. |
   | `:optional?`  | `optional boolean` Is the field optional? |
   | `:min-length` | `optional integer` Minimum length of the array. |
   | `:max-length` | `optional integer` Maximum length of the array. |
   | `:unique?`    | `optional boolean` Whether array values must be unique. |

   **Examples**
   
   ```Clojure
   ; field-1 must be an array of strings and contain at least one element:
   (create-collection! :coll :schema {:field-1 (array (string) :min-length 1)})
   ```"
  {:arglists '([<schema> & :optional? <boolean> :min-length <minimum length> :max-length <maximum length> :unique? <boolean>])}
  [schema & {:keys [min-length max-length unique?] :as options}]
  (merge {:bsonType ARRAY
          :items (dissoc schema :optional)}
         (set-required options)
         (when min-length
           {:minItems min-length})
         (when max-length
           {:maxItems max-length})
         (when unique?
           {:uniqueItems unique?})))

(defn any
    "Field can be of any type.
   
   | Parameter     | Description |
   | ---           | --- |
   | `:optional?`  | `optional boolean` Is the field optional? |

   **Examples**
   
   ```Clojure
   (create-collection! :coll :schema {:field-1 (any)})
   ```"
  [& options]
  (merge {:bsonType TYPES}
         (set-required options)))

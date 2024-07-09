(ns ^:no-doc com.timezynk.mongo.config
  (:require
   [com.timezynk.mongo.codecs.binary     :refer [binary-codec]]
   [com.timezynk.mongo.codecs.boolean    :refer [boolean-codec]]
   [com.timezynk.mongo.codecs.byte-array :refer [byte-array-codec]]
   [com.timezynk.mongo.codecs.datetime   :refer [datetime-codec]]
   [com.timezynk.mongo.codecs.decimal    :refer [decimal-codec]]
   [com.timezynk.mongo.codecs.double     :refer [double-codec]]
   [com.timezynk.mongo.codecs.int32      :refer [int32-codec]]
   [com.timezynk.mongo.codecs.int64      :refer [int64-codec]]
   [com.timezynk.mongo.codecs.keyword    :refer [keyword-codec]]
   [com.timezynk.mongo.codecs.object-id  :refer [object-id-codec]]
   [com.timezynk.mongo.codecs.regex      :refer [regex-codec]]
   [com.timezynk.mongo.codecs.string     :refer [string-codec]]
   [com.timezynk.mongo.codecs.symbol     :refer [symbol-codec]]
   [com.timezynk.mongo.codecs.timestamp  :refer [timestamp-codec]]
   [com.timezynk.mongo.codecs.undefined  :refer [undefined-codec]]
   [com.timezynk.mongo.codecs.uuid       :refer [uuid-codec]])
  (:import [clojure.lang PersistentArrayMap PersistentVector Symbol]
           [java.util Date]
           [java.util.regex Pattern]
           [org.bson BsonType BsonUndefined]
           [org.bson.types Binary BSONTimestamp ObjectId]))

(defonce default-codecs
  [(binary-codec)
   (boolean-codec)
   (byte-array-codec)
   (datetime-codec)
   (decimal-codec)
   (double-codec)
   (int32-codec)
   (int64-codec)
   (keyword-codec)
   (object-id-codec)
   (regex-codec)
   (string-codec)
   (symbol-codec)
   (timestamp-codec)
   (undefined-codec)
   (uuid-codec)])

(defonce default-types
  {BsonType/ARRAY              PersistentVector
   BsonType/BINARY             Binary
   BsonType/BOOLEAN            Boolean
   BsonType/DATE_TIME          Date
   BsonType/DECIMAL128         BigDecimal
   BsonType/DOCUMENT           PersistentArrayMap
   BsonType/DOUBLE             Double
   BsonType/INT32              Integer
   BsonType/INT64              Long
   BsonType/OBJECT_ID          ObjectId
   BsonType/REGULAR_EXPRESSION Pattern
   BsonType/STRING             String
   BsonType/SYMBOL             Symbol
   BsonType/TIMESTAMP          BSONTimestamp
   BsonType/UNDEFINED          BsonUndefined})

(def ^:dynamic *mongo-client*   nil)
(def ^:dynamic *mongo-database* nil)
(def ^:dynamic *mongo-session*  nil)
(def ^:dynamic *mongo-codecs*   default-codecs)
(def ^:dynamic *mongo-types*    default-types)

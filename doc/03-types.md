# Type Conversion


[text](https://www.mongodb.com/docs/drivers/java/sync/current/fundamentals/data-formats/codecs/)


Clojure and MongoDB handle types differently. Converting between different types is handled using *codecs*. MongoDB types are declared internally using the `org.bson.BsonType` enum class. Each is matched to a codec that handles one or many external types:

| BsonType           | Clojure (or other) type
| ---                | ---
| ARRAY              | **PersistentVector**, LazySeq, PersistentHashSet
| BINARY             | byte[], java.util.UUID
| BOOLEAN            | Boolean
| DATE_TIME          | java.util.Date
| DECIMAL128         | java.math.BigDecimal
| DOCUMENT           | PersistentArrayMap
| DOUBLE             | Double
| INT32              | **Long**, Integer
| INT64              | Long
| OBJECT_ID          | org.bson.types.ObjectId
| REGULAR_EXPRESSION | java.util.regex.Pattern
| STRING             | String
| SYMBOL             | Symbol
| TIMESTAMP          | **Long**, org.bson.types.BSONTimestamp
| UNDEFINED          | **nil**, org.bson.BsonUndefined

Any external type not represented in the table above can be added in a custom codec, and any of the represented ones can be replaced with a different codec.

#### ARRAY

Array types in Clojure,

* LazySeq, returned from `map`, `vector`,
* PersistentHashSet, `#{}`,
* PersistentVector, `[]` `'()`,

are converted **to** array type in MongoDB.

An array retrieved **from** MongoDB is converted to PersistentVector.

#### BINARY

Handles binary data types, byte arrays and suchlike. There are different sub-types, most notably MD5-hashes and UUIDs, but the only sub-type that has a meaningful representation is UUID, which will be converted to and from `java.util.UUID`. The rest are usually represented as `bytes[]`, and will be handled as such.

#### BOOLEAN

Clojure Boolean <-> MongoDB Boolean.

#### DATE_TIME

Reimplementation of the default date-time codec.

#### DECIMAL128

BigDecimal values are natively declared in Clojure by attaching an 'M' to a floating point number, e.g. `123.456M`.

#### DOCUMENT

The base object for all documents, defined by `{}` in Clojure.

#### DOUBLE

Clojure Double <-> MongoDB Double.

#### INT32

Clojure doesn't really handle 32-bit integers, although they can be declared with `int`. They are converted to 64-bit when writing them to MongoDB.

32-bit integers are converted to `long` when read from MongoDB.

#### INT64

Clojure Long <-> MongoDB Int64.

64-bit integers, declared `long`, is the native integer format in Clojure.

#### OBJECT_ID

The native MongoDB document id object.

#### REGULAR_EXPRESSION

`java.util.regex.Pattern` is the native class when using `#""` regex declaration in Clojure.

#### STRING

Clojure String <-> MongoDB String.

#### SYMBOL

Clojure Symbol <-> MongoDB Symbol.

#### TIMESTAMP

MongoDB timestamps are only intended for internal use. Besides, Clojure doesn't have a good representation for timestamp objects. Therefore, timestamps are converted to 64-bit integers going in or out.

#### UNDEFINED

MongoDB undefined values are even more intended only for internal use, since reading an undefined value returns null. Consequently, undefined values are both written and read as nil.

## Define a custom codec

At some point you will likely want to write your own encoder, since there are many more types than the predefined ones. And maybe you want a better decoder for the internal types.

Custom codecs are defined by implementing the `org.bson.codecs.Codec` interface. It has three method declarations:

* `(decode [this reader decoder-context])` \
Reads an object from MongoDB.
* `(encode [this writer value encoder-context])` \
Writes an object to MongoDB.
* `(getEncoderClass [this])` \
Used by the codec framework to match the object to the codec.

MongoDB expects a `java.util.Date` object for storing date-time values. This class isn't very useful, so as an example let's use the much better `java.time.LocalDateTime` class instead:

```clojure
(defn localdatetime-codec []
  (reify org.bson.codecs.Codec
    (decode [_this reader _decoder-context]
      (-> (.readDatetime reader)
          (java.time.Instant/ofEpochMilli)
          (.to)))
    
    (encode [_this writer value _encoder-context]
      (->> (java.time.Instant/from value)
           (.toEpochMilli)
           (.writeDatetime writer)))
    
    (getEncoderClass [_this]
      java.time.LocalDateTime)))
```

Date-time values are read and written as epoch milli-seconds. TzMongo expects codecs in an array:

```clojure
(def new-codecs [(localdatetime-codec)])
```

Adding this codec allows TzMongo to write `java.time.LocalDateTime` objects to MongoDB, but MongoDB date-time objects will still be read back as `java.util.Date` objects, because we need to tell the codec framework how to match the objects when decoding:

```clojure
(def new-types {org.bson.BsonType/DATE_TIME java.time.LocalDateTime})
```

Then we add the codec with the binding macro `with-codecs`:

```clojure
(with-codecs new-codecs new-types
  (insert! :coll {:date-time (java.time.LocalDateTime/now)}) ; Adds current time and date
  (insert! :coll (:date-time (java.util.Date.))) ; Still works fine
  (insert! :coll (:date-time ()))) ; Don't have a codec for this one yet!
```

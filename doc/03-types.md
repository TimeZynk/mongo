# Type Conversion


[text](https://www.mongodb.com/docs/drivers/java/sync/current/fundamentals/data-formats/codecs/)


Clojure and MongoDB handle types differently. There is a protocol `ConvertTypes` that handles conversion for the standard Clojure types. It can be extended to handle any type that needs conversion. There are two functions for this:
* `clj->doc` is called for each and every value before it is sent to MongoDB,
* `doc->clj` is called for each and every value returned from MongoDB.

For example, MongoDB uses java.util.Date for storing date-time values. 

```Clojure
(def (atom time-zone (atom (java.time.ZoneId. "UTC"))))

(defn datetime-codec []
  (reify org.bson.codecs.Codec
    (decode [_this reader _decoder-context]
      (-> (.readDatetime reader)
          (java.time.Instant/ofEpochMilli)
          (.atZone @time-zone)))
    
    (encode [_this writer value _encoder-context]
      (->> (java.time.Instant/from value)
           (.toEpochMilli)
           (.writeDatetime writer)))
    
    (getEncoderClass [_this]
      java.time.ZonedDateTime)))
```

```Clojure
def new-codecs [(datetime-codec)]
```

```Clojure
def new-types {org.bson.BsonType/DATE_TIME java.time.ZonedDateTime}
```

```Clojure
(with-codecs new-codecs new-types
  <code>)
```

# Call-back Functions

There are three ways to automate interjection through call-back functions during calls:
* type conversion,
* document hooks,
* preemptive guards.


## Type Conversion

Clojure and MongoDB handle types differently. There is a protocol `ConvertTypes` that handles conversion for the standard Clojure types. It can be extended to handle any type that needs conversion. There are two functions for this:
* `clj->doc` is called for each and every value before it is sent to MongoDB,
* `doc->clj` is called for each and every value returned from MongoDB.

For example, MongoDB uses java.util.Date for storing date-time values. 


```Clojure
(require '[com.timezynk.mongo.convert-types :refer [ConvertTypes]])

(extend-protocol ConvertTypes
  java.time.ZonedDateTime
  (clj->doc [v]
    (-> (java.time.Instant/from v)
        .toEpochMilli
        java.util.Date.))

  java.util.Date
  (doc->clj [v]
    (-> (.getTime v)
        (java.time.Instant/ofEpochMilli)
        (.atZone java.time.ZoneId/systemDefault))))


```

## Hooks

Hooks allow you to make operations on documents (queries and payloads) during an API call. The main intention is to allow schema conversions to and from MongoDB.

```Clojure
(require '[com.timezynk.mongo.hooks :refer [with-hooks]])

(with-hooks {:write #(rename-keys % {:a :b})
             :read  #(rename-keys % {:b :a})}
  (fetch-and-update-one! :coll
                         {:a 1}
                         {:$set {}}))
```

## Guards

Guards allow you to add checks and operations before an API call is made. Inserts, updates, replacements each have their own guard.

Each guard has a default behaviour.

### Insert

text

### Update

text

### Replace

text

### Early Return

text

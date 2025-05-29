(ns com.timezynk.mongo.watch
  "Add call-back functions for listening to query events."
  (:require
   [com.timezynk.mongo.assert :refer [assert-keys]]
   [com.timezynk.mongo.watch-methods.delete :refer [on-delete-method]]
   [com.timezynk.mongo.watch-methods.insert :refer [on-insert-method]]
   [com.timezynk.mongo.watch-methods.replace :refer [on-replace-method]]
   [com.timezynk.mongo.watch-methods.update :refer [on-update-method]]))

(defmacro on-insert
  "Listen for an `insert!` event.
   
   | Parameter           | Description
   | ---                 | ---
   | `collection`        | `keyword/string/nil` The collection. If `nil` it is set for the database.
   | `callback function` | `function` A callback function that accepts three parameters:
   |                     | `keyword` The affected collection.
   |                     | `java.util.Date` Time of event.
   |                     | `map` The inserted document.
   | `:collation`        | `optional collation object` Collation used.
   | `:filter`           | `optional map` Filter query.
   
   **Examples**
   
   ```clojure
   (defn insert-event [coll time doc]
     ; Do something
   )

   (on-insert :coll insert-event)
   ```"
  {:added "1.0"
   :arglists '([<collection> <callback function> & :collation <collation object> :filter {}])}
  [coll insert-fn & {:as options}]
  (assert-keys options #{:collation :filter})
  `(on-insert-method ~coll ~insert-fn ~options))

(defmacro on-update
  "Listen for an `update!` event.
   
   | Parameter           | Description
   | ---                 | ---
   | `collection`        | `keyword/string/nil` The collection. If `nil` it is set for the database.
   | `callback function` | `function` A callback function that accepts three or four parameters:
   |                     | `keyword` The affected collection.
   |                     | `java.util.Date` Time of event.
   |                     | `map` Document with changed fields.
   |                     | `optional map` Old document before changes.
   | `:collation`        | `optional collation object` Collation used.
   | `:filter`           | `optional map` Filter query.
   | `:full?`            | `optional boolean` (>= v6.0) Return complete documents after and before change.
   
   **Examples**
   
   ```clojure
   (defn update-event [coll time updated-fields]
     ; Do something
   )

   (on-update :coll update-event)
   ```"
  {:added "1.0"
   :arglists '([<collection> <callback function> & :collation <collation object> :filter {} :full? <boolean>])}
  [coll update-fn & {:as options}]
  (assert-keys options #{:collation :filter :full?})
  `(on-update-method ~coll ~update-fn ~options))

(defmacro on-replace
  "Listen for a `replace!` event.
   
   | Parameter           | Description
   | ---                 | ---
   | `collection`        | `keyword/string/nil` The collection. If `nil` it is set for the database.
   | `callback function` | `function` A callback function that accepts three or four parameters:
   |                     | `java.util.Date` Time of event.
   |                     | `map` The new document.
   |                     | `optional map` The replaced document.
   | `:collation`        | `optional collation object` Collation used.
   | `:filter`           | `optional map` Filter query.
   | `:full?`            | `optional boolean` (>= v6.0) Return complete documents after and before change.
   
   **Examples**
   
   ```clojure
   (defn update-event [coll time updated-fields]
     ; Do something
   )

   (on-update :coll update-event)
   ```"
  {:added "1.0"
   :arglists '([<collection> <callback function> & :collation <collation object> :filter {} :full? <boolean>])}
  [coll replace-fn & {:as options}]
  (assert-keys options #{:collation :filter :full?})
  `(on-replace-method ~coll ~replace-fn ~options))

(defmacro on-delete
  "Listen for an `update!` event.
   
   | Parameter           | Description
   | ---                 | ---
   | `collection`        | `keyword/string/nil` The collection. If `nil` it is set for the database.
   | `callback function` | `function` A callback function that accepts three parameters:
   |                     | `java.util.Date` Time of event.
   |                     | `map` Contains either a single `_id` field with the id of the deleted document, or the entire deleted document.
   | `:full?`            | `optional boolean` (>= v6.0) Return complete deleted document.
   
   **Examples**
   
   ```clojure
   (defn delete-event [coll time id]
     ; Do something
   )

   (on-delete :coll delete-event)
   ```"
  {:added "1.0"
   :arglists '([<collection> <callback function> & :full? <boolean>])}
  [coll delete-fn & {:as options}]
  (assert-keys options #{:full?})
  `(on-delete-method ~coll ~delete-fn ~options))

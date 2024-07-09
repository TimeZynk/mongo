(ns com.timezynk.mongo.watch
  (:require
   [com.timezynk.mongo.methods.watch :refer [delete-method insert-method update-method]]))

(defn on-insert
  "Listen for an `insert!` event.
   
   | Parameter           | Description
   | ---                 | ---
   | `collection`        | `keyword/string` The collection.
   | `callback function` | `function` A callback function that accepts two parameters:
   |                     | `java.util.Date` Time of event.
   |                     | `map` The inserted document.
   
   **Examples**
   
   ```clojure
   (defn insert-event [time doc]
     ; Do something
   )

   (on-insert :coll insert-event)
   ```"
  {:arglists '([<collection> <callback function>])}
  [coll insert-fn]
  (insert-method coll insert-fn))

(defn on-update
  "Listen for an `update!` event.
   
   | Parameter           | Description
   | ---                 | ---
   | `collection`        | `keyword/string` The collection.
   | `callback function` | `function` A callback function that accepts three parameters:
   |                     | `java.util.Date` Time of event.
   |                     | `map` Contains the id of the updated document, plus the fields that were updated.
   
   **Examples**
   
   ```clojure
   (defn update-event [time updated-fields]
     ; Do something
   )

   (on-update :coll update-event)
   ```"
  {:arglists '([<collection> <callback function>])}
  [coll update-fn]
  (update-method coll update-fn))

(defn on-delete
  "Listen for an `update!` event.
   
   | Parameter           | Description
   | ---                 | ---
   | `collection`        | `keyword/string` The collection.
   | `callback function` | `function` A callback function that accepts three parameters:
   |                     | `java.util.Date` Time of event.
   |                     | `map` Contains a single `_id` field with the id of the deleted document.
   
   **Examples**
   
   ```clojure
   (defn delete-event [time id]
     ; Do something
   )

   (on-delete :coll delete-event)
   ```"
  {:arglists '([<collection> <callback function>])}
  [coll delete-fn]
  (delete-method coll delete-fn))

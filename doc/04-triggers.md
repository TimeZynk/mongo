# Triggers

Also known as *listeners* or *watchers*, these functions allow you to monitor collection activity; Each time a document is inserted, updated, or deleted, a call-back function is invoked.

```clojure
(require '[clojure.tools.logging :as log])
(require '[com.timezynk.mongo.watch :as w])

(defn insert-fn [time doc]
  (log/spy time)
  (log/spy doc))

(w/on-insert :coll insert-fn)
```

Each of the three trigger functions &ndash; `on-insert`, `on-update`, `on-delete` &ndash; accepts a call-back function that must take two parameters:

* `time` Operation time, expressed as a `java.util.Date` object.
* `doc` A map containing either:

  (on insert) The inserted document including the newly created `:_id` field.

  (on update) The `:_id` field plus the updated fields of the document.

  (on delete) Only the `:_id` field of the deleted document.

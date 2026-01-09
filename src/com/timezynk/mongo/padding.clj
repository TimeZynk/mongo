(ns com.timezynk.mongo.padding
  "Pad payload before sending to database."
  (:require
   [com.timezynk.mongo.assert :refer [assert-keys]]))

(def ^:no-doc ^:dynamic *insert-padding*  identity)
(def ^:no-doc ^:dynamic *update-padding*  identity)
(def ^:no-doc ^:dynamic *replace-padding* identity)

(defmacro with-padding
  "Optional padding of each payload document, before sending to the database.

   You may for example want to add a `created` field for timestamping each inserted
   document.

   | Parameter  | Description
   | ---        | ---
   | `:insert`  | `optional fn` Called for `insert!`.
   | `:update`  | `optional fn` Called for `update!`, `update-one!`, `fetch-and-update-one!`.
   | `:replace` | `optional fn` Called for `replace-one!`, `fetch-and-replace-one!`.
   | `body`     | Encapsulated program calling the database.

   **Examples**

   ```clojure
   ; Add `created` field:
   (with-padding {:insert (fn [doc] (assoc doc :created (System/currentTimeMillis)))}
     (insert! :coll []))
   ```"
  {:added "1.0"
   :arglists '([{:insert <insert-fn> :update <update-fn> :replace <replace-fn>} & <body>])}
  [{:keys [insert update replace] :as options} & body]
  (assert-keys options #{:insert :update :replace})
  `(binding [*insert-padding*  (or ~insert  *insert-padding*)
             *update-padding*  (or ~update  *update-padding*)
             *replace-padding* (or ~replace *replace-padding*)]
     ~@body))

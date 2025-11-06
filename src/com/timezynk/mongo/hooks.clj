(ns com.timezynk.mongo.hooks
  (:require
   [com.timezynk.mongo.assert :refer [assert-keys]]))

(def ^:no-doc ^:dynamic *read-hook*  identity)
(def ^:no-doc ^:dynamic *write-hook* identity)

(defmacro with-hooks
  "Hooks for converting documents. The <read-fn> function will be called for each document
   returned from MongoDB; the <write-fn> will be called for each document sent to MongoDB, both
   for the query and for the payload.

   The hook is applied recursively to nested documents.
   
   | Parameter | Description
   | ---       | ---
   | `:read`   | `optional fn` Called when returning a document from MongoDB.
   | `:write`  | `optional fn` Called for a query or payload to MongoDB.
   | `body`    | Encapsulated program calling the database.
   
   **Examples**
   
   ```clojure
   ; When a document is written to the database, replace the field 'name' with 'username':
   (with-hooks {:write #(clojure.set/rename-keys % {:name :username})}
     (insert! :coll {:name \"My Name\"}))
   ```"
  {:added "1.0"
   :arglists '([{:read <read-fn> :write <write-fn>} & <body>])}
  [{:keys [read write] :as options} & body]
  (assert-keys options #{:read :write})
  `(binding [*read-hook*  (or ~read  *read-hook*)
             *write-hook* (or ~write *write-hook*)]
     ~@body))

(defmacro ignore-hooks
  "If any hooks are set, ignore them.

   | Parameter | Description
   | ---       | ---
   | `body`    | Encapsulated program for which database hooks are to be ignored."
  {:added "1.0"
   :arglists '([& <body>])}
  [& body]
  `(binding [*read-hook*  identity
             *write-hook* identity]
     ~@body))

(ns com.timezynk.mongo.hooks)

(def ^:no-doc ^:dynamic *read-hook*  identity)
(def ^:no-doc ^:dynamic *write-hook* identity)

(defmacro with-hooks
  "Insert hooks for converting documents. The <read-fn> function will be called for each document
   returned from MongoDB; the <write-fn> will be called for each document sent to MongoDB, both
   for the query and for the payload.

   The hook is applied recursively to nested documents.
   
   | Parameter | Description |
   | ---       | --- |
   | `:read`   | `optional fn` Called when returning a document from MongoDB. |
   | `:write`  | `optional fn` Called for a query or payload to MongoDB. |
   | `body`    | Encapsulated program calling the database. |
   
   **Examples**
   
   ```Clojure
   ; When a document is written to the database, replace the field 'name' with 'username':
   (with-hooks {:write #(clojure.set/rename-keys % {:name :username})}
     (insert! :coll {:name \"My Name\"}))
   ```"
  {:arglists '([{:read <read-fn> :write <write-fn>} & <body>])}
  [{:keys [read write]} & body]
  `(let [read#  ~read
         write# ~write]
     (binding [*read-hook*  (or read#  *read-hook*)
               *write-hook* (or write# *write-hook*)]
       ~@body)))

(defmacro ignore-hooks
  "If any hooks are set, ignore them.

   | Parameter | Description |
   | ---       | --- |
   | `body`    | Encapsulated program for which database hooks are to be ignored. |"
  {:arglists '([& <body>])}
  [& body]
  `(binding [*read-hook*  identity
             *write-hook* identity]
     ~@body))

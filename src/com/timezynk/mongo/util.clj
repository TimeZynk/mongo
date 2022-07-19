(ns com.timezynk.mongo.util
  (:require
   [clojure.tools.logging :as log]
   [com.timezynk.mongo.methods.connection :refer [connection-method]]
   [com.timezynk.mongo.config :refer [*mongo-client* *mongo-database*]])
  (:import [com.mongodb MongoClientException]))

; ------------------------
; Persistent binding
; ------------------------

(def ^:no-doc ^:private mongo-uri (atom nil))

(defn set-mongo-uri!
  "Set connection string prior to creating a persistent binding.
   
   | Parameter | Description |
   | ---       | --- |
   | `uri`     | `string` Database location. |"
  {:arglists '([<uri>])}
  [uri & options]
  (reset! mongo-uri {:uri uri
                     :options options}))

(def ^:no-doc make-connection
  (delay (let [uri (or @mongo-uri
                       {:uri (System/getenv "MONGO_URI")}
                       (throw (MongoClientException. "Call set-mongo-uri! or define MONGO_URI environment variable prior to calling wrap-mongo")))]
           (log/info "MongoDB servers" uri)
           (connection-method (:uri uri) (:options uri)))))

(defmacro wrap-mongo
  "Functionally set up or change mongodb connection, using a persistent connection. Reverts to earlier settings when leaving scope.
   
   1. Looks for a connection string set by a prior call to `set-mongo-uri!`.
   2. Failing that, retrieves a connection string `MONGO_URI` environment variable.

   The connection string is used only once to set up the persistent connection.
   
   | Parameter | Description |
   | ---       | --- |
   | `body`    | Encapsulated program calling the database. |

   **Returns**

   The result of the last encapsulated expression.

   **Examples**
   
   ```Clojure
   (set-mongo-uri! \"mongodb://localhost:27017/my-database\")
   (wrap-mongo
    (insert! :users {:name \"My Name\"})
    (fetch! :users))
   ```"
  {:arglists '([& <body>])}
  [& body]
  `(let [client# (force make-connection)]
     (binding [*mongo-client*   (:client client#)
               *mongo-database* (:database client#)]
       ~@body)))

; ------------------------
; ObjectId
; ------------------------



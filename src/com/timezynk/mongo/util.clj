(ns com.timezynk.mongo.util
  "Convenience functions for automating certain tasks."
  (:require
   [clojure.tools.logging :as log]
   [com.timezynk.mongo.config :refer [*mongo-client* *mongo-codecs* *mongo-database*]]
   [com.timezynk.mongo.helpers :as h]
   [com.timezynk.mongo.methods.connection :refer [connection-method]]
   [com.timezynk.mongo.methods.create-collection :refer [create-collection-method]]
   [com.timezynk.mongo.methods.drop-index :refer [drop-index-method]]
   [com.timezynk.mongo.methods.modify-collection :refer [modify-collection-method]])
  (:import [clojure.lang Keyword Symbol]
           [com.mongodb MongoClientException MongoCommandException]
           [org.bson.types ObjectId]))

; ------------------------
; Persistent binding
; ------------------------

(def ^:no-doc ^:private mongo-uri (atom nil))

(defn set-mongo-uri!
  "Set connection string prior to creating a persistent binding.
   
   | Parameter | Description
   | ---       | ---
   | `uri`     | `string` Database location."
  {:added "1.0"
   :arglists '([<uri>])}
  [uri & options]
  (reset! mongo-uri {:uri uri
                     :options options}))

(def ^:no-doc ^:private connection-map
  (atom {}))

(defn ^:no-doc upsert-connection []
  (let [uri (or @mongo-uri
                {:uri (System/getenv "MONGO_URI")}
                (throw (MongoClientException. "Call set-mongo-uri! or define MONGO_URI environment variable prior to calling wrap-mongo")))]
    (or (get @connection-map uri)
        (let [connection (connection-method (:uri uri) (:options uri))]
          (log/info "MongoDB servers" uri)
          (swap! connection-map assoc uri connection)
          connection))))

(defmacro wrap-mongo
  "Functionally set up or change mongodb connection, creating a persistent connection from a previously defined connection string.
   Reverts to earlier settings when leaving scope.
   
   1. Looks for a connection string set by a prior call to `set-mongo-uri!`.
   2. Failing that, retrieves a connection string `MONGO_URI` environment variable.

   The connection string is used only once to set up the persistent connection.
   
   | Parameter | Description
   | ---       | ---
   | `body`    | Encapsulated program calling the database.

   **Returns**

   The result of the last encapsulated expression.

   **Examples**
   
   ```clojure
   (set-mongo-uri! \"mongodb://localhost:27017/my-database\")
   (wrap-mongo
     (insert! :users {:name \"My Name\"})
     (fetch! :users))
   ```"
  {:added "1.0"
   :arglists '([& <body>])}
  [& body]
  `(let [client# (upsert-connection)]
     (binding [*mongo-client*   (:client client#)
               *mongo-database* (h/codec-registry (:database client#)
                                                  *mongo-codecs*)]
       ~@body)))

(defn wrap-request
  "Wrap a request for a middleware setup
   
   | Parameter | Description
   | ---       | ---
   | `handler` | `fn` A request handler function.

   **Returns**
   
   A function that takes a `request` paramater and makes a call to `handler` with that request, inside a `wrap-mongo` call."
  {:added "1.0"
   :arglists '([<handler>])}
  [handler]
  (fn [request]
    (wrap-mongo (handler request))))

; ------------------------
; Collections
; ------------------------

(defn make-collection!
  "Try to create a collection. If it already exists, modify it."
  {:added "1.0"}
  [coll & options]
  (try
    (create-collection-method (name coll) options)
    (catch MongoCommandException e
      (if (= (.getErrorCode e) 48)
        (modify-collection-method coll options)
        (throw e)))))

(defn discard-index!
  "Try to drop an index. If it doesn't exist, ignore the exception."
  {:added "1.0"}
  [coll index]
  (try
    (drop-index-method coll index)
    (catch MongoCommandException e
      (when (not= (.getErrorCode e) 27)
        (throw e)))))

; ------------------------
; ObjectId
; ------------------------

(defprotocol ToObjectId
  "Convert to ObjectId."
  ;; {:added "1.0"}
  (->object-id [v]))

(extend-protocol ToObjectId
  Keyword
  (->object-id [k]
    (ObjectId. (name k)))

  String
  (->object-id [s]
    (ObjectId. s))

  Symbol
  (->object-id [s]
    (ObjectId. (name s)))

  ObjectId
  (->object-id [o] o)

  nil
  (->object-id [_] nil))

(defprotocol IsObjectId
  "Check if valid ObjectId or valid string."
  (object-id? [v]))

(extend-protocol IsObjectId
  String
  (object-id? [s]
    (ObjectId/isValid s))

  ObjectId
  (object-id? [_o]
    true))

; ------------------------
; String
; ------------------------

(defn random-string [size]
  (apply str (repeatedly size #(rand-nth "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"))))

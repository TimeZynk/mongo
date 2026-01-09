(ns com.timezynk.mongo.guards
  "Guards make checks of payloads before the API call proper."
  (:require
   [com.timezynk.mongo.assert :refer [assert-keys]])
  (:import [clojure.lang ExceptionInfo]))

; ------------------------
; Guard helpers
; ------------------------

(defmacro return
  "Return early without making any API call.

   **Examples**

   Ignore any insert calls with a message:

   ```clojure
   (with-guards {:insert (fn [_] (return \"a message\"))}
     (insert! :coll {:a 1})) ; No call made, just returns \"a message\"
   ```"
  {:added "1.0"}
  [result]
  `(throw (ex-info "returning early" {:result ~result})))

(defmacro ^:no-doc catch-return [& body]
  `(try
     ~@body
     (catch ExceptionInfo e#
       (-> e# ex-data :result))))

(defn bounce-empty
  "A default guard for `insert!`, `insert-one!`.
   An empty payload is allowed but does nothing."
  {:added "1.0"}
  [doc]
  (when (= [] doc)
    (return doc)))

(defn nil-update
  "A default guard for `update!`, `update-one!`, `fetch-and-update-one!`.
   Ensure consistent exception for nil."
  {:added "1.0"}
  [doc]
  (when (nil? doc)
    (throw (IllegalArgumentException. "Update can not be null"))))

(defn empty-update
  "A default guard for `update!`, `update-one!`, `fetch-and-update-one!`, `replace-one!`,
   `fetch-and-replace-one!`. An empty payload should always throw an exception."
  {:added "1.0"}
  [doc]
  (when (= [] doc)
    (throw (IllegalArgumentException. "Invalid pipeline for an update. The pipeline can not be empty."))))

; ------------------------
; Guards
; ------------------------

(def ^:no-doc ^:dynamic *insert-guard*
  bounce-empty)

(def ^:no-doc ^:dynamic *update-guard*
  (fn [doc]
    (nil-update doc)
    (empty-update doc)))

(def ^:no-doc ^:dynamic *replace-guard*
  empty-update)

(defmacro with-guards
  "Guards for API functions. They allow you to check for payload conditions before
   a call is made. If the function returns truthy, the call goes through. If it
   returns falsy, no call is made, instead the payload is returned. For serious
   errors, throw an exception.

   <insert-fn> guards `insert!`. By default, it returns false for empty lists, true otherwise.

   <update-fn> guards `update!`, `update-one!`, `fetch-and-update-one!`. By default, it will
   throw exceptions for a list payload and a root field that isn't a modifier. List
   handling is a bit wonky. Since it's not needed, it's better to not accept it.

   | Parameter  | Description
   | ---        | ---
   | `:insert`  | `optional fn` Called for `insert!`.
   | `:update`  | `optional fn` Called for `update!`, `update-one!`, `fetch-and-update-one!`.
   | `:replace` | `optional fn` Called for `replace-one!`, `fetch-and-replace-one!`.
   | `body`     | Encapsulated program calling the database.

   **Examples**

   ```clojure
   ; Remove guards for payload:
   (with-guards {:insert identity}
     (insert! :coll [])) ; Throws exception
   ```"
  {:added "1.0"
   :arglists '([{:insert <insert-fn> :update <update-fn> :replace <replace-fn>} & <body>])}
  [{:keys [insert update replace] :as options} & body]
  (assert-keys options #{:insert :update :replace})
  `(binding [*insert-guard*  (or ~insert  *insert-guard*)
             *update-guard*  (or ~update  *update-guard*)
             *replace-guard* (or ~replace *replace-guard*)]
     ~@body))

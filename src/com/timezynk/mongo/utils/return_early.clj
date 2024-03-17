(ns ^:no-doc com.timezynk.mongo.utils.return-early
  (:import [clojure.lang ExceptionInfo]))

(defmacro return [result]
  `(throw (ex-info "returning early" {:result ~result})))

(defmacro catch-return [& body]
  `(try
     ~@body
     (catch ExceptionInfo e#
       (-> e# ex-data :result))))

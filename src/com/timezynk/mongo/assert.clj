(ns ^:no-doc com.timezynk.mongo.assert
  (:require
   [clojure.set :as set])
  (:import [clojure.lang Compiler$CompilerException]))

(defn assert-keys
  "Called from inside a macro. Validates keys at compile-time."
  [check-map valid-keys]
  (let [k (set (keys check-map))]
    (when-not (set/subset? k valid-keys)
      (throw (AssertionError. (str "Function call contains invalid key: "
                                   k " => " valid-keys))))))

(defn assert-value
  "Called from inside a macro. Validates a parameter at compile-time."
  [v valid-keys]
  (and v
       (not (contains? valid-keys v))
       (throw (AssertionError. (str "Function call has invalid parameter: "
                                    v " => " valid-keys)))))

(defmacro catch-assert
  "For testing compile-time assertion.

   **Returns**

    0 <- Call went through without exceptions.
    1 <- Assertion caused compiler exception.
   -1 <- Other compiler exception or general exception."
  [body]
  (try
    (eval body)
    0
    (catch Compiler$CompilerException e
      (if (re-find #"Function call contains invalid key|Function call has invalid parameter"
                   (-> e .getCause .getMessage))
        1
        -1))
    (catch Exception _e
      -1)))

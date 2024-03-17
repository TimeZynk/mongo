(ns com.timezynk.mongo.types)

;; Define your own methods to expand type conversion
(defmulti  to-mongo
  "asdmklasd "
  (fn [v] (type v)))
(defmethod to-mongo :default [v] v)
(defmulti  from-mongo (fn [v] (type v)))
(defmethod from-mongo :default [v] v)

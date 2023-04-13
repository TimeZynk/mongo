(ns com.timezynk.mongo.test.modify-schema
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [clojure.tools.logging :as log]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.schema :as s]
   [com.timezynk.mongo.test.utils.db-utils :as dbu])
  (:import [org.bson.types ObjectId]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest modify
  (is true)
  (m/create-collection! :user :schema {:name (s/string)} :validation {:name "Boo"})
  (log/spy (m/collection-info :user))
  (m/modify-collection! :user :schema {:name (s/string)}))
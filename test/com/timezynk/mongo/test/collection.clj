(ns com.timezynk.mongo.test.collection
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [clojure.tools.logging :as log]
   [com.timezynk.mongo :as mongo]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest list-coll
  (mongo/create-collection! :users)
  (is (= [:users] (mongo/list-collection-names))))

(deftest set-collation
  (testing "Ignore whitespace and punctuation in search"
    (mongo/create-collection! :users :collation (mongo/collation "se" :alternate :shifted))
    (mongo/insert! :users {:name "12"})
    (is (= 1 (mongo/fetch-count :users {:name ".12 "})))))

(deftest collection-info
  (mongo/create-collection! :users :collation (mongo/collation "se" :alternate :shifted))
  (is (= "shifted"
         (get-in (mongo/collection-info :users)
                 [:options :collation :alternate]))))

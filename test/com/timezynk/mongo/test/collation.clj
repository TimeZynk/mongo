(ns com.timezynk.mongo.test.collation
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest set-collation
  (testing "Ignore whitespace and punctuation in search"
    (m/create-collection! :coll :collation (m/collation "se" :alternate :shifted))
    (m/insert! :coll {:name "12"})
    (is (= 1 (m/fetch-count :coll {:name ".12 "})))))

(deftest collection-info
  (m/create-collection! :coll :collation (m/collation "se" :alternate :shifted))
  (is (= "shifted"
         (get-in (m/collection-info :coll)
                 [:options :collation :alternate]))))

(deftest change-collation
  (m/insert! :coll {:name "12"})
  (is (= 0 (m/fetch-count :coll {:name ".12 "})))
  (is (= 1 (-> (m/fetch :coll {:name ".12 "} :collation (m/collation "se" :alternate :shifted))
               count))))

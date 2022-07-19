(ns com.timezynk.mongo.test.collection
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
  ;;  [clojure.tools.logging :as log]
   [com.timezynk.mongo :as mongo]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest list-coll
  (mongo/create-collection! :coll)
  (is (= [:coll] (mongo/list-collection-names))))

(deftest set-collation
  (testing "Ignore whitespace and punctuation in search"
    (mongo/create-collection! :coll :collation (mongo/collation "se" :alternate :shifted))
    (mongo/insert! :coll {:name "12"})
    (is (= 1 (mongo/fetch-count :coll {:name ".12 "})))))

(deftest collection-info
  (mongo/create-collection! :coll :collation (mongo/collation "se" :alternate :shifted))
  (is (= "shifted"
         (get-in (mongo/collection-info :coll)
                 [:options :collation :alternate]))))

(deftest change-name
  (mongo/insert! :coll {:name "A"})
  (mongo/modify-collection! :coll :name :coll-2)
  (is (nil? (mongo/fetch-one :coll)))
  (is (= "A" (:name (mongo/fetch-one :coll-2)))))

(deftest change-collation
  (mongo/insert! :coll {:name "12"})
  (is (= 0 (mongo/fetch-count :coll {:name ".12 "})))
  (is (= 1 (-> (mongo/fetch :coll {:name ".12 "} :collation (mongo/collation "se" :alternate :shifted))
               count))))

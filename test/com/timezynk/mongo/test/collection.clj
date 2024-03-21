(ns com.timezynk.mongo.test.collection
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.schema :as s]
   [com.timezynk.mongo.util :as u]
   [com.timezynk.mongo.test.utils.db-utils :as dbu])
  (:import [com.mongodb MongoCommandException]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest list-coll
  (m/create-collection! :coll)
  (is (= [:coll] (m/list-collection-names))))

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

(deftest change-name
  (m/insert! :coll {:name "A"})
  (m/modify-collection! :coll :name :coll-2)
  (is (nil? (m/fetch-one :coll)))
  (is (= "A" (:name (m/fetch-one :coll-2)))))

(deftest change-collation
  (m/insert! :coll {:name "12"})
  (is (= 0 (m/fetch-count :coll {:name ".12 "})))
  (is (= 1 (-> (m/fetch :coll {:name ".12 "} :collation (m/collation "se" :alternate :shifted))
               count))))

(deftest double-create
  (m/create-collection! :coll :schema {:name (s/string)})
  (is (thrown-with-msg? MongoCommandException
                        #"Collection already exists"
                        (m/create-collection! :coll)))
  (u/make-collection! :coll :schema {:c (s/integer)})
  (is (= "long"
         (get-in (m/collection-info :coll)
                 [:options :validator :$jsonSchema :properties :c :bsonType]))))

(ns com.timezynk.mongo.test.collection
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.schema :as s]
   [com.timezynk.mongo.util :as u]
   [com.timezynk.mongo.test.utils.db-utils :as dbu])
  (:import [com.mongodb MongoCommandException MongoWriteException]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest list-coll
  (m/create-collection! :coll)
  (is (= [:coll] (m/list-collection-names))))

(deftest default-validator
  (m/create-collection! :coll)
  (is (= {:validationLevel "strict"
          :validationAction "error"}
         (:options (m/collection-info :coll)))))

(deftest id-schema
  (m/create-collection! :coll :schema {:_id (s/string)})
  (is (= {:$jsonSchema {:bsonType "object"
                        :properties {:_id {:bsonType "string"}}
                        :additionalProperties false
                        :required ["_id"]}}
         (get-in (m/collection-info :coll) [:options :validator])))
  (is (= {:_id "1234"} (m/insert! :coll {:_id "1234"})))
  (is (thrown-with-msg? MongoWriteException
                        #"Document failed validation"
                        (m/insert! :coll {}))))

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

(deftest double-create
  (u/make-collection! :coll :schema {:name (s/string)})
  (is (thrown-with-msg? MongoCommandException
                        #"Collection already exists"
                        (m/create-collection! :coll)))
  (u/make-collection! :coll :schema {:c (s/integer)})
  (is (= "long"
         (get-in (m/collection-info :coll)
                 [:options :validator :$jsonSchema :properties :c :bsonType]))))

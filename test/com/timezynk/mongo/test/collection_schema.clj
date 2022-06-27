(ns com.timezynk.mongo.test.collection-schema
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [clojure.tools.logging :as log]
   [com.timezynk.mongo :as mongo]
   [com.timezynk.mongo.schema :as s]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest with-schema
  (mongo/create-collection! :users :schema {:name    (s/string)
                                            :address (s/string :optional? true)})
  (testing "Check correctness of schema"
    (is (= {:validator
            {:$jsonSchema
             {:bsonType "object"
              :required ["name"]
              :properties
              {:_id {:bsonType "objectId"}
               :name {:bsonType "string"}
               :address {:bsonType "string"}}
              :additionalProperties false}}
            :validationLevel "strict"}
           (:options (mongo/collection-info :users)))))
  (testing "Inserts that should pass validation"
    (try
      (mongo/insert! :users {:name "Name"})
      (mongo/insert! :users {:name "Name" :address "Address"})
      (catch Exception _e
        (is false))))
  (testing "Inserts that don't pass validation"
    (is (thrown-with-msg? Exception
                          #"Document failed validation"
                          (mongo/insert! :users {:address "Address"})))
    (is (thrown-with-msg? Exception
                          #"Document failed validation"
                          (mongo/insert! :users {:name 1})))))

(deftest enum-schema
  (mongo/create-collection! :users :schema {:name (s/string :in #{"A"})})
  (testing "Valid name"
    (try
      (mongo/insert! :users {:name "A"})
      (catch Exception _e
        (is false))))
  (testing "Invalid name"
    (is (thrown-with-msg? Exception
                          #"Document failed validation"
                          (mongo/insert! :users {:name "B"})))))

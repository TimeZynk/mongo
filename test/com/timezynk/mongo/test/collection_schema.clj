(ns com.timezynk.mongo.test.collection-schema
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.schema :as s]
   [com.timezynk.mongo.test.utils.db-utils :as dbu])
  (:import [com.mongodb MongoWriteException]
           [org.bson.types ObjectId]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest with-schema
  (m/create-collection! :users :schema {:name    (s/string)
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
            :validationLevel "strict"
            :validationAction "error"}
           (:options (m/collection-info :users)))))
  (testing "Inserts that should pass validation"
    (try
      (m/insert! :users {:name "Name"})
      (m/insert! :users {:name "Name" :address "Address"})
      (catch Exception _e
        (is false))))
  (testing "Inserts that don't pass validation"
    (is (thrown-with-msg? MongoWriteException
                          #"Document failed validation"
                          (m/insert! :users {:address "Address"})))
    (is (thrown-with-msg? MongoWriteException
                          #"Document failed validation"
                          (m/insert! :users {:name 1})))))

(deftest id-schema
  (m/create-collection! :users :schema {:id (s/id)})
  (testing "Insert valid id"
    (try
      (m/insert! :users {:id (ObjectId.)})
      (catch Exception _e
        (is false))))
  (testing "Insert invalid id"
    (is (thrown-with-msg? MongoWriteException
                          #"Document failed validation"
                          (m/insert! :users {:id "A"})))))

(deftest string-schema
  (m/create-collection! :coll-1 :schema {:field (s/string :regex "[A-Z]")})
  (m/create-collection! :coll-2 :schema {:field (s/string :in ["A"])})
  (testing "Insert string"
    (try
      (m/insert! :coll-1 {:field "S"})
      (m/insert! :coll-2 {:field "A"})
      (catch Exception _e
        (is false))))
  (testing "Insert not string"
    (is (thrown-with-msg? MongoWriteException
                          #"Document failed validation"
                          (m/insert! :coll-1 {:field 1}))))
  (testing "Fail regex"
    (is (thrown-with-msg? MongoWriteException
                          #"Document failed validation"
                          (m/insert! :coll-1 {:field "s"}))))
  (testing "Fail enum"
    (is (thrown-with-msg? MongoWriteException
                          #"Document failed validation"
                          (m/insert! :coll-2 {:field "B"})))))

(deftest number-schema
  (m/create-collection! :users :schema {:number (s/number)})
  (testing "Insert number"
    (try
      (m/insert! :users {:number 1.2})
      (catch Exception _e
        (is false))))
  (testing "Insert not number"
    (is (thrown-with-msg? MongoWriteException
                          #"Document failed validation"
                          (m/insert! :users {:number "1"})))))

(deftest integer-schema
  (m/create-collection! :users :schema {:integer (s/integer)})
  (testing "Insert integer"
    (try
      (m/insert! :users {:integer 2})
      (catch Exception _e
        (is false))))
  (testing "Insert not integer"
    (is (thrown-with-msg? MongoWriteException
                          #"Document failed validation"
                          (m/insert! :users {:integer 1.2})))))

(deftest boolean-schema
  (m/create-collection! :users :schema {:boolean (s/boolean)})
  (testing "Insert boolean"
    (try
      (m/insert! :users {:boolean true})
      (catch Exception _e
        (is false))))
  (testing "Insert not boolean"
    (is (thrown-with-msg? MongoWriteException
                          #"Document failed validation"
                          (m/insert! :users {:boolean 1})))))

(deftest date-schema
  (m/create-collection! :users :schema {:date (s/date-time)})
  (testing "Insert date-time"
    (try
      (m/insert! :users {:date (java.util.Date.)})
      (catch Exception _e
        (is false))))
  (testing "Insert not date-time"
    (is (thrown-with-msg? MongoWriteException
                          #"Document failed validation"
                          (m/insert! :users {:date "2020-01-01T10:00:00.000"})))))

(deftest map-schema
  (m/create-collection! :users :schema {:map (s/map {:first (s/string)
                                                     :last  (s/string)})})
  (testing "Insert map"
    (try
      (m/insert! :users {:map {:first "A" :last "B"}})
      (catch Exception _e
        (is false))))
  (testing "Insert not map"
    (is (thrown-with-msg? MongoWriteException
                          #"Document failed validation"
                          (m/insert! :users {:map {:first "A"}})))))

(deftest array-schema
  (m/create-collection! :coll-1 :schema {:array (s/array (s/map {:first (s/string) :last (s/string)}) :optional? true)})
  (m/create-collection! :coll-2 :schema {:array (s/array (s/string :max-length 3))})
  (m/create-collection! :coll-3 :schema {:array (s/array (s/integer) :min-length 1 :max-length 2 :unique? true)})
  (testing "Insert array"
    (try
      (m/insert! :coll-1 {:array [{:first "A" :last "B"}]})
      (m/insert! :coll-1 {:array [{:first "A" :last "B"}]})
      (m/insert! :coll-2 {:array ["A" "A"]})
      (m/insert! :coll-2 {:array []})
      (catch Exception _e
        (is false))))
  (testing "Array insert fail"
    (is (thrown-with-msg? MongoWriteException
                          #"Document failed validation"
                          (m/insert! :coll-1 {:array [{:first "A"}]})))
    (is (thrown-with-msg? MongoWriteException
                          #"Document failed validation"
                          (m/insert! :coll-2 {:array [1]})))
    (is (thrown-with-msg? MongoWriteException
                          #"Document failed validation"
                          (m/insert! :coll-2 {:array ["ABCD"]})))
    (is (thrown-with-msg? MongoWriteException
                          #"Document failed validation"
                          (m/insert! :coll-3 {:array []})))
    (is (thrown-with-msg? MongoWriteException
                          #"Document failed validation"
                          (m/insert! :coll-3 {:array [1 1]})))
    (is (thrown-with-msg? MongoWriteException
                          #"Document failed validation"
                          (m/insert! :coll-3 {:array [1 2 3]})))))

(deftest any-schema
  (m/create-collection! :users :schema {:any (s/any)})
  (is true)
  (testing "Insert any"
    (try
      (m/insert! :users {:any "1"})
      (m/insert! :users {:any 1.2})
      (m/insert! :users {:any true})
      (catch Exception _e
        (is false)))))

(deftest enum-schema
  (m/create-collection! :users :schema {:name (s/string :in #{"A"})})
  (testing "Valid name"
    (try
      (m/insert! :users {:name "A"})
      (catch Exception _e
        (is false))))
  (testing "Invalid name"
    (is (thrown-with-msg? MongoWriteException
                          #"Document failed validation"
                          (m/insert! :users {:name "B"})))))

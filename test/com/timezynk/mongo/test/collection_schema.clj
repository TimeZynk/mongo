(ns com.timezynk.mongo.test.collection-schema
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [clojure.tools.logging :as log]
   [com.timezynk.mongo :as mongo]
   [com.timezynk.mongo.schema :as s]
   [com.timezynk.mongo.test.utils.db-utils :as dbu])
  (:import [org.bson.types ObjectId]))

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

(deftest id-schema
  (mongo/create-collection! :users :schema {:id (s/id)})
  (testing "Insert valid id"
    (try
      (mongo/insert! :users {:id (ObjectId.)})
      (catch Exception _e
        (is false))))
  (testing "Insert invalid id"
    (is (thrown-with-msg? Exception
                          #"Document failed validation"
                          (mongo/insert! :users {:id "A"})))))

(deftest string-schema
  (mongo/create-collection! :coll-1 :schema {:field (s/string :regex "[A-Z]")})
  (mongo/create-collection! :coll-2 :schema {:field (s/string :in ["A"])})
  (testing "Insert string"
    (try
      (mongo/insert! :coll-1 {:field "S"})
      (mongo/insert! :coll-2 {:field "A"})
      (catch Exception _e
        (is false))))
  (testing "Insert not string"
    (is (thrown-with-msg? Exception
                          #"Document failed validation"
                          (mongo/insert! :coll-1 {:field 1}))))
  (testing "Fail regex"
    (is (thrown-with-msg? Exception
                          #"Document failed validation"
                          (mongo/insert! :coll-1 {:field "s"}))))
  (testing "Fail enum"
    (is (thrown-with-msg? Exception
                          #"Document failed validation"
                          (mongo/insert! :coll-2 {:field "B"})))))

(deftest number-schema
  (mongo/create-collection! :users :schema {:number (s/number)})
  (testing "Insert number"
    (try
      (mongo/insert! :users {:number 1.2})
      (catch Exception _e
        (is false))))
  (testing "Insert not number"
    (is (thrown-with-msg? Exception
                          #"Document failed validation"
                          (mongo/insert! :users {:number "1"})))))

(deftest integer-schema
  (mongo/create-collection! :users :schema {:integer (s/integer)})
  (testing "Insert integer"
    (try
      (mongo/insert! :users {:integer 2})
      (catch Exception _e
        (is false))))
  (testing "Insert not integer"
    (is (thrown-with-msg? Exception
                          #"Document failed validation"
                          (mongo/insert! :users {:integer 1.2})))))

(deftest boolean-schema
  (mongo/create-collection! :users :schema {:boolean (s/boolean)})
  (testing "Insert boolean"
    (try
      (mongo/insert! :users {:boolean true})
      (catch Exception _e
        (is false))))
  (testing "Insert not boolean"
    (is (thrown-with-msg? Exception
                          #"Document failed validation"
                          (mongo/insert! :users {:boolean 1})))))

(deftest date-schema
  (mongo/create-collection! :users :schema {:date (s/date-time)})
  (testing "Insert date-time"
    (try
      (mongo/insert! :users {:date (java.util.Date.)})
      (catch Exception _e
        (is false))))
  (testing "Insert not date-time"
    (is (thrown-with-msg? Exception
                          #"Document failed validation"
                          (mongo/insert! :users {:date "2020-01-01T10:00:00.000"})))))

(deftest map-schema
  (mongo/create-collection! :users :schema {:map (s/map {:first (s/string)
                                                         :last  (s/string)})})
  (testing "Insert map"
    (try
      (mongo/insert! :users {:map {:first "A" :last "B"}})
      (catch Exception _e
        (is false))))
  (testing "Insert not map"
    (is (thrown-with-msg? Exception
                          #"Document failed validation"
                          (mongo/insert! :users {:map {:first "A"}})))))

(deftest array-schema
  (mongo/create-collection! :coll-1 :schema {:array (s/array (s/map {:first (s/string) :last (s/string)}) :optional? true)})
  (mongo/create-collection! :coll-2 :schema {:array (s/array (s/string :max-length 3))})
  (mongo/create-collection! :coll-3 :schema {:array (s/array (s/integer) :min-length 1 :max-length 2 :unique? true)})
  (testing "Insert array"
    (try
      (mongo/insert! :coll-1 {:array [{:first "A" :last "B"}]})
      (mongo/insert! :coll-1 {:array [{:first "A" :last "B"}]})
      (mongo/insert! :coll-2 {:array ["A" "A"]})
      (mongo/insert! :coll-2 {:array []})
      (catch Exception e
        (log/spy e)
        (is false))))
  (testing "Array insert fail"
    (is (thrown-with-msg? Exception
                          #"Document failed validation"
                          (mongo/insert! :coll-1 {:array [{:first "A"}]})))
    (is (thrown-with-msg? Exception
                          #"Document failed validation"
                          (mongo/insert! :coll-2 {:array [1]})))
    (is (thrown-with-msg? Exception
                          #"Document failed validation"
                          (mongo/insert! :coll-2 {:array ["ABCD"]})))
    (is (thrown-with-msg? Exception
                          #"Document failed validation"
                          (mongo/insert! :coll-3 {:array []})))
    (is (thrown-with-msg? Exception
                          #"Document failed validation"
                          (mongo/insert! :coll-3 {:array [1 1]})))
    (is (thrown-with-msg? Exception
                          #"Document failed validation"
                          (mongo/insert! :coll-3 {:array [1 2 3]})))))

(deftest any-schema
  (mongo/create-collection! :users :schema {:any (s/any)})
  (is true)
  (testing "Insert any"
    (try
      (mongo/insert! :users {:any "1"})
      (mongo/insert! :users {:any 1.2})
      (mongo/insert! :users {:any true})
      (catch Exception _e
        (is false)))))

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

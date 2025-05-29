(ns com.timezynk.mongo.test.collection
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.schema :as s]
   [com.timezynk.mongo.test.utils.db-utils :as dbu])
  (:import [com.mongodb MongoClientException MongoCommandException MongoWriteException]))

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

(deftest empty-id
  (testing "nil _id allowed for schema-less"
    ; Must be unacknowledged. Returning nil _id throws.
    (m/with-write-concern :unacknowledged
      (is (= {:_id nil} (m/insert! :coll {:_id nil}))))
    (is (= [{:_id nil}] (m/fetch :coll))))
  (testing "Modify schema prohibits nil _id"
    (m/delete! :coll {})
    (m/modify-collection! :coll :schema {:a (s/integer)})
    (is (thrown-with-msg? MongoWriteException
                          #"\"propertyName\"\: \"_id\".+type did not match"
                          (m/insert! :coll {:_id nil :a 1}))))
  (testing "Create with schema prohibits nil _id"
    (m/drop-collection! :coll)
    (m/create-collection! :coll :schema {:a (s/integer)})
    (is (thrown-with-msg? MongoWriteException
                          #"\"propertyName\"\: \"_id\".+type did not match"
                          (m/insert! :coll {:_id nil :a 1}))))
  (testing "Unacknowledged does not throw, but still prevents insert"
    (is (= {:_id nil :a 2}
           (m/with-write-concern :unacknowledged
             (m/insert! :coll {:_id nil :a 2}))))
    (is (= [] (m/fetch :coll))))
  (testing "Set _id to optional to allow nil value"
    (m/modify-collection! :coll :schema {:_id (s/id :optional? true)
                                         :a (s/integer)})
    (is (= {:_id nil :a 3}
           (m/with-write-concern :unacknowledged
             (m/insert! :coll {:_id nil :a 3}))))
    (is (= [{:_id nil :a 3}]
           (m/fetch :coll)))))

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

(deftest double-create
  (m/create-collection! :coll :schema {:name (s/string)})
  (is (thrown-with-msg? MongoCommandException
                        #""
                        (m/create-collection! :coll)))
  (m/modify-collection! :coll :schema {:c (s/integer)})
  (is (= "long"
         (get-in (m/collection-info :coll)
                 [:options :validator :$jsonSchema :properties :c :bsonType]))))

(deftest rename-collection
  (m/insert! :coll {:name "A"})
  (m/modify-collection! :coll :name :coll-2)
  (is (nil? (m/fetch-one :coll)))
  (is (= "A" (:name (m/fetch-one :coll-2)))))

(deftest modify-id
  (testing "Schema change of id"
    (m/create-collection! :coll)
    (m/modify-collection! :coll :schema {:_id (s/string)})
    (is (= {:$jsonSchema {:bsonType "object"
                          :properties {:_id {:bsonType "string"}}
                          :additionalProperties false
                          :required ["_id"]}}
           (get-in (m/collection-info :coll) [:options :validator])))
    (m/modify-collection! :coll :schema {:_id (s/integer)})
    (is (= {:$jsonSchema {:bsonType "object"
                          :properties {:_id {:bsonType "long"}}
                          :additionalProperties false
                          :required ["_id"]}}
           (get-in (m/collection-info :coll) [:options :validator])))))

(deftest validate
  (m/create-collection! :coll)
  (m/insert! :coll {:a 1})
  (testing "Fail set new schema"
    (is (thrown-with-msg? MongoClientException
                          #"Existing documents failed new schema validation"
                          (m/modify-collection! :coll :schema {:a (s/string)} :validate? true))))
  (testing "Fail set new validation"
    (is (thrown-with-msg? MongoClientException
                          #"Existing documents failed new custom validation"
                          (m/modify-collection! :coll :validation {:a 2} :validate? true))))
  (testing "Set new schema and validation without check"
    (m/modify-collection! :coll :schema {:a (s/string)} :validation {:a 2})
    (is (= {:validator {:$jsonSchema {:bsonType "object"
                                      :properties {:_id {:bsonType "objectId"}
                                                   :a {:bsonType "string"}}
                                      :additionalProperties false
                                      :required ["a"]}
                        :a 2}
            :validationLevel "strict"
            :validationAction "error"}
           (:options (m/collection-info :coll)))))
  (testing "Nil schema and validation does nothing"
    (m/modify-collection! :coll :schema nil :validation nil)
    (is (= {:validator {:$jsonSchema {:bsonType "object"
                                      :properties {:_id {:bsonType "objectId"}
                                                   :a {:bsonType "string"}}
                                      :additionalProperties false
                                      :required ["a"]}
                        :a 2}
            :validationLevel "strict"
            :validationAction "error"}
           (:options (m/collection-info :coll)))))
  (testing "Remove schema and validation"
    (m/modify-collection! :coll :schema {} :validation {})
    (is (= {:validationLevel "strict"
            :validationAction "error"}
           (:options (m/collection-info :coll))))))

(deftest modify-validator
  (testing "Create schema, change schema"
    (m/create-collection! :user-1 :schema {:name (s/string)})
    (m/modify-collection! :user-1 :schema {:address (s/string)})
    (is (= {:$jsonSchema {:bsonType "object"
                          :properties {:_id     {:bsonType "objectId"}
                                       :address {:bsonType "string"}}
                          :additionalProperties false
                          :required ["address"]}}
           (get-in (m/collection-info :user-1) [:options :validator]))))
  (testing "Create validation, change validation"
    (m/create-collection! :user-2 :validation {:name "Boo"})
    (m/modify-collection! :user-2 :validation {:name "Baa"})
    (is (= {:name "Baa"}
           (get-in (m/collection-info :user-2) [:options :validator]))))
  (testing "Change validation, keep schema"
    (m/modify-collection! :user-1 :validation {:name "Boo"})
    (is (= {:$jsonSchema {:bsonType   "object"
                          :properties {:_id     {:bsonType "objectId"}
                                       :address {:bsonType "string"}}
                          :additionalProperties false
                          :required ["address"]}
            :name "Boo"}
           (get-in (m/collection-info :user-1) [:options :validator]))))
  (testing "Change schema, keep validation"
    (m/modify-collection! :user-2 :schema {:name (s/string)})
    (is (= {:$jsonSchema {:bsonType   "object"
                          :properties {:_id  {:bsonType "objectId"}
                                       :name {:bsonType "string"}}
                          :additionalProperties false
                          :required ["name"]}
            :name "Baa"}
           (get-in (m/collection-info :user-2) [:options :validator])))))

(ns com.timezynk.mongo.test.modify-schema
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.schema :as s]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest check-validation
  (m/create-collection! :user
                        :schema {:name (s/string)}
                        :validation {:name "Boo"})
  (is (= {:options {:validator {:$jsonSchema {:bsonType "object"
                                              :properties {:_id  {:bsonType "objectId"}
                                                           :name {:bsonType "string"}}
                                              :additionalProperties false
                                              :required ["name"]}
                                :name "Boo"}
                    :validationLevel "strict"
                    :validationAction "error"}}
         (-> (m/collection-info :user)
             (select-keys [:options]))))
  (m/modify-collection! :user :schema {:name (s/string)})
  (is (= {:options {:validator {:$jsonSchema {:bsonType "object"
                                              :properties
                                              {:_id {:bsonType "objectId"}
                                               :name {:bsonType "string"}}
                                              :additionalProperties false
                                              :required ["name"]}}
                    :validationLevel "strict"
                    :validationAction "error"}}
         (-> (m/collection-info :user)
             (select-keys [:options])))))

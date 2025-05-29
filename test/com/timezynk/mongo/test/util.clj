(ns com.timezynk.mongo.test.util
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.timezynk.mongo.util :as mu])
  (:import [org.bson.types ObjectId]))

(deftest to-object-id
  (let [object-id (ObjectId.)
        id-str    (str object-id)]
    (testing "Converting nil returns nil"
      (is (nil? (mu/->object-id nil))))
    (testing "Convert string"
      (is (= object-id
             (mu/->object-id id-str))))
    (testing "Convert keyword"
      (is (= object-id
             (mu/->object-id (keyword id-str)))))
    (testing "Convert symbol"
      (is (= object-id
             (mu/->object-id (symbol id-str)))))))

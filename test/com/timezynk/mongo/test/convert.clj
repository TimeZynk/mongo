(ns com.timezynk.mongo.test.convert
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.timezynk.mongo.utils.convert :refer [clj->doc doc->clj]])
  (:import [clojure.lang PersistentArrayMap PersistentVector]
           [java.lang String]
           [java.util ArrayList]
           [org.bson BsonString Document]))

(deftest clj->doc-tests
  (testing "Can convert keyword"
    (is (= "testword" (clj->doc :testword))))
  (testing "Can convert keyword with slash"
    (is (= "my/testword" (clj->doc :my/testword))))
  (testing "Can convert map with keywords with slash"
    (is (= {"my/testword" 1} (clj->doc {:my/testword 1}))))
  (testing "Can save map with string keys"
    (is (= {"my/testword" 1} (clj->doc {"my/testword" 1})))))

(deftest doc->clj-tests
  (testing "Can convert keyword"
    (is (= {:testword 1} (doc->clj (.append (Document.) "testword" 1)))))
  (testing "Can convert keyword with slash"
    (is (= {:my/testword 1} (doc->clj (.append (Document.) "my/testword" 1)))))
  (testing "Can convert keyword with double slashes"
    (is (= {(keyword "my/test/word") 1} (doc->clj (.append (Document.) "my/test/word" 1)))))
  (testing "Can convert ArrayLists"
    (let [list (doc->clj (ArrayList. [(.append (Document.) "a" 1)
                                      (BsonString. "ABC")]))]
      (is (= PersistentVector (type list)))
      (is (= [{:a 1} "ABC"] list))
      (is (= [PersistentArrayMap String] (map type list))))))

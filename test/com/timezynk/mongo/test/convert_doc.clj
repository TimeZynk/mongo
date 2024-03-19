(ns com.timezynk.mongo.test.convert-doc
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.timezynk.mongo.convert-types :refer [clj->doc doc->clj]])
  (:import [clojure.lang PersistentArrayMap PersistentVector]
           [java.lang String]
           [java.util ArrayList]
           [org.bson BsonString Document]))

(deftest clj-doc
  (testing "Can convert keyword"
    (is (= "testword"
           (clj->doc :testword))))
  (testing "Can convert keyword with slash"
    (is (= "my/testword"
           (clj->doc :my/testword))))
  (testing "Can convert map with keywords with slash"
    (let [doc (clj->doc {:my/testword 1})]
      (is (= Document (type doc)))
      (is (= {"my/testword" 1}
             doc))))
  (testing "Can save map with string keys"
    (is (= {"my/testword" 1}
           (clj->doc {"my/testword" 1}))))
  (testing "List"
    (let [doc (clj->doc [{:a 1} {:b 2}])]
      (is (= PersistentVector (type doc)))
      (is (= [{"a" 1} {"b" 2}]
             doc)))))

(deftest doc-clj
  (testing "Can convert keyword"
    (is (= {:testword 1}
           (doc->clj (.append (Document.) "testword" 1)))))
  (testing "Can convert keyword with slash"
    (is (= {:my/testword 1}
           (doc->clj (.append (Document.) "my/testword" 1)))))
  (testing "Can convert keyword with double slashes"
    (is (= {(keyword "my/test/word") 1}
           (doc->clj (.append (Document.) "my/test/word" 1)))))
  (testing "Can convert ArrayLists"
    (let [list (doc->clj (ArrayList. [(.append (Document.) "a" 1)
                                      (BsonString. "ABC")]))]
      (is (= PersistentVector (type list)))
      (is (= [{:a 1} "ABC"] list))
      (is (= [PersistentArrayMap String] (map type list))))))

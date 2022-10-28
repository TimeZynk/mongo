(ns com.timezynk.mongo.test.convert
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.timezynk.mongo.utils.convert :refer [clj->doc doc->clj]]))

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
    (is (= {:testword 1} (doc->clj (doto (org.bson.Document.) (.append "testword" 1))))))
  (testing "Can convert keyword with slash"
    (is (= {:my/testword 1} (doc->clj (doto (org.bson.Document.) (.append "my/testword" 1))))))
  (testing "Can convert keyword with double slashes"
    (is (= {(keyword "my/test/word") 1} (doc->clj (doto (org.bson.Document.) (.append "my/test/word" 1)))))))

(ns com.timezynk.mongo.test.connection
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.timezynk.mongo :as m]))

(deftest check-uri
  (testing "uri is null"
    (is (thrown-with-msg? AssertionError
                          #""
                          (m/create-connection! nil))))
  )

(ns com.timezynk.mongo.test.files.rename
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [com.timezynk.mongo.files :as mf]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest rename-by-name
  (mf/upload! :bucket (.getBytes "A") "file-1")
  (mf/upload! :bucket (.getBytes "AB") "file-1")
  (mf/upload! :bucket (.getBytes "ABC") "file-2")
  (mf/rename! :bucket "file-1" "file-3")
  (is (= #{{:length 1
            :filename "file-3"}
           {:length 2
            :filename "file-3"}
           {:length 3
            :filename "file-2"}}
         (->> (mf/info :bucket {})
              (map #(select-keys % [:filename :length]))
              (set)))))

(deftest rename-by-id
  (mf/upload! :bucket (.getBytes "A") "file-1")
  (let [id (mf/upload! :bucket (.getBytes "AB") "file-1")]
    (mf/rename-by-id! :bucket id "file-2")
    (is (= #{{:length 1
              :filename "file-1"}
             {:length 2
              :filename "file-2"}}
           (->> (mf/info :bucket {})
                (map #(select-keys % [:filename :length]))
                (set))))))

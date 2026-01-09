(ns com.timezynk.mongo.test.files.delete
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [com.timezynk.mongo.files :as mf]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest delete-by-name
  (mf/upload! :bucket (.getBytes "A") "file-1")
  (mf/upload! :bucket (.getBytes "AB") "file-1")
  (mf/upload! :bucket (.getBytes "ABC") "file-2")
  (mf/delete! :bucket "file-1")
  (is (= ["file-2"]
         (->> (mf/info :bucket {})
              (map :filename)))))

(deftest delete-by-id
  (let [id-1 (mf/upload! :bucket (.getBytes "A") "file-1")
        id-2 (mf/upload! :bucket (.getBytes "AB") "file-1")]
    (mf/delete-by-id! :bucket id-2)
    (is (= [id-1]
           (->> (mf/info :bucket {})
                (map :_id))))))

(deftest delete-by-query
  (mf/upload! :bucket (.getBytes "A") "file-1")
  (mf/upload! :bucket (.getBytes "AB") "file-2")
  (mf/upload! :bucket (.getBytes "ABC") "file-3")
  (mf/upload! :bucket (.getBytes "ABCD") "file-4")
  (is (= {:deleted-count 2}
         (mf/delete-by-query! :bucket {:$or [{:length 1}
                                             {:length 2}]})))
  (is (= #{"file-3" "file-4"}
         (->> (mf/info :bucket {})
              (map :filename)
              (set))))
  (mf/delete-by-query! :bucket {:upload-date {:$lt (-> (mf/info :bucket {:filename "file-4"})
                                                       (first)
                                                       (:upload-date))}})
  (is (= ["file-4"]
         (->> (mf/info :bucket {})
              (map :filename)))))

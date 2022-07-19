(defproject com.timezynk/mongo "0.6.4"
  :description "Clojure wrapper for com.mongodb.client Java API."
  :url "https://github.com/TimeZynk/mongo"
  :license {:name "MIT"
            :url "https://mit-license.org"}
  :scm {:name "git"
        :url  "https://github.com/TimeZynk/mongo"}
  :dependencies [[ch.qos.logback/logback-core "1.2.9"]
                 [ch.qos.logback/logback-classic "1.2.9"]
                 [ch.qos.logback.contrib/logback-jackson "0.1.5"]
                 [ch.qos.logback.contrib/logback-json-classic "0.1.5"]
                 [org.apache.logging.log4j/log4j-to-slf4j "2.17.0"]
                 [org.clojure/clojure "1.10.3"]
                 [org.clojure/core.async "1.3.618"]
                 [org.clojure/tools.logging "1.2.4"]
                 [org.mongodb/mongo-java-driver "3.12.10"]]
  :repl-options {:init-ns com.timezynk.mongo}
  :test-paths ["src" "test"]
  :plugins [[lein-cljfmt "0.6.7"]]
  :profiles {:kaocha {:dependencies [[lambdaisland/kaocha "1.0.632"]]
                      :jvm-opts ["-Djdk.tls.client.protocols=TLSv1,TLSv1.1,TLSv1.2"]}}
  :aliases {"kaocha" ["with-profile" "+kaocha" "run" "-m" "kaocha.runner"]})

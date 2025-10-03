(defproject jepsen.patroni "0.1.0-SNAPSHOT"
  :description "Patroni tests"
  :url "https://github.com/patroni/patroni"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/tools.nrepl "0.2.13"]
                 [clojure-complete "0.2.5"]
                 [jepsen "0.2.6"]
                 [org.clojure/java.jdbc "0.7.12"]
                 [org.postgresql/postgresql "42.3.2"]])

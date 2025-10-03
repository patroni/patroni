(ns jepsen.patroni-test
  (:require [clojure.test :refer :all]
            [jepsen.core :as jepsen]
            [jepsen.patroni :as patroni]))

(def patroni_nodes ["patroni1" "patroni2" "patroni3"])

(def etcd_nodes ["etcd1" "etcd2" "etcd3"])

(deftest patroni-test
  (is (:valid? (:results (jepsen/run! (patroni/patroni-test patroni_nodes etcd_nodes))))))

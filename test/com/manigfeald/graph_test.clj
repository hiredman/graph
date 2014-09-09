(ns com.manigfeald.graph-test
  (:require [clojure.test :refer :all]
            [com.manigfeald.graph :refer :all]
            [com.manigfeald.kvgc :as k]
            [loom.graph :as g]
            [loom.attr :as a]
            [clojure.java.jdbc :as jdbc]
            [com.manigfeald.graph.alloc :refer [alloc]])
  (:import (java.util UUID)))

(defn t-gs []
  (gs (let [con {:connection-uri "jdbc:derby:memory:myDB;create=true"}]
        (assoc con :connection (jdbc/get-connection con)))
      {:named-graph "ng"
       :graph "g"
       :fragment "f"
       :graph-fragments "gf"
       :edge "e"
       :node "n"
       :attribute/text "t"}))

(defn allocate-graph [gs]
  (alloc gs -1 (constantly nil)))

(deftest a-test
  (let [gs (t-gs)
        _ (create-tables! gs)
        graph-id (allocate-graph gs)
        _ (is graph-id)
        g (id-graph gs graph-id)
        [a b c] (repeatedly 3 #(UUID/randomUUID))
        g (g/add-nodes g a b c)
        _ (is g)
        _ (is a)
        _ (is b)
        _ (is c)
        g (g/add-edges g [a b 2] [a c 4] [c b 0])
        n (g/nodes g)
        _ (is (contains? (set n) a))
        _ (is (contains? (set n) b))
        _ (is (contains? (set n) c))
        edges (g/edges g)
        _ (is (contains? (set edges) [a b 2]))
        _ (is (contains? (set edges) [a c 4]))
        _ (is (contains? (set edges) [c b 0]))
        graph-id2 (allocate-graph gs)
        g2 (id-graph gs graph-id2)
        [a2 b2 c2] (repeatedly 3 #(UUID/randomUUID))
        g2 (g/add-nodes g2 a2 b2 c2)
        n (g/nodes g2)
        _ (is (not (contains? (set n) a)))
        _ (is (not (contains? (set n) b)))
        _ (is (not (contains? (set n) c)))
        _ (is (contains? (set n) a2))
        _ (is (contains? (set n) b2))
        _ (is (contains? (set n) c2))
        _ (is (g/has-node? g a))
        _ (is (not (g/has-node? g a2)))
        g3 (a/add-attr g a :text/label "a")
        _ (is (nil? (a/attr g a :text/label)))
        _ (is (= "a" (a/attr g3 a :text/label)))
        g4 (a/add-attr g3 b :text/label "b")
        _ (is (= "a" (a/attr g4 a :text/label)))
        _ (is (= "b" (a/attr g4 b :text/label)))
        _ (is (g/has-edge? g a b))
        _ (is (= [a] (g/predecessors g c)))
        _ (is (= #{b c} (set (g/successors g a))))
        _ (is (= 2 (g/out-degree g a)))
        _ (is (= #{[a b 2] [a c 4]} (set (g/out-edges g a))))
        _ (is (= 1 (g/in-degree g c)))
        _ (is (= #{[a c 4]} (set (g/in-edges g c))))
        _ (is (= 4 (g/weight g a c)))
        _ (is (= 0 (g/weight g2 a c)))
        _ (is (= 4 (g/weight g4 a c)))
        n (transact gs "foo" (fn [g]
                               (let [g (g/add-nodes g (UUID/randomUUID))]
                                 [(g/nodes g) g])))
        _ (is (= (count n) 1))
        x (transact gs "foo" (fn [g] [(g/nodes g) g]))
        _ (is (= 1 (count x)))
        g4 (read-only-view gs "foo")
        n (g/nodes g4)
        _ (is (= 1 (count n)))
        _ (transact gs "foo" (fn [g] [(g/nodes g) g]))
        n (g/nodes g4)
        _ (is (= 1 (count n)))
        [a b] (repeatedly 2 #(UUID/randomUUID))
        e (transact gs "bar" (fn [g]
                               (let [g (g/add-nodes g a b)
                                     g (g/add-edges g [a b])]
                                 [(g/edges g) g])))
        _ (is (= (count e) 1))
        e (transact gs "bar" (fn [g]
                               (let [[a b] (g/nodes g)
                                     g (g/remove-edges g [a b])
                                     _ (is (number? (:id g)))
                                     g (g/remove-edges g [b a])
                                     _ (is (number? (:id g)))]
                                 [(g/edges g) g])))
        _ (is (= (count e) 0))
        g (g/add-nodes g 1 2 3)
        _ (transact gs "h"
                    (fn [g]
                      (let [n (vec (repeatedly 1e3 #(UUID/randomUUID)))
                            g (apply g/add-nodes g n)
                            g (apply g/add-edges g (map vec (partition 2 1 n)))]
                        (doseq [[a b] (partition 2 1 n)]
                          (is (g/has-edge? g a b)))
                        [nil g])))
        _ (transact gs "123"
                    (fn [g]
                      (let [g (g/add-nodes g 1)
                            g (g/add-nodes g 1)]
                        (is (= 1 (count (g/nodes g)) ))
                        [nil g])))]))

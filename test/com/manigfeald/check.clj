(ns com.manigfeald.check
  (:require [clojure.test :refer :all]
            [com.manigfeald.graph :refer :all]
            [com.manigfeald.kvgc :as k]
            [loom.graph :as g]
            [loom.attr :as a]
            [clojure.java.jdbc :as jdbc]
            [com.manigfeald.graph.alloc :refer [alloc]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]])
  (:import (java.util UUID)))


(defn t-gs []
  (gs (let [con {:connection-uri "jdbc:derby:memory:check;create=true"}]
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

(def node (gen/fmap (fn [i] (UUID. 0 i)) gen/pos-int))

(def edges (gen/tuple node node))

(defn set-equal [a b]
  (= (set a) (set b)))

(def graph-ops
  (gen/one-of
   [(gen/tuple (gen/return [:g/nodes :view-coll]))
    (gen/tuple (gen/return [:g/edges :view-edges-coll]))
    (gen/tuple (gen/return [:g/has-node? :view]) node)
    (gen/tuple (gen/return [:g/has-edge? :view]) node node)
    (gen/tuple (gen/return [:g/successors :view-coll]) node)
    (gen/tuple (gen/return [:g/predecessors :view-coll]) node)
    (gen/tuple (gen/return [:g/out-degree :view]) node)
    (gen/tuple (gen/return [:g/in-degree :view]) node)
    (gen/tuple (gen/return [:g/add-nodes :update]) node)
    (gen/tuple (gen/return [:g/add-edges :update])
               (gen/tuple node node gen/pos-int))
    (gen/tuple (gen/return [:g/weight :nil-or-zero]) node node)
    (gen/tuple (gen/return [:g/remove-nodes :update]) node)
    (gen/tuple (gen/return [:g/remove-edges :update])
               (gen/tuple node node gen/pos-int))
    (gen/tuple (gen/return [:g/transpose :update]))]))

(def graph-program
  (gen/vector graph-ops))

(defn step-program
  [{:keys [loom graph result]} [idx [[fun fun-type] & args :as code]]]
  (try
    (let [fun @(ns-resolve 'com.manigfeald.check
                           (symbol (name (namespace fun))
                                   (name fun)))
          loom' (apply fun loom args)
          graph' (apply fun graph args)
          [loom graph] (if (= fun-type :update)
                         [loom' graph']
                         [loom graph])
          r (case fun-type
              :nil-or-zero (= (or loom' 0)
                              (or graph' 0))
              :update true
              :view (= loom' graph')
              :view-coll (set-equal loom' graph')
              :view-edges-coll
              (set-equal (map (juxt g/src g/dest) loom')
                         (map (juxt g/src g/dest) graph')))]
      (assert r [idx code loom' graph'])
      {:loom loom
       :graph graph
       :result (and result r)})
    (catch Exception e
      (throw (Exception. (print-str idx code) e)))))

(defn run-program [lg g program]
  (:result
   (reduce
    step-program
    {:loom lg
     :graph g
     :result true}
    (map-indexed vector program))))

(defspec regular-graph
  500
  (prop/for-all
   [program graph-program]
   (let [gs (t-gs)
         _ (try
             (create-tables! gs)
             (catch Exception _))
         gid (allocate-graph gs)
         g (id-graph gs gid)]
     (run-program (g/weighted-digraph) g program))))

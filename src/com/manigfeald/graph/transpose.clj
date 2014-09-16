(ns com.manigfeald.graph.transpose
  (:require [loom.graph :as g]
            [loom.attr :as a]
            [clojure.java.jdbc :as jdbc]
            [com.manigfeald.graph.readonly :as ro]
            [com.manigfeald.kvgc :as k]
            [com.manigfeald.graph.gc :as gc]
            [com.manigfeald.graph.alloc :refer [alloc]]
            [com.manigfeald.graph.rel :as r])
  (:import (java.util.concurrent.locks ReentrantReadWriteLock)
           (java.nio ByteBuffer)
           (java.lang.ref WeakReference)
           (java.util UUID)))

(defrecord G [graph]
  g/EditableGraph
  (add-nodes* [g nodes]
    (->G (g/add-nodes* graph nodes)))
  (add-edges* [g edges]
    (->G (g/add-edges* graph
                       (for [[src dest weight] edges]
                         [dest src weight]))))
  (remove-edges* [g edges]
    (->G (g/remove-edges* graph
                          (for [[src dest weight] edges]
                            [dest src weight]))))
  (remove-nodes* [g nodes]
    (->G (g/remove-nodes* graph nodes)))
  g/Graph
  (nodes [this]
    (g/nodes graph))
  (edges [this]
    (for [edge (g/edges graph)]
      [(g/dest edge) (g/src edge) (g/weight this edge)]))
  (has-node? [this node]
    (g/has-node? graph node))
  (has-edge? [this src dest]
    (g/has-edge? graph dest src))
  (successors [g]
    (partial g/successors g))
  (successors [this node]
    (g/predecessors graph node))
  (out-degree [this node]
    (g/in-degree graph node))
  (out-edges [this node]
    (g/in-edges graph node))
  a/AttrGraph
  (add-attr [g node-or-edge k v]
    (if (satisfies? g/Edge node-or-edge)
      (->G (a/add-attr graph [(g/dest node-or-edge) (g/src node-or-edge)] k v))
      (->G (a/add-attr graph node-or-edge k v))))
  (attr [g node-or-edge k]
    (if (satisfies? g/Edge node-or-edge)
      (a/attr graph [(g/dest node-or-edge) (g/src node-or-edge)] k)
      (a/attr graph node-or-edge k)))
  g/Digraph
  (predecessors [g]
    (partial g/predecessors g))
  (predecessors [this node]
    (g/successors graph node))
  (in-degree [this node]
    (count (g/predecessors this node)))
  (in-edges [this node]
    (g/out-edges graph node))
  (transpose [this]
    graph)
  g/WeightedGraph
  (weight [g]
    (partial g/weight g))
  (weight [this edge]
    (g/weight this (g/src edge) (g/dest edge)))
  (weight [g n1 n2]
    (g/weight graph n2 n1)))

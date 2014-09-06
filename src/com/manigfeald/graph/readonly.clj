(ns com.manigfeald.graph.readonly
  (:require [loom.graph :as g]
            [loom.attr :as a])
  (:import (java.io Closeable)))

(defrecord ROG [g cb]
  g/Graph
  (nodes [this]
    (g/nodes g))
  (edges [this]
    (g/edges g))
  (has-node? [this node]
    (g/has-node? g node))
  (has-edge? [this src dest]
    (g/has-edge? g src dest))
  (successors [g]
    (partial g/successors g))
  (successors [this node]
    (g/successors g node))
  (out-degree [this node]
    (g/out-degree g node))
  (out-edges [this node]
    (g/out-edges g node))
  a/AttrGraph
  (attr [_ node-or-edge k]
    (a/attr g node-or-edge k))
  g/Digraph
  (predecessors [g]
    (partial g/predecessors g))
  (predecessors [this node]
    (g/predecessors g node))
  (in-degree [this node]
    (g/in-degree g node))
  (in-edges [this node]
    (g/in-edges g node))
  (transpose [this]
    (assert nil))
  g/WeightedGraph
  (weight [g]
    (partial g/weight g))
  (weight [this edge]
    (g/weight this (g/src edge) (g/dest edge)))
  (weight [_ n1 n2]
    (g/weight g n1 n2))
  Closeable
  (close [this]
    (cb this)))

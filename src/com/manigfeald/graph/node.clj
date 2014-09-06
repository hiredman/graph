(ns com.manigfeald.graph.node)

(defprotocol HasNodes
  (allocate-nodes [graph n]))

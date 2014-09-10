(ns com.manigfeald.graph.gc
  (:require [clojure.java.jdbc :as jdbc]
            [com.manigfeald.kvgc :as k]))

(defn gs-references [con config]
  (for [[k v] config
        i (case k
            :named-graph (map
                          (juxt (constantly k) :id)
                          (jdbc/query con [(format "SELECT id FROM %s" v)]))
            :graph (map
                    (juxt (constantly k) :id)
                    (jdbc/query con [(format "SELECT id FROM %s" v)]))
            :fragment (map
                       (juxt (constantly k) :id)
                       (jdbc/query con [(format "SELECT id FROM %s" v)]))
            :graph-fragments (map
                              (juxt (constantly k) :id)
                              (jdbc/query con [(format "SELECT id FROM %s" v)]))
            :edge (map
                   (juxt (constantly k) :id)
                   (jdbc/query con [(format "SELECT id FROM %s" v)]))
            :node (map
                   (juxt (constantly k) :id)
                   (jdbc/query con [(format "SELECT id FROM %s" v)]))
            (if (= "attribute" (namespace k))
              (map
               (juxt (constantly k) :id)
               (jdbc/query con [(format "SELECT id FROM %s" v)]))
              (assert nil [k v])))]
    [i nil]))

;; TODO: simplify the reference "graph" presented to the garbage
;; collector
(defrecord Heap [con config mark-state]
  k/Heap
  (references [heap ptr]
    (let [[table id] ptr
          _ (assert table)
          _ (assert id)
          x (case table
              :named-graph (for [{:keys [graph_id name]}
                                 (jdbc/query con [(format "SELECT name, graph_id FROM %s WHERE id = ?" (:named-graph config)) id])]
                             (do
                               (assert graph_id name)
                               [:graph graph_id]))
              :graph (for [{:keys [id]}
                           (jdbc/query con [(format "SELECT id FROM %s WHERE graph_id = ?" (:graph-fragments config)) id])]
                       [:graph-fragments id])
              :fragment (for [[k v] config
                              :when (not (contains? #{:named-graph :graph :fragment :graph-fragments} k))
                              {:keys [id]} (jdbc/query con [(format "SELECT id FROM %s WHERE fragment_id = ?" (k config)) id])]
                          [k id])
              :graph-fragments (for [{:keys [fragment_id graph_id]}
                                     (jdbc/query
                                      con
                                      [(format "SELECT graph_id,fragment_id FROM %s WHERE id = ?" (:graph-fragments config)) id])
                                     i [[:fragment fragment_id]
                                        [:graph graph_id]]]
                                 i)
              :edge (for [{:keys [src dest fragment_id]}
                          (jdbc/query con [(format "SELECT src,dest,fragment_id FROM %s WHERE id = ?" (:edge config)) id])
                          n (concat (for [id [src dest]
                                          i (jdbc/query con [(format "SELECT id FROM %s WHERE vid = ?" (:node config)) id])]
                                      [:node (:id i)])
                                    [[:fragment fragment_id]])]
                      n)
              :node (for [{:keys [fragment_id]} (jdbc/query con [(format "SELECT fragment_id FROM %s WHERE id = ?" (:node config)) id])]
                      [:fragment fragment_id])
              ;; TODO: fixme for vids
              (if (= "attribute" (namespace table))
                (for [{:keys [object_type object_id  fragment_id]}
                      (jdbc/query con [(format "SELECT object_type, object_vid, fragment_id FROM %s WHERE id = ?" (table config)) id])
                      i [[(keyword object_type) object_id]
                         [:fragment fragment_id]]]
                  i)
                (assert nil [table ptr])))]
      (assert (not (seq (for [[k v] x
                              :when (or (nil? k) (nil? v))]
                          k)))
              [table (first (for [[k v] x
                                  :when (or (nil? k) (nil? v))]
                              k))])
      (assert (not (seq (for [[k v] x
                              :when (not (number? v))]
                          true))))
      x))
  (tag [heap ptr tag-value]
    (let [tag-value (case tag-value
                      true 1
                      false 0)
          [table id] ptr
          table-name (get config table)]
      (jdbc/update! con table-name {:tag tag-value} ["id = ?" id])
      heap))
  (tag-value [heap ptr]
    (let [[table id] ptr
          table-name (get config table)]
      (case (:tag (first (jdbc/query con [(format "SELECT tag FROM %s WHERE id = ?" table-name) id])))
        nil true
        1 true
        0 false)))
  (free [heap ptr]
    (let [[table id] ptr
          table-name (get config table)]
      (jdbc/delete! con table-name ["id = ?" id])
      heap))
  (with-heap-lock [heap fun]
    (fun heap))
  clojure.core.protocols/CollReduce
  (coll-reduce [_ fun]
    (reduce fun (gs-references con config)))
  (coll-reduce [_ fun init]
    (reduce fun init (gs-references con config))))

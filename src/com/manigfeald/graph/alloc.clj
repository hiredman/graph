(ns com.manigfeald.graph.alloc
  (:require [clojure.java.jdbc :as jdbc]))

(def max-frag-size 5)

(defn clone-frag [gs frag-id]
  (let [[{:keys [size]}]
        (jdbc/query (:con gs) [(format "SELECT size FROM %s WHERE id = ?" (:fragment (:config gs))) frag-id])
        new-frag-id (first (vals (first (jdbc/insert! (:con gs) (:fragment (:config gs)) {:size size}))))]
    (doseq [[k v] (:config gs)
            :when (not (contains? #{:fragment :named-graph :graph :graph-fragments} k))
            item (jdbc/query (:con gs) [(format "SELECT * FROM %s WHERE fragment_id = ?" v) frag-id])]
      (jdbc/insert! (:con gs) v (assoc (dissoc item :id :tag) :fragment_id new-frag-id)))
    [new-frag-id size]))

(defn new-graph [gs]
  (first (vals (first (jdbc/insert! (:con gs) (:graph (:config gs)) {:x 0})))))

(defn copy-with-shared [gs chosen-frag graph-id]
  (fn [id frag]
    (if (= (:id chosen-frag) (:fragment_id frag))
      (let [[frag-id size] (clone-frag gs (:fragment_id frag))]
        (jdbc/insert! (:con gs) (:graph-fragments (:config gs)) {:graph_id graph-id :fragment_id frag-id})
        (jdbc/update! (:con gs) (:fragment (:config gs)) {:size (inc size)} ["id = ?" frag-id])
        frag-id)
      (do
        (jdbc/insert! (:con gs) (:graph-fragments (:config gs)) (dissoc (assoc frag :graph_id graph-id) :id :tag))
        id))))

(defn frag-stats [gs]
  (first (jdbc/query (:con gs)
                     [(format "SELECT (SUM(size) / COUNT(size)) AS avgn, COUNT(size) AS countn, MAX(size) AS maxn, MIN(size) AS minn FROM %s" (:fragment (:config gs)))])))

(defn alloc
  ([gs prev-graph]
     (alloc gs prev-graph (constantly nil)))
  ([gs prev-graph fun]
    ;; (prn (frag-stats gs))
     (let [graph-id (new-graph gs)
           chosen-frag (first (jdbc/query (:con gs)
                                          [(format "
SELECT %s.id AS id, size FROM %s
  JOIN %s ON %s.id = %s.graph_id
  WHERE graph_id = ?
  AND size < ?
"
                                                   (:fragment (:config gs))
                                                   (:graph-fragments (:config gs))
                                                   (:fragment (:config gs))
                                                   (:fragment (:config gs))
                                                   (:graph-fragments (:config gs)))
                                           prev-graph
                                           max-frag-size]))
           new-frag-id (reduce
                        (copy-with-shared gs chosen-frag graph-id)
                        nil
                        (jdbc/query (:con gs) [(format "SELECT * FROM %s WHERE graph_id = ?"
                                                       (:graph-fragments (:config gs)))
                                               prev-graph]))]
       (if new-frag-id
         (fun (biginteger new-frag-id))
         (let [frag-id (first (vals (first (jdbc/insert! (:con gs) (:fragment (:config gs)) {:size 1}))))]
           (jdbc/insert! (:con gs) (:graph-fragments (:config gs)) {:graph_id graph-id :fragment_id frag-id})
           (fun (biginteger frag-id))))
       (biginteger graph-id))))

(ns com.manigfeald.graph.alloc
  (:require [clojure.java.jdbc :as jdbc]
            [com.manigfeald.graph.rel :as r]))

(def max-frag-size 5)

(def frag-and-higher #{:fragment :named-graph :graph :graph-fragments})

(defn clone-frag
  "given a frag id make a new frag containing all the same things,
  returns a tuple of [frag-id size]"
  [gs frag-id]
  (let [[{:keys [size]}]
        (jdbc/query (:con gs) [(format "SELECT size FROM %s WHERE id = ?"
                                       (:fragment (:config gs)))
                               frag-id])
        [new-frag-id] (vals (first (jdbc/insert!
                                    (:con gs)
                                    (:fragment (:config gs)) {:size size})))]
    (doseq [[k v] (:config gs)
            :when (not (contains? frag-and-higher k))
            item (jdbc/query
                  (:con gs)
                  [(format "SELECT * FROM %s WHERE fragment_id = ?" v)
                   frag-id])]
      (jdbc/insert! (:con gs) v (assoc (dissoc item :id :tag)
                                  :fragment_id new-frag-id)))
    [new-frag-id size]))

(defn new-graph [gs]
  (first (vals (first (jdbc/insert! (:con gs) (:graph (:config gs)) {:x 0})))))

(defn copy-with-shared
  [gs chosen-frag graph-id]
  (fn [id frag]
    (if (= (:id chosen-frag) (:fragment_id frag))
      (let [[frag-id size] (clone-frag gs (:fragment_id frag))]
        (jdbc/insert! (:con gs) (:graph-fragments (:config gs))
                      {:graph_id graph-id :fragment_id frag-id})
        (jdbc/update! (:con gs) (:fragment (:config gs))
                      {:size (inc size)} ["id = ?" frag-id])
        frag-id)
      (do
        (jdbc/insert! (:con gs) (:graph-fragments (:config gs))
                      (dissoc (assoc frag :graph_id graph-id) :id :tag))
        id))))

(defn frag-stats
  "dumps some interesting stats about frags"
  [gs]
  (first (jdbc/query
          (:con gs)
          [(format (str "SELECT (SUM(size) / COUNT(size)) AS avgn,"
                        " COUNT(size) AS countn, MAX(size) AS maxn,"
                        " MIN(size) AS minn FROM %s")
                   (:fragment (:config gs)))])))

(defn alloc
  "create a new graph containing everything in a previous graph, takes
  an optional callback to which a fragment id is passed in to which new
  items can be inserted"
  ([gs prev-graph]
     (alloc gs prev-graph (constantly nil)))
  ([gs prev-graph fun]
     (let [graph-id (new-graph gs)
           f (r/as (r/t (:fragment (:config gs)) :id :size) :f)
           gfs (r/as (r/t (:graph-fragments (:config gs))
                          :graph_id :fragment_id) :gfs)
           [chosen-frag] (jdbc/query
                          (:con gs)
                          (-> (r/⨝ f gfs (r/≡ :f/id :gfs/fragment_id))
                              (r/σ #{(r/less-than
                                      :f/size (r/lit max-frag-size))
                                     (r/≡ :gfs/graph_id (r/lit prev-graph))})
                              (r/π :f/id)
                              (r/to-sql)))
           new-frag-id (reduce
                        (copy-with-shared gs chosen-frag graph-id)
                        nil
                        (jdbc/query
                         (:con gs)
                         [(format "SELECT * FROM %s WHERE graph_id = ?"
                                  (:graph-fragments (:config gs)))
                          prev-graph]))]
       (if new-frag-id
         (fun (biginteger new-frag-id))
         (let [frag-id (->> (jdbc/insert!
                             (:con gs)
                             (:fragment (:config gs)) {:size 1})
                            (first)
                            (vals)
                            (first))]
           (jdbc/insert!
            (:con gs)
            (:graph-fragments (:config gs))
            {:graph_id graph-id :fragment_id frag-id})
           (fun (biginteger frag-id))))
       (biginteger graph-id))))

(defn copy-without [gs prev-graph frag-id table id]
  {:pre [(number? prev-graph)
         (number? frag-id)
         (keyword? table)
         (number? id)]}
  (let [graph-id (first
                  (vals
                   (first
                    (jdbc/insert! (:con gs) (:graph (:config gs)) {:x 0}))))
        _ (reduce
           (fn [_ frag]
             (if (= frag-id (:fragment_id frag))
               (let [new-frag-id (first
                                  (vals
                                   (first
                                    (jdbc/insert!
                                     (:con gs)
                                     (:fragment (:config gs))
                                     {:size 0}))))
                     c (atom 0)]
                 (jdbc/insert! (:con gs) (:graph-fragments (:config gs))
                               {:graph_id graph-id :fragment_id new-frag-id})
                 (doseq [[k v] (:config gs)
                         :when (not (contains? #{:fragment :named-graph :graph
                                                 :graph-fragments} k))
                         :let [qs (format
                                   "SELECT * FROM %s WHERE fragment_id = ?" v)]
                         item (jdbc/query (:con gs) [qs frag-id])
                         :when (not (and (= frag-id (:fragment_id item))
                                         (= table k)
                                         (= id (:id item))))]
                   (swap! c inc)
                   (jdbc/insert! (:con gs) v (assoc (dissoc item :id :tag)
                                               :fragment_id new-frag-id)))
                 (jdbc/update! (:con gs) (:fragment (:config gs))
                               {:size @c}
                               ["id = ?" frag-id]))
               (jdbc/insert!
                (:con gs)
                (:graph-fragments (:config gs))
                (dissoc (assoc frag :graph_id graph-id) :id :tag))))
           nil
           (jdbc/query (:con gs) [(format "SELECT * FROM %s WHERE graph_id = ?"
                                          (:graph-fragments (:config gs)))
                                  prev-graph]))]
    (biginteger graph-id)))

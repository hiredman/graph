# graph

A Clojure library designed to provide persistent durable graphs with
the loom api, stored in derby.

## Why?

I started writing a little app that was storing data in
[derby](http://db.apache.org/derby/). Some of that data ended up
having a graph like shape, so I started implementing the
[loom api](https://github.com/aysylu/loom) on top of it. It was all
mutable and gross, so I decided to make a proper go of it in a
library.

## Usage

`[com.manigfeald/graph 0.1.0]`

`(require '[com.manigfeald.graph :as g])`

- clojure state model
- references to graphs
  - a reference is a name
- with a reference you can
  - `transact!`
      - similar to swap!
  - get a read only view
      - similar to deref on an atom
      - a departure from the clojure state model because the view
        really is read only, it doesn't support even creating a new
        graph based on the read only view, but that may be possible to
        add in the future
- unreferenced data is gced
  - uses a very simple stop the world mark and sweep gc that runs at
    the end of a `transact!`

`com.manigfeald.graph/graph-store` takes two parameters `con` and
`config`. `con` is anything that clojure.java.jdbc recognizes as a
connection or a way to get a connection to a database. `config` is a
map that gives the names for the tables in the database that should be
used, an example `config` would look like:

```clj
       {:named-graph "ng"
        :graph "g"
        :fragment "f"
        :graph-fragments "gf"
        :edge "e"
        :node "n"
        :attribute/text "t"}
```

attribute tables are special in that they are named with a namespace
qualified keyword like `:attribute/text` where the name part of the
keyword (`text` here) is the type in the data base of the value of
those attributes, `text` is even more of a special case because of
course in derby there is no `text` column type, it is an internal
alias for the derby type `varchar(1024)`, a less special example would
be `:attribute/int`. when actually adding attributes to a graph, the
attribute name must be a keyword in the form `:int/foo` where `int` is
the type of the value of the attribute and `foo` is the name of the
attribute.

given a graph store you can do three operations:
1. `create-tables!`
2. `transact!`
3. `read-only-view`

`create-tables!` creates the tables given in `config`

`transact!` takes a graph store, a graph name, and a function. the
function will be called with the current graph referenced by the give
name, the function must return a pair of 
`[return-value updated-graph]` were `return-value` will be the return
value of the call to transact and `updated-graph` will be the new
graph value of the named graph when the transaction commits. the
function may be called multiple times if the reference value of the
graph changes between reading and writing. currently `updated-graph`
must be a graph representation backed by this library, but it may be
able to handle arbitrary graph values in the future, although that
will never likely to be terribly efficient.

`read-only-view` returns a graph object for a given name, but the
graph object only implements the read only parts of the loom graph
apis, the view is immutable and its existence prevents garbage
collection of the bits it references. the read only view is
`Closeable` and calling `(.close ...)` will make sure it is available
for gc asap instead of waiting for the jvm to clear weakrefs under
memory pressure.

## Structural Sharing

the immutable graphs are implemented using structural sharing, so each
graph is represented as a tree in the database, each tree in the
database goes graph -> fragments, each graph is composed of some
number of fragments and each fragment is composed of 10 or fewer
attributes, nodes, or edges. fragments can be shared between
graphs.

## TODO
- `transact!` on multiple graphs

## License

Copyright © 2014 Kevin Downey

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

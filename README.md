# graph

A Clojure library designed to provide persistent durable graphs with
the loom api, stored in derby.

## Usage

- clojure state model
- references to graphs
  - a reference is a name
- with a reference you can
  - transact
   - similar to swap!
  - get a read only view
   - similar to deref on an atom
- unreferenced data is gced

## License

Copyright © 2014 Kevin Downey

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

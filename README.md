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

## Why?

I started writing a little app that was storing data in
[derby](http://db.apache.org/derby/). Some of that data ended up
having a graph like shape, so I started implementing the
[loom api](https://github.com/aysylu/loom) on top of it. It was all
mutable and gross, so I decided to make a proper go of it in a
library.

## License

Copyright © 2014 Kevin Downey

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

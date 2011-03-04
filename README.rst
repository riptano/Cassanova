---------
Cassanova
---------

Cassanova is a fake Cassandra cluster. It runs in a single Python thread and
is very lightweight for startup and shutdown- making it particularly useful
for automated testing of Cassandra client interfaces and libraries.

Features:

- Passes most of the Cassandra Thrift system tests!
- Can mimic multiple nodes on different network interfaces
- Easy to start and stop
- Implemented as a Twisted Service, so it can share a process and reactor
  with your own Twisted code

What it does *not* support (yet?):

- Partitioners other than RandomPartitioner
- Indexes
- Column validators or custom comparators
- TTLs
- Consistency levels (there's only one actual data store between all "nodes")
- Write to disk (everything is lost when the process exits)
- CQL (durr)
- describe_splits (durr)
- Perform efficiently (data is stored according to what was easiest to write,
  not what would be most efficient for writes or lookups)

-----
Using
-----

Requirements: Python (2.5 or higher), Twisted Matrix (8.0 or higher, I think),
Thrift python library

The simplest way to run Cassanova is with the ``twistd`` tool. To run in the
foreground and log to the console, listening on the default port (12379)::

    twistd -noy test.tap

To fork and run in the background, leaving a pid file::

    twistd -y test.tap --pidfile whatever.pid --logfile cassanova.log

To change the listening port, set the ``$CASSANOVA_CLUSTER_PORT`` environment
variable.

To run the test suite, have a Cassandra 0.7 source tree handy (I've been
working against the 0.7.3 branch, so I know it works there), make sure the
``nose`` test coverage tool is installed, and run::

    ./run_tests.sh ~/path/to/cassandra-tree

-------------
Code overview
-------------

All the real code is in ``cassanova.py``. The more important classes are:

- ``CassanovaInterface``: Provides the implementation of each of the
  supported Thrift calls. Corresponds to a single Thrift connection to the
  service.
- ``CassanovaNode``: Corresponds to a single (fake) Cassandra node, listening
  on its own interface. You'll need to have a local network interface for each
  node you want to run.
- ``CassanovaService``: The central service, which holds the schema and data.
  Corresponds to a whole cluster. The ``add_node`` method is used to
  instantiate new listening ``CassanovaNode``\s, given a unique network
  interface.

There is short explanation of the way data is stored in memory at the top of
``cassanova.py``.

flowgrid
========

This example is more involved. It starts "flows" of producer+consumer. Where producers generate
string message with a minimum size of `-msgsize` and each producer sends `-msgcount` messages.
Each actor has random errors introduced into it, which cases the system to react and coordinate
to these errors. The source is a great place to look at to understand how to put together all
the peices while actually checking and dealing with errors.

### Running

    $ go install
    $ flowgrid \
    -etcd http://127.0.0.1:2379 \
    -nats nats://127.0.0.1:4222 \
    -nodes 1 \
    -msgsize 1000 \
    -msgcount 100000 \
    -producers 6 \
    -consumers 6 \
    -flows 2

The example requires Etcd and Nats to be running on the addresses passed on the command line.

 1. `-etcd` specifies which etcd servers to use.
 1. `-nats` specifies which nats servers to use.
 1. `-nodes` specifies the number of hosts that will be running the process. The grid will wait until that number of processes have joined.
 1. `-producers` specifies the number of producers to schedule, per flow.
 1. `-consumers` specifies the number of consumers to schedule, per flow.
 1. `-flows` specifies the number of flows.
 1. `-minsize` is the minimum size of each message in bytes, the max is 2x the min.
 1. `-msgcount` is the count of messages each producer will send.

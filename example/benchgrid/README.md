gridbench
=========

This example is more involved. It in effect runs a performance test. A set of
actors called the producers shuffle messages to another set of actors called
the consumers. Each message has a number, and is sent to one consumer. The
consumer is chosen by take the message number modulo the number of consumers.
When all consumers are done, they exit, and a singleton leader actor logs
the aggragate metrics for message passing rates.

# Running

    $ gridbench \
      -etcd http://localhost:2379 \
      -nats nats://localhost:4222 \
      -nodes 1 \
      -producers 6 \
      -consumers 6 \
      -minsize 1000 \
      -mincount 100000

The example requires Etcd and Nats to be running on the addresses passed
on the command line.

 1. `-nodes` specifies the number of hosts that will be running the process. The grid will wait until that number of process have been detected before producing messages.
 1. `-producers` specifies the number of producers to schedule.
 1. `-consumers` specifies the number of consumers to schedule.
 1. `-minsize` is the minimum size of each message in bytes, the max is 2x the min.
 1. `-mincount` is the minimum count of messages each producer will send, the max is 2x the min.
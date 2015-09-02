firstgrid
=========

This is a simple example of how to use the library. Two actors are started,
one named "leader" another named "follower", the leader sends timestamp
messages to the follower, which prints the message. All the basics of
starting, creating, messaging, and running a system using grid is 
contained in this example.

## Running

    $ firstgrid

The code requires that Etcd and Nats are both running on localhost, 
accessable from `http://localhost:2379` for Etcd, and 
`nats://localhost:4222` for Nats.
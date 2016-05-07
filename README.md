# Tansu

[![Build Status](https://travis-ci.org/shortishly/tansu.svg)](https://travis-ci.org/shortishly/tansu)


Tansu is a distributed key value store designed to maintain
configuration and other data that must be highly available. It uses
the [Raft Consensus algorithm](https://raft.github.io) for leadership
election and distribution of state amongst its members. Node discovery
is via [mDNS](https://github.com/shortishly/mdns) and automatically
forms a mesh of nodes sharing the same environment.

## Key Value Store

Tansu has a REST interface to set, get or delete the value represented
by a key. It also provides a HTTP
[Server Sent Event Stream](https://en.wikipedia.org/wiki/Server-sent_events)
of changes to the store.

## Locks

Tansu provides test and set operations that can be used to operate
locks through a simple REST based HTTP
[Server Sent Event Stream](https://en.wikipedia.org/wiki/Server-sent_events)
interface.


# Quick Start

To start a 5 node Tansu cluster using Docker:

```shell
for i in {1..5}; do 
    docker run \
        --name tansu-$(printf %03d $i) \
        -d shortishly/tansu;
done
```

## Key Value Store

Assign the value "world" to the key "hello" via a random node of the
cluster:

```shell
curl \
    -s \
    http://$(
        docker inspect \
            --format={{.NetworkSettings.IPAddress}} \
            tansu-$(printf %03d $[1 + $[RANDOM % 5]]))/api/keys/hello \
            -d value=world
```

Obtain the current value of "hello" from a random member of the cluster:

```shell
curl \
    -s \
    http://$(
        docker inspect \
            --format={{.NetworkSettings.IPAddress}} \
            tansu-$(printf %03d $[1 + $[RANDOM % 5]]))/api/keys/hello
```

Delete the key by asking a random member of the cluster:

```shell
curl \
    -X DELETE \
    -i \
    http://$(
        docker inspect \
            --format={{.NetworkSettings.IPAddress}} \
            tansu-$(printf %03d $[1 + $[RANDOM % 5]]))/api/keys/hello
```

## Locks

In several different shells simultaneously request a lock on "abc":

```shell
curl \
    -s \
    http://$(
        docker inspect \
            --format={{.NetworkSettings.IPAddress}} \
            tansu-$(printf %03d $[1 + $[RANDOM % 5]]))/api/locks/abc
```

```shell
curl \
    -s \
    http://$(
        docker inspect \
            --format={{.NetworkSettings.IPAddress}} \
            tansu-$(printf %03d $[1 + $[RANDOM % 5]]))/api/locks/abc
```

```shell
curl \
    -s \
    http://$(
        docker inspect \
            --format={{.NetworkSettings.IPAddress}} \
            tansu-$(printf %03d $[1 + $[RANDOM % 5]]))/api/locks/abc
```

One shell is granted the lock, with the remaining shells waiting their
turn. Drop the lock by hitting `^C` on the holder, the lock is then
allocated to another waiting shell.

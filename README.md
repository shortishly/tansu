# Tansu

[![Build Status](https://travis-ci.org/shortishly/tansu.svg)](https://travis-ci.org/shortishly/tansu)


Tansu is a distributed key value store designed to maintain
configuration and other data that must be highly available. It uses
the [Raft Consensus algorithm](https://raft.github.io) for leadership
election and distribution of state amongst its members. Node discovery
is via [mDNS](https://github.com/shortishly/mdns) and will
automatically form a mesh of nodes sharing the same environment.

## Features

### Key Value Store

Tansu has a REST interface to set, get or delete the value represented
by a key. It also provides a HTTP
[Server Sent Event Stream](https://en.wikipedia.org/wiki/Server-sent_events)
of changes to the store.

### Locks

Tansu provides test and set operations that can be used to operate
locks through a simple REST based HTTP
[Server Sent Event Stream](https://en.wikipedia.org/wiki/Server-sent_events)
interface.


## Quick Start

To start a 5 node Tansu cluster using Docker:

```shell
for i in {1..5}; do 
    docker run \
        --name tansu-$(printf %03d $i) \
        -d shortishly/tansu;
done
```

### Key Value Store

Using a random node in the cluster stream changes to the key "hello":

```shell
curl \
    -i \
    -s \
    http://$(docker inspect \
            --format={{.NetworkSettings.IPAddress}} \
            tansu-$(printf %03d $[1 + $[RANDOM % 5]]))/api/keys/hello?stream=true
```

Note that you can create streams from keys that do not currently exist
in the store. Once a value has been assigned to the key the stream
will issue change notifications.

#### Set

In another shell assign the value "world" to the key "hello" using a
random member of the cluster:

```shell
curl \
    -i \
    -s \
    http://$(docker inspect \
            --format={{.NetworkSettings.IPAddress}} \
            tansu-$(printf %03d $[1 + $[RANDOM % 5]]))/api/keys/hello \
            -d value=world
```

The stream will now contain a `set` notification:

```shell
id: -576460752303423422
event: set
data: {"category":"user","key":"/hello","value":"world"}
```

#### Get

Ask a random member of the cluster for the current value of "hello":

```shell
curl \
    -i \
    -s \
    http://$(docker inspect \
            --format={{.NetworkSettings.IPAddress}} \
            tansu-$(printf %03d $[1 + $[RANDOM % 5]]))/api/keys/hello
```

#### Delete

Ask a random member of the cluster to delete the key "hello":

```shell
curl \
    -i \
    -X DELETE \
    http://$(docker inspect \
            --format={{.NetworkSettings.IPAddress}} \
            tansu-$(printf %03d $[1 + $[RANDOM % 5]]))/api/keys/hello
```

The stream now contains a `delete` notification:

```shell
id: -576460752303423414
event: deleted
data: {"category":"user","deleted":"world","key":"/hello"}
```

#### TTL

A value can also be given a time to live by also supplying a TTL header:


```shell
curl \
    -i \
    -s \
    -H "ttl: 10" \
    http://$(docker inspect \
            --format={{.NetworkSettings.IPAddress}} \
            tansu-$(printf %03d $[1 + $[RANDOM % 5]]))/api/keys/hello \
            -d value=ephemeral
```

The event stream will contain details of the `set` together with a TTL
attribute:

```
id: -576460752303421879
event: set
data: {"category":"user","key":"/hello","ttl":10,"value":"ephemeral"}
```

Ten seconds later the key is removed:

```
id: -576460752303421871
event: deleted
data: {"category":"user","deleted":"ephemeral","key":"/hello"}
```



### Locks

Locks are obtained by issuing a HTTP GET on `/api/locks/` followed by
the name of the lock. The response is a Server Sent Event stream that
will indicate the status the lock to the caller. The lock holder will
retain the lock until the connection is dropped (either by the client
or sever). The free lock is then automatically granted to a waiting
connection.

In several different shells simultaneously request a lock on "abc":

```shell
curl \
    -i \
    -s \
    http://$(docker inspect \
            --format={{.NetworkSettings.IPAddress}} \
            tansu-$(printf %03d $[1 + $[RANDOM % 5]]))/api/locks/abc
```

```shell
curl \
    -i \
    -s \
    http://$(docker inspect \
            --format={{.NetworkSettings.IPAddress}} \
            tansu-$(printf %03d $[1 + $[RANDOM % 5]]))/api/locks/abc
```

```shell
curl \
    -i \
    -s \
    http://$(docker inspect \
            --format={{.NetworkSettings.IPAddress}} \
            tansu-$(printf %03d $[1 + $[RANDOM % 5]]))/api/locks/abc
```

One shell is granted the lock, with the remaining shells waiting their
turn. Drop the lock by hitting `^C` on the holder, the lock is then
allocated to another waiting shell.

## Leadership Election

Tansu provides cluster information via the `/api/info` resource as
follows, picking a random node:

```shell
curl \
    -s \
    http://$(docker inspect \
    --format={{.NetworkSettings.IPAddress}} tansu-$(printf %03d $[1 + $[RANDOM % 5]]))/api/info|python -m json.tool
```

Each node may be in `follower` or `candidate` state, with only one
node in the `leader` role:

```json
{
    "applications": {
        "any": "rolling",
        "asn1": "4.0.2",
        "cowboy": "2.0.0-pre.2",
        "cowlib": "1.3.0",
        "crown": "0.0.1",
        "crypto": "3.6.3",
        "envy": "0.0.1",
        "gproc": "git",
        "gun": "1.0.0-pre.1",
        "inets": "6.2.2",
        "jsx": "2.8.0",
        "kernel": "4.2",
        "mdns": "0.4.1",
        "mnesia": "4.13.4",
        "public_key": "1.1.1",
        "ranch": "1.1.0",
        "recon": "2.2.1",
        "rfc4122": "0.0.3",
        "sasl": "2.7",
        "shelly": "0.1.0",
        "ssh": "4.2.2",
        "ssl": "7.3.1",
        "stdlib": "2.8",
        "tansu": "0.13.0"
    },
    "consensus": {
        "cluster": "fadbf747-f700-4d87-b986-73c2a1de18e4",
        "commit_index": 13668,
        "connections": {
            "1c64ca8a-303b-43f7-914f-09e3b680f9ed": {
                "host": "172.17.0.2",
                "port": 80
            },
            "41fc20ae-70be-4e9d-a40d-a8164b165283": {
                "host": "172.17.0.7",
                "port": 80
            },
            "6343a50d-acc6-4f62-866d-b5a7dcde5d04": {
                "host": "172.17.0.5",
                "port": 80
            },
            "e8d25d82-5f07-4598-bb0c-6074793ee111": {
                "host": "172.17.0.3",
                "port": 80
            },
            "e9057f76-24b5-4f4f-b81c-96d555798ef4": {
                "host": "172.17.0.4",
                "port": 80
            }
        },
        "env": "dev",
        "id": "f327cc37-114f-4237-942b-972199e364a1",
        "last_applied": 13668,
        "leader": {
            "commit_index": 13668,
            "id": "e8d25d82-5f07-4598-bb0c-6074793ee111"
        },
        "role": "follower",
        "term": 877
    },
    "version": {
        "major": 0,
        "minor": 13,
        "patch": 0
    }
}
```

This section optionally uses `jq` to parse some of the JSON output
from Tansu. To install use:

```shell
dnf install -y jq
```

The following script iterates over the members of the Tansu cluster
outputting the role of each member:

```shell
for i in {1..5};
do
echo tansu-$(printf %03d $i) $(curl -m 1 -s http://$(docker inspect --format={{.NetworkSettings.IPAddress}} tansu-$(printf %03d $i))/api/info|jq .consensus.role);
done
```

As an example:

```shell
tansu-001 "follower"
tansu-002 "leader"
tansu-003 "follower"
tansu-004 "follower"
tansu-005 "follower"
```

Pause the leader (replace `tansu-002` with your leader):

```shell
docker pause tansu-002
```

Check that one of the other nodes has been established as the leader
by repeating the curl of `/api/info` on the remaining nodes:


```shell
for i in {1..5};
do
echo tansu-$(printf %03d $i) $(curl -m 1 -s http://$(docker inspect --format={{.NetworkSettings.IPAddress}} tansu-$(printf %03d $i))/api/info|jq .consensus.role);
done
```

As an example, `tansu-002` is now paused:

```shell
tansu-001 "leader"
tansu-002
tansu-003 "follower"
tansu-004 "follower"
tansu-005 "follower"
```

Fire some updates into one of the remaining nodes (change `tansu-003` to a running node):

```shell
for i in {0..100};
do
    curl \
        -s \
        http://$(docker inspect --format={{.NetworkSettings.IPAddress}} tansu-003)/api/keys/pqr \
        -d value=$i;
done
```

Tansu will create a snapshot of its current state every so often, and
uses this snapshot when members join (or rejoin) the cluster together
with any log entries subsequent to that snapshot.

Unpause the diposed former leader:

```shell
docker unpause tansu-002
```

Check that `tansu-002` has rejoined the cluster and is now in the `follower` role:

```shell
for i in {1..5};
do
echo tansu-$(printf %03d $i) $(curl -m 1 -s http://$(docker inspect --format={{.NetworkSettings.IPAddress}} tansu-$(printf %03d $i))/api/info|jq .consensus.role);
done
```

The node `tansu-002` is now in the `follower` role:

```shell
tansu-001 "leader"
tansu-002 "follower"
tansu-003 "follower"
tansu-004 "follower"
tansu-005 "follower"
```

Ask the unpaused node for the value of `pqr`:

```shell
curl -i http://$(docker inspect --format={{.NetworkSettings.IPAddress}} tansu-002)/api/keys/pqr
```

The node will have same value as the remainder of the cluster:

```json
{"value":"100"}
```

## Configuration

Tansu uses the following configuration environment.

|environment variable                     |default              |
|-----------------------------------------|---------------------|
|TANSU\_BATCH\_SIZE\_APPEND\_ENTRIES      |32                   |
|TANSU\_CAN\_ADVERTISE                    |true                 |
|TANSU\_CAN\_MESH                         |true                 |
|TANSU\_SNAPSHOT\_DIRECTORY               |/snapshots           |
|TANSU\_DEBUG                             |true                 |
|TANSU\_SM                                |tansu\_sm\_mnesia\_kv|
|TANSU\_ENDPOINT\_SERVER                  |/server              |
|TANSU\_ENDPOINT\_API                     |/api                 |
|TANSU\_HTTP\_PORT                        |80                   |
|TANSU\_DB\_SCHEMA                        |ram                  |
|TANSU\_ENVIRONMENT                       |dev                  |
|TANSU\_ACCEPTORS                         |100                  |
|TANSU\_TIMEOUT\_ELECTION\_LOW            |1500                 |
|TANSU\_TIMEOUT\_ELECTION\_HIGH           |3000                 |
|TANSU\_TIMEOUT\_LEADER\_LOW              |500                  |
|TANSU\_TIMEOUT\_LEADER\_HIGH             |1000                 |
|TANSU\_TIMEOUT\_KV\_EXPIRY               |1000                 |
|TANSU\_TIMEOUT\_KV\_SNAPSHOT             |1000 * 60            |
|TANSU\_TIMEOUT\_MNESIA\_WAIT\_FOR\_TABLES|infinity             |
|TANSU\_TIMEOUT\_SYNC\_SEND\_EVENT        |infinity             |
|TANSU\_TIMEOUT\_STREAM\_PING             |5000                 |
|TANSU\_MINIMUM\_QUORUM                   |3                    |
|TANSU\_MAXIMUM\_SNAPSHOT                 |3                    |

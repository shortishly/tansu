# Raft

An implementation of the
[Raft Consensus algorithm](https://raft.github.io) using web sockets
to communicate between peers and mnesia as the log store.

## Quick Start

Each server needs a port to listen to HTTP connections from other
peers. Start three Raft servers, each on separate port:

### Server 1

Start the first server on port 8080, as follows:

```shell
HTTP_PORT=8080 make shell
```

Then start the second server on port 8081, as follows:

### Server 2:

```shell
HTTP_PORT=8081 make shell
```

Finally the third server on port 8082, as follows:

### Server 3:

```shell
HTTP_PORT=8082 make shell
```

### Initial State

You can use `sys:get_state(raft_consensus).` to determine the current
state of each server:

```erlang
1> sys:get_state(raft_consensus).
{candidate,#{against => [],
             commit_index => 0,
             connecting => #{},
             for => [<<"61752590-95f0-4160-b10a-41b405d5210a">>],
             id => <<"61752590-95f0-4160-b10a-41b405d5210a">>,
             last_applied => 0,
             term => 5,
             timer => #Ref<0.0.4.216>,
             voted_for => <<"61752590-95f0-4160-b10a-41b405d5210a">>}}
```

The candidates haven't been introduced to each other, so each server
will be flip flopping between the `follower` and `candidate` state
busily voting for itself - failing to elect themselves as the leader
without a majority of more than one vote.



### Introductions

#### Server 1

Firstly verify that there are no active connections by running:

```erlang
3> ets:i(raft_connection).
EOT  (q)uit (p)Digits (k)ill /Regexp -->q
```

Each connection is stored as an ETS record in the `raft_connection`
table. Connect the first server to the second and third servers:

```erlang
raft:connect("http://localhost:8081/api").
raft:connect("http://localhost:8082/api").
```

Verify that the connections are in place by:

```erlang
6> ets:i(raft_connection).
<1   > {raft_connection,<0.216.0>,<<"edd9c936-ee5d-45f1-946c-fc3ed1cbf833"  ...
<2   > {raft_connection,<0.217.0>,<<"141b0896-032c-4c66-90ae-248a3a877ed0"  ...
EOT  (q)uit (p)Digits (k)ill /Regexp -->
```

Each connection is identified the PID used and may also have the UUID
of the peer (the leader knows all peer UUIDs, other peers may not know
the UUID for all their connections).

#### Server 2

Verify that one connection is already established between server 1 and server 2:

```erlang
2> ets:i(raft_connection).
<1   > {raft_connection,<0.212.0>,<<"61752590-95f0-4160-b10a-41b405d5210a"  ...
EOT  (q)uit (p)Digits (k)ill /Regexp -->q
```

Note that in this case the UUID for the connection is the same as the
ID for server 1 above.

Connect the second server to the third server:

```erlang
raft:connect("http://localhost:8082/api").
```

Verify that two connections are now in place:

```erlang
6> ets:i(raft_connection).
<1   > {raft_connection,<0.212.0>,<<"61752590-95f0-4160-b10a-41b405d5210a"  ...
<2   > {raft_connection,<0.218.0>,undefined,#Fun<raft_consensus.0.42427295>}
EOT  (q)uit (p)Digits (k)ill /Regexp -->q
```

#### Server 3

This server should already be connected its other peers. You can verify this by:

```erlang
2> ets:i(raft_connection).
<1   > {raft_connection,<0.212.0>,<<"61752590-95f0-4160-b10a-41b405d5210a"  ...
<2   > {raft_connection,<0.216.0>,undefined,#Fun<raft_api_resource.0.30726026>}
```

### Election

Now that each of the servers is connected we can verify that one
leader has been elected.

```erlang
7> sys:get_state(raft_consensus).
{leader,#{against => [],
          commit_index => 0,
          connecting => #{},
          for => [<<"141b0896-032c-4c66-90ae-248a3a877ed0">>,
           <<"61752590-95f0-4160-b10a-41b405d5210a">>],
          id => <<"61752590-95f0-4160-b10a-41b405d5210a">>,
          last_applied => 0,
          next_indexes => #{<<"141b0896-032c-4c66-90ae-248a3a877ed0">> => 1,
            <<"edd9c936-ee5d-45f1-946c-fc3ed1cbf833">> => 1},
          term => 1173,
          timer => #Ref<0.0.1.2527>,
          voted_for => <<"61752590-95f0-4160-b10a-41b405d5210a">>}}
```

Only 1 leader should be elected with the other 2 servers acting as followers:

```erlang
7> sys:get_state(raft_consensus).
{follower,#{commit_index => 0,
            connecting => #{},
            id => <<"edd9c936-ee5d-45f1-946c-fc3ed1cbf833">>,
            last_applied => 0,
            leader => <<"61752590-95f0-4160-b10a-41b405d5210a">>,
            term => 1236,
            timer => #Ref<0.0.2.3012>,
            voted_for => <<"61752590-95f0-4160-b10a-41b405d5210a">>}}
```

```erlang
3> sys:get_state(raft_consensus).
{follower,#{commit_index => 0,
            connecting => #{},
            id => <<"141b0896-032c-4c66-90ae-248a3a877ed0">>,
            last_applied => 0,
            leader => <<"61752590-95f0-4160-b10a-41b405d5210a">>,
            term => 1276,
            timer => #Ref<0.0.1.3666>,
            voted_for => <<"61752590-95f0-4160-b10a-41b405d5210a">>}}
```

### Logging

Using the server that has been elected as the leader, write a log
entry as follows:

```erlang
8> raft:log(#{a => 1}).
ok
```

Note that the leader's state has also updated:

```erlang
9> sys:get_state(raft_consensus).
{leader,#{against => [],
          commit_index => 0,
          connecting => #{},
          for => [<<"141b0896-032c-4c66-90ae-248a3a877ed0">>,
           <<"61752590-95f0-4160-b10a-41b405d5210a">>],
          id => <<"61752590-95f0-4160-b10a-41b405d5210a">>,
          last_applied => 1,
          next_indexes => #{<<"141b0896-032c-4c66-90ae-248a3a877ed0">> => 2,
            <<"edd9c936-ee5d-45f1-946c-fc3ed1cbf833">> => 2},
          term => 1442,
          timer => #Ref<0.0.1.3364>,
          voted_for => <<"61752590-95f0-4160-b10a-41b405d5210a">>}}
```

The `last_applied` indicates the latest log entry that has been
applied, and the `next_indices` shows the next entry to be replicated
to the other peers.

You can verify the state of the log on the leader by:

```erlang
10> ets:i(raft_log).
<1   > {raft_log,1,1414,#{a => 1}}
EOT  (q)uit (p)Digits (k)ill /Regexp -->q
```

The same state should have been replicated to the followers:

```erlang
8> sys:get_state(raft_consensus).
{follower,#{commit_index => 0,
            connecting => #{},
            id => <<"edd9c936-ee5d-45f1-946c-fc3ed1cbf833">>,
            last_applied => 1,
            leader => <<"61752590-95f0-4160-b10a-41b405d5210a">>,
            term => 1682,
            timer => #Ref<0.0.2.4349>,
            voted_for => <<"61752590-95f0-4160-b10a-41b405d5210a">>}}
```

Note that the `last_applied` is now 1, and that the `raft_log` also
contains the same entry:

```erlang
9> ets:i(raft_log).
<1   > {raft_log,1,1414,#{a => 1}}
EOT  (q)uit (p)Digits (k)ill /Regexp -->
```


%% Copyright (c) 2016 Peter Morgan <peter.james.morgan@gmail.com>
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(tansu_consensus).
-behaviour(gen_fsm).

-export([add_connection/6]).
-export([add_server/1]).
-export([candidate/2]).
-export([ckv_delete/2]).
-export([ckv_get/2]).
-export([ckv_set/3]).
-export([ckv_set/4]).
-export([ckv_test_and_delete/3]).
-export([ckv_test_and_set/4]).
-export([ckv_test_and_set/5]).
-export([code_change/4]).
-export([commit_index/0]).
-export([demarshall/2]).
-export([do_add_server/2]).
-export([do_broadcast/2]).
-export([do_call_election_after_timeout/1]).
-export([do_drop_votes/1]).
-export([do_end_of_term_after_timeout/1]).
-export([do_log/2]).
-export([do_rerun_election_after_timeout/1]).
-export([do_send/3]).
-export([do_voted_for/1]).
-export([expired/0]).
-export([follower/2]).
-export([handle_event/3]).
-export([handle_info/3]).
-export([handle_sync_event/4]).
-export([id/0]).
-export([info/0]).
-export([init/1]).
-export([last_applied/0]).
-export([leader/0]).
-export([leader/2]).
-export([quorum/1]).
-export([remove_server/1]).
-export([snapshot/0]).
-export([snapshots/0]).
-export([start/0]).
-export([start_link/0]).
-export([stop/0]).
-export([terminate/3]).


start() ->
    gen_fsm:start({local, ?MODULE}, ?MODULE, [], []).

start_link() ->
    gen_fsm:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    sync_send_all_state_event(stop).

add_connection(Pid, Id, IP, Port, Sender, Closer) ->
    send_all_state_event({add_connection, Pid, Id, IP, Port, Sender, Closer}).

demarshall(_, #{} = Payload) ->
    [Command] = maps:keys(Payload),
    #{Command := Detail} = Payload,
    send_event({Command, Detail}).


add_server(URI) ->
    send_event({add_server, URI}).

remove_server(URI) ->
    send_event({remove_server, URI}).

leader() ->
    sync_send_all_state_event(leader).

id() ->
    sync_send_all_state_event(id).

expired() ->
    sync_send_all_state_event(expired).

info() ->
    sync_send_all_state_event(info).

snapshot() ->
    sync_send_all_state_event(snapshot).

snapshots() ->
    sync_send_all_state_event(snapshots).

last_applied() ->
    sync_send_all_state_event(last_applied).

commit_index() ->
    sync_send_all_state_event(commit_index).


ckv_set(Category, Key, Value) ->
    sync_send_all_state_event({ckv_set, Category, Key, Value}).

ckv_set(Category, Key, Value, TTL) ->
    sync_send_all_state_event({ckv_set, Category, Key, Value, TTL}).

ckv_get(Category, Key) ->
    sync_send_all_state_event({ckv_get, Category, Key}).

ckv_delete(Category, Key) ->
    sync_send_all_state_event({ckv_delete, Category, Key}).

ckv_test_and_set(Category, Key, ExistingValue, NewValue) ->
    sync_send_all_state_event({ckv_test_and_set, Category, Key, ExistingValue, NewValue}).

ckv_test_and_delete(Category, Key, ExistingValue) ->
    sync_send_all_state_event({ckv_test_and_delete, Category, Key, ExistingValue}).

ckv_test_and_set(Category, Key, ExistingValue, NewValue, TTL) ->
    sync_send_all_state_event({ckv_test_and_set, Category, Key, ExistingValue, NewValue, TTL}).

send_event(Event) ->
    gen_fsm:send_event(?MODULE, Event).

send_all_state_event(Event) ->
    gen_fsm:send_all_state_event(?MODULE, Event).

sync_send_all_state_event(Event) ->
    gen_fsm:sync_send_all_state_event(
      ?MODULE, Event, tansu_config:timeout(sync_send_event)).


init([]) ->
    Id = tansu_ps:id(),
    %% subscribe to mDNS advertisements if we are can mesh
    _ = [mdns:subscribe(advertisement) || tansu_config:can(mesh)],
    {ok, follower, do_call_election_after_timeout(
                     do_voted_for(
                       #{term => tansu_ps:term(Id),
                         env => tansu_config:environment(),
                         associations => #{},
                         connections => #{},
                         id => Id,
                         commit_index => tansu_log:commit_index(),
                         last_applied => 0,
                         state_machine => undefined,
                         time_offset_monitor =>  monitor(time_offset, clock_service),
                         connecting => #{}}))}.


do_voted_for(#{id := Id} = Data) ->
    case tansu_ps:voted_for(Id) of
        undefined ->
            maps:without([voted_for], Data);
        VotedFor ->
            Data#{voted_for => VotedFor}
    end.

handle_event({demarshall, Pid, Message}, State, Data) ->
    {next_state, State, do_demarshall(Pid, Message, Data)};

handle_event({add_connection, Peer, Id, IP, Port, Sender, Closer}, State,Data) ->
    case Data of
        #{associations := #{Id := _}} ->
            Closer(),
            {next_state, State, Data};
        _ ->
            {next_state, State, do_add_connection(Peer, Id, IP, Port, Sender, Closer, Data)}
    end;

handle_event({mdns_advertisement, #{ttl := 0}}, State, Data) ->
    %% ignore "goodbyes" for the moment, rely on 'DOWN' when
    %% connection is pulled.
    {next_state, State, Data};

handle_event({mdns_advertisement, #{id := Id}}, State, #{id := Id} = Data) ->
    %% ignore advertisements from ourselves
    {next_state, State, Data};

handle_event(
  {mdns_advertisement, #{id := Id,
                         env := Env,
                         port := Port,
                         host := IP}},
  State,
  #{env := Env} = Data) ->
    case Data of
        #{associations := #{Id := _}} ->
            {next_state, State, Data};

        #{associations := #{}} ->
            {next_state, State, do_add_server(url(IP, Port), Data)}
    end;

handle_event({mdns_advertisement, _}, State, Data) ->
    %% ignore advertisements from others
    {next_state, State, Data};

handle_event(_, _, Data) ->
    {stop, error, Data}.

handle_sync_event({ckv_get, _, _}, _, StateName, #{state_machine := undefined} = Data) ->
    {reply, error, StateName, Data};

handle_sync_event({ckv_get, Category, Key}, _, StateName, #{state_machine := StateMachine} = Data) ->
    {Result, StateMachine} = tansu_sm:ckv_get(Category, Key, StateMachine),
    {reply, Result, StateName, Data};

handle_sync_event(Event, From, StateName = leader, Data) when is_tuple(Event) ->
    [Command | Parameters] = tuple_to_list(Event),
    do_log(#{f => Command, a => Parameters, from => From}, Data),
    {next_state, StateName, Data};

handle_sync_event(Event, _, StateName, Data) when is_tuple(Event) ->
    {reply, {error, not_leader}, StateName, Data};

handle_sync_event(last_applied, _From, StateName, #{last_applied := LA} = Data) ->
    {reply, LA, StateName, Data};

handle_sync_event(commit_index, _From, StateName, #{commit_index := CI} = Data) ->
    {reply, CI, StateName, Data};

handle_sync_event(id, _From, StateName, #{id := Id} = Data) ->
    {reply, Id, StateName, Data};

handle_sync_event(leader, _From, leader = StateName, #{id := Leader} = Data) ->
    {reply, Leader, StateName, Data};

handle_sync_event(leader, _From, StateName, #{leader := Leader} = Data) ->
    {reply, Leader, StateName, Data};

handle_sync_event(leader, _From, StateName, Data) ->
    {reply, {error, not_found}, StateName, Data};

handle_sync_event(info, _From, StateName, Data) ->
    {reply, do_info(StateName, Data), StateName, Data};

handle_sync_event(snapshot, _From, StateName, #{state_machine := undefined} = Data) ->
    {reply, ok, StateName, Data};

handle_sync_event(snapshot, _From, StateName, Data) ->
    {reply, ok, StateName, do_snapshot(Data)};

handle_sync_event(snapshots, _From, StateName, Data) ->
    {reply, do_snapshots(Data), StateName, Data};

handle_sync_event(expired, _From, leader = StateName, #{state_machine := undefined} = Data) ->
    {reply, {ok, []}, StateName, Data};

handle_sync_event(expired, _From, leader = StateName, #{state_machine := StateMachine} = Data) ->
    {Expired, _} = tansu_sm:expired(StateMachine),
    {reply, {ok, Expired}, StateName, Data};

handle_sync_event(expired, _From, StateName, Data) ->
    {reply, {error, not_leader}, StateName, Data};

handle_sync_event(stop, _From, _Name, Data) ->
    {stop, normal, ok, Data}.

handle_info({_,
             {mdns, advertisement},
             #{advertiser := tansu_tcp_advertiser,
               id := Id,
               env := Env,
               port := Port,
               ttl := TTL,
               ip := Host}},
            Name,
            Data) ->
    send_all_state_event(
      {mdns_advertisement, #{id => any:to_binary(Id),
                             env => Env,
                             port => Port,
                             ttl => TTL,
                             host => Host}}),
    {next_state, Name, Data};

handle_info({_,
             {mdns, advertisement},
             #{advertiser := _}},
            Name,
            Data) ->
    {next_state, Name, Data};

handle_info({'CHANGE', Monitor, time_offset, clock_service, _} = Change, leader = Role, #{time_offset_monitor := Monitor} = Data) ->
    error_logger:warning_report([{module, ?MODULE},
                                 {line, ?LINE},
                                 {time, Change},
                                 {role, Role},
                                 {data, Data}]),
    {next_state, follower, tansu_consensus_leader:transition_to_follower(Data)};

handle_info({'CHANGE', Monitor, time_offset, clock_service, _} = Change, candidate = Role, #{time_offset_monitor := Monitor} = Data) ->
    error_logger:warning_report([{module, ?MODULE},
                                 {line, ?LINE},
                                 {time, Change},
                                 {role, Role},
                                 {data, Data}]),
    {next_state, follower, tansu_consensus_candidate:transition_to_follower(Data)};

handle_info({'CHANGE', Monitor, time_offset, clock_service, _} = Change, follower = Role, #{time_offset_monitor := Monitor} = Data) ->
    error_logger:warning_report([{module, ?MODULE},
                                 {line, ?LINE},
                                 {time, Change},
                                 {role, Role},
                                 {data, Data}]),
    {next_state, follower, Data};

handle_info({'DOWN', _, process, Pid, _},
            StateName,
            #{connecting := Connecting,
              connections := Connections,
              associations := Associations,
              match_indexes := MatchIndexes,
              next_indexes := NextIndexes} = Data) ->
    case {Connecting, Connections} of
        {#{Pid := _}, _} ->
            {next_state, StateName, maps:without([change], Data#{connecting := maps:without([Pid], Connecting)})};

        {_, #{Pid := #{association := Association}}} ->
            {next_state, StateName, Data#{connections := maps:without([Pid], Connections),
                                          match_indexes := maps:without([Association], MatchIndexes),
                                          next_indexes := maps:without([Association], NextIndexes),
                                          associations := maps:without([Association], Associations)}};

        {_, #{Pid := _}} ->
            {next_state, StateName, Data#{connections := maps:without([Pid], Connections)}};

        {_, _} ->
            {next_state, StateName, Data}
    end;

handle_info({'DOWN', _, process, Pid, _},
            StateName,
            #{connecting := Connecting,
              connections := Connections,
              associations := Associations} = Data) ->
    case {Connecting, Connections} of
        {#{Pid := _}, _} ->
            {next_state, StateName, maps:without([change], Data#{connecting := maps:without([Pid], Connecting)})};

        {_, #{Pid := #{association := Association}}} ->
            {next_state, StateName, Data#{connections := maps:without([Pid], Connections),
                                          associations := maps:without([Association], Associations)}};

        {_, #{Pid := _}} ->
            {next_state, StateName, Data#{connections := maps:without([Pid], Connections)}};

        {_, _} ->
            {next_state, StateName, Data}

    end;

handle_info({gun_down, _Peer, _, _, _, _}, Name, Data) ->
    {next_state, Name, Data};

handle_info({gun_up, Peer, _}, Name, #{id := Id, connecting := Connecting} = Data) ->
    case maps:find(Peer, Connecting) of
        {ok, Path} ->
            gun:ws_upgrade(
              Peer, Path, [{<<"tansu-id">>, Id},
                           {<<"tansu-host">>, any:to_binary(net_adm:localhost())},
                           {<<"tansu-port">>, any:to_binary(tansu_config:port(http))}]),
            {next_state, Name, Data};

        error ->
            {next_state, Name, Data}
    end;

handle_info({gun_ws_upgrade, Peer, ok, _}, Name, #{connecting := C, change := #{type := add_server, host := Host, port := Port}} = Data) ->
    case maps:find(Peer, C) of
        {ok, _} ->
            {next_state,
             Name,
             do_add_connection(
               Peer,
               Host,
               Port,
               fun
                   (Message) ->
                       gun:ws_send(Peer, {binary, tansu_rpc:encode(Message)})
               end,
               fun
                   () ->
                                      gun:close(Peer)
                              end,
               maps:without([change], Data#{connecting := maps:without([Peer], C)}))};

        error ->
            {next_state, Name, Data}
    end;

handle_info({gun_ws, Peer, {binary, Message}}, Name, Data) ->
    {next_state, Name, do_demarshall(Peer, tansu_rpc:decode(Message), Data)};

handle_info({gun_ws, Pid, {close, _, _}}, Name, Data) ->
    gun:close(Pid),
    {next_state, Name, Data}.


terminate(_Reason, _State, _Data) ->
    gproc:goodbye().

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.


follower({Event, Detail}, Data) ->
    tansu_consensus_follower:Event(Detail, Data);
follower(Event, Data) when is_atom(Event) ->
    tansu_consensus_follower:Event(Data).


candidate({Event, Detail}, Data) ->
    tansu_consensus_candidate:Event(Detail, Data);
candidate(Event, Data) when is_atom(Event) ->
    tansu_consensus_candidate:Event(Data).


leader({Event, Detail}, Data) ->
    tansu_consensus_leader:Event(Detail, Data);
leader(Event, Data) when is_atom(Event) ->
    tansu_consensus_leader:Event(Data).


do_drop_votes(#{id := Id} = Data) ->
    tansu_ps:voted_for(Id, undefined),
    maps:without([voted_for, for, against, leader], Data).


do_end_of_term_after_timeout(State) ->
    after_timeout(end_of_term, tansu_timeout:leader(), State).

do_call_election_after_timeout(State) ->
    after_timeout(call_election, tansu_timeout:election(), State).

do_rerun_election_after_timeout(State) ->
    after_timeout(rerun_election, tansu_timeout:election(), State).

after_timeout(Event, Timeout, #{timer := Timer} = State) ->
    _ = gen_fsm:cancel_timer(Timer),
    after_timeout(Event, Timeout, maps:without([timer], State));
after_timeout(Event, Timeout, State) ->
    State#{timer => gen_fsm:send_event_after(Timeout, Event)}.

do_demarshall(Pid, #{request_vote := #{candidate := Candidate}} = Command, Data) ->
    eval_or_drop_duplicate_connection(
      Candidate,
      Pid,
      fun
          () ->
              demarshall(Pid, Command)
      end,
      Data);

do_demarshall(Pid, #{vote := #{elector := Elector}} = Command, Data) ->
    eval_or_drop_duplicate_connection(
      Elector,
      Pid,
      fun
          () ->
              demarshall(Pid, Command)
      end,
      Data);

do_demarshall(Pid, #{append_entries := #{leader := Leader}} = Command, Data) ->
    eval_or_drop_duplicate_connection(
      Leader,
      Pid,
      fun
          () -> 
              demarshall(Pid, Command)
      end,
      Data);

do_demarshall(Pid, #{install_snapshot := #{leader := Leader}} = Command, Data) ->
    eval_or_drop_duplicate_connection(
      Leader,
      Pid,
      fun
          () -> 
              demarshall(Pid, Command)
      end,
      Data);

do_demarshall(Pid, #{append_entries_response := #{follower := Follower}} = Command, Data) ->
    eval_or_drop_duplicate_connection(
      Follower,
      Pid,
      fun
          () ->
              demarshall(Pid, Command)
      end,
      Data);

do_demarshall(Pid,
              #{log := _} = Command,
              Data) ->
    demarshall(Pid, Command),
    Data.


eval_or_drop_duplicate_connection(Id, Pid, Eval, #{associations := Associations, connections := Connections} = Data) ->
    case {Connections, Associations} of
        {#{Pid := _}, #{Id := Pid}} ->
            %% association already present for this {Id, Pid}
            %% combination: evaluate only required.
            Eval(),
            Data;

        {#{Pid := #{closer := Closer}}, #{Id := _}} ->
            %% association already exists for a different Pid for this
            %% Id, close this one as a duplicate and drop the message.
            Closer(),
            Data;

        {#{Pid := Connection}, #{}} ->
            %% evaluate and associate this {Id, Pid}.
            Eval(),
            Data#{connections := Connections#{Pid := Connection#{association => Id}},
                  associations := Associations#{Id => Pid}}
    end.

do_broadcast(Message, #{connections := Connections} = Data) ->
    maps:fold(
      fun
          (_, #{sender := Sender}, _) ->
              Sender(Message)
      end,
      ok,
      Connections),
    Data.
              
do_send(Message, Recipient, #{associations := Associations, connections := Connections} = Data) ->
    case Associations of
        #{Recipient := Pid} ->
            #{Pid := #{sender := Sender}} = Connections,
            Sender(Message),
            Data;

        #{} ->
            Data
    end.

do_add_connection(Peer, Id, Host, Port, Sender, Closer, #{associations := Associations,
                                                        connections := Connections} = Data) ->
    monitor(process, Peer),
    Data#{associations := Associations#{Id => Peer},
          connections := Connections#{Peer => #{sender => Sender,
                                                closer => Closer,
                                                host => Host,
                                                port => Port,
                                                association => Id}}}.

do_add_connection(Peer, Host, Port, Sender, Closer, #{connections := Connections} = Data) ->
    monitor(process, Peer),
    Data#{connections := Connections#{Peer => #{sender => Sender,
                                                host => Host,
                                                port => Port,
                                                closer => Closer}}}.

do_add_server(_, #{change := _} = Data) ->
    Data;
do_add_server(URI, #{connecting := Connecting} = Data) ->
    case http_uri:parse(URI) of
        {ok, {_, _, Host, Port, Path, _}} ->
            {ok, Peer} = gun:open(Host, Port),
            monitor(process, Peer),
            Data#{connecting := Connecting#{Peer => Path}, change => #{type => add_server, uri => URI, host => Host, port => Port, path => Path}};

        {error, _} ->
            Data
    end.



do_log(Command, #{id := Id, term := Term, commit_index := CI, next_indexes := NI} = Data) ->
    LastLogIndex = tansu_log:write(Term, Command),
     Data#{
       next_indexes := maps:fold(
                         fun
                             (Follower, Index, A) when LastLogIndex >= Index ->
                                 do_send(
                                   tansu_rpc:append_entries(
                                     Term,
                                     Id,
                                     LastLogIndex-1,
                                     tansu_log:term_for_index(LastLogIndex-1),
                                     CI,
                                     [#{term => Term, command => Command}]),
                                   Follower,
                                   Data),
                                 A#{Follower => LastLogIndex+1};

                            (Follower, Index, A) ->
                                 A#{Follower => Index}
                         end,
                         #{},
                         NI)}.


quorum(#{associations := Associations}) ->
    max(tansu_config:minimum(quorum), ((map_size(Associations) + 1) div 2) + 1).

do_info(State, Data) ->
    maps:fold(
      fun
          (env, Env, A) ->
                     A#{env => any:to_binary(Env)};

          (connections, Connections, A) ->
                     A#{connections => connections(Connections)};

          (state_machine, undefined, A) ->
                     A;

          (state_machine, StateMachine, A) ->
                     case tansu_sm:ckv_get(system, [<<"cluster">>], StateMachine) of
                         {{ok, Id}, _} ->
                             A#{cluster => Id};
                         _ ->
                             A
                     end;

          (K, V, A) ->
                     A#{K => V}
      end,
      #{role => State},
      maps:with([against,
                 commit_index,
                 connections,
                 env,
                 id,
                 for,
                 last_applied,
                 leader,
                 match_indexes,
                 next_indexes,
                 state_machine,
                 term,
                 voted_for],
                Data)).


connections(Connections) ->
    maps:fold(
      fun
          (_, #{association := Association, host := Host, port := Port}, A) ->
              A#{Association => #{host => any:to_binary(Host), port => Port}};

          (_, _, A) ->
              A
      end,
      #{},
      Connections).


url(IP, Port) ->
    "http://" ++
        inet:ntoa(IP) ++
        ":" ++
        any:to_list(Port) ++
        tansu_config:endpoint(server).

do_snapshot(#{last_applied := LastApplied, state_machine := StateMachine} = Data) ->
    {{Year, Month, Date}, {Hour, Minute, Second}} = erlang:universaltime(),
    Name = filename:join(
             tansu_config:directory(snapshot),
             iolist_to_list(
               io_lib:format(
                 "~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0wZ",
                 [Year, Month, Date, Hour, Minute, Second]))),
    file:make_dir(tansu_config:directory(snapshot)),
    tansu_sm:snapshot(Name, LastApplied, StateMachine),
    prune_snapshots(),
    Data.

prune_snapshots() ->
    prune_snapshots(
      tansu_config:directory(snapshot),
      tansu_config:maximum(snapshot)).

prune_snapshots(Directory, Maximum) ->
    case file:list_dir(Directory) of
        {ok, Snapshots} when length(Snapshots) > Maximum ->
            {_, TooMany} = lists:split(
                                Maximum, lists:reverse(lists:sort(Snapshots))),
            lists:foreach(
              fun
                  (Superflous) ->
                      ok = file:delete(filename:join(Directory, Superflous))
              end,
              TooMany);

        _ ->
            nop
    end.
                


iolist_to_list(IOL) ->
    binary_to_list(iolist_to_binary(IOL)).

do_snapshots(_Data) ->
    {ok, Snapshots} = file:list_dir(tansu_config:directory(snapshot)),
    lists:sort(Snapshots).

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

-module(raft_consensus).
-behaviour(gen_fsm).

%% API.
-export([add_connection/6]).
-export([add_server/1]).
-export([append_entries/6]).
-export([append_entries_response/5]).
-export([ckv_get/2]).
-export([ckv_set/3]).
-export([ckv_test_and_set/4]).
-export([commit_index/0]).
-export([demarshall/2]).
-export([do_add_server/2]).
-export([do_apply_to_state_machine/3]).
-export([do_broadcast/2]).
-export([do_call_election_after_timeout/1]).
-export([do_drop_votes/1]).
-export([do_end_of_term_after_timeout/1]).
-export([do_log/2]).
-export([do_rerun_election_after_timeout/1]).
-export([do_send/3]).
-export([do_voted_for/1]).
-export([id/0]).
-export([last_applied/0]).
-export([log/1]).
-export([quorum/1]).
-export([remove_server/1]).
-export([request_vote/4]).
-export([start/0]).
-export([start_link/0]).
-export([stop/0]).
-export([vote/3]).

%% gen_fsm.
-export([code_change/4]).
-export([handle_event/3]).
-export([handle_info/3]).
-export([handle_sync_event/4]).
-export([init/1]).
-export([terminate/3]).

%% asynchronous
-export([candidate/2]).
-export([follower/2]).
-export([leader/2]).


start() ->
    gen_fsm:start({local, ?MODULE}, ?MODULE, [], []).

start_link() ->
    gen_fsm:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    sync_send_all_state_event(stop).

add_connection(Pid, Id, IP, Port, Sender, Closer) ->
    send_all_state_event({add_connection, Pid, Id, IP, Port, Sender, Closer}).

demarshall(_, #{request_vote := #{term := Term,
                                  candidate := Candidate,
                                  last_log_index := LastLogIndex,
                                  last_log_term := LastLogTerm}}) ->
    request_vote(Term, Candidate, LastLogIndex, LastLogTerm);

demarshall(_, #{vote := #{elector := Elector, term := Term, granted := Granted}}) ->
    vote(Elector, Term, Granted);

demarshall(_, #{append_entries := #{term := LeaderTerm,
                                    leader := Leader,
                                    prev_log_index := LastApplied,
                                    prev_log_term := PrevLogTerm,
                                    entries := Entries,
                                    leader_commit := LeaderCommitIndex}}) ->
    append_entries(
      LeaderTerm,
      Leader,
      LastApplied,
      PrevLogTerm,
      Entries,
      LeaderCommitIndex);

demarshall(_Pid, #{append_entries_response := #{term := Term,
                                                leader := _Leader,
                                                prev_log_index := PrevLogIndex,
                                                prev_log_term := PrevLogTerm,
                                                follower := Follower,
                                                success := Success}}) ->
    append_entries_response(
      Follower,
      Term,
      Success,
      PrevLogIndex,
      PrevLogTerm);

demarshall(_Pid, #{log := Command}) ->
    log(Command).

add_server(URI) ->
    send_event({add_server, URI}).

remove_server(URI) ->
    send_event({remove_server, URI}).

id() ->
    sync_send_all_state_event(id).

last_applied() ->
    sync_send_all_state_event(last_applied).

commit_index() ->
    sync_send_all_state_event(commit_index).


ckv_set(Category, Key, Value) ->
    sync_send_all_state_event({ckv_set, Category, Key, Value}).

ckv_get(Category, Key) ->
    sync_send_all_state_event({ckv_get, Category, Key}).

ckv_test_and_set(Category, Key, ExistingValue, NewValue) ->
    sync_send_all_state_event({ckv_test_and_set, Category, Key, ExistingValue, NewValue}).
    
    
    
    

append_entries_response(Follower, Term, Success, PrevLogIndex, PrevLogTerm) ->
    send_event({append_entries_response, #{term => Term, follower => Follower,
                                           prev_log_index => PrevLogIndex,
                                           prev_log_term => PrevLogTerm,
                                           success => Success}}).

append_entries(LeaderTerm, Leader, LastApplied, PrevLogTerm, Entries,
               LeaderCommitIndex) ->
    send_event({append_entries, #{term => LeaderTerm, leader => Leader,
                                  prev_log_index => LastApplied,
                                  prev_log_term => PrevLogTerm,
                                  entries => Entries,
                                  leader_commit => LeaderCommitIndex}}).

request_vote(Term, Candidate, LastLogIndex, LastLogTerm) ->
    send_event({request_vote, #{term => Term, candidate => Candidate,
                                last_log_index => LastLogIndex,
                                last_log_term => LastLogTerm}}).

vote(Elector, Term, Granted) ->
    send_event({vote, #{elector => Elector, term => Term, granted => Granted}}).

log(Command) ->
    send_event({log, Command}).

send_event(Event) ->
    gen_fsm:send_event(?MODULE, Event).

send_all_state_event(Event) ->
    gen_fsm:send_all_state_event(?MODULE, Event).

sync_send_all_state_event(Event) ->
    gen_fsm:sync_send_all_state_event(?MODULE, Event, 5000).


init([]) ->
    Id = raft_ps:id(),
    %% subscribe to mDNS advertisements if we are can mesh
    [mdns:subscribe(advertisement) || raft_config:can(mesh)],
    {ok, follower, do_call_election_after_timeout(
                     do_voted_for(
                       #{term => raft_ps:term(Id),
                         env => raft_config:environment(),
                         associations => #{},
                         connections => #{},
                         id => Id,
                         commit_index => raft_log:commit_index(),
                         last_applied => 0,
                         state_machine => undefined,
                         connecting => #{}}))}.


do_voted_for(#{id := Id} = Data) ->
    case raft_ps:voted_for(Id) of
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
            URL = "http://" ++ inet:ntoa(IP) ++ ":" ++ any:to_list(Port) ++ "/api/",
            {next_state, State, do_add_server(URL, Data)}
    end;

handle_event(_, _, Data) ->
    {stop, error, Data}.


handle_sync_event({ckv_test_and_set = F, Category, Key, ExistingValue, NewValue}, From, leader, Data) ->
    do_log(#{f => F, a => [Category, Key, ExistingValue, NewValue], from => From}, Data),
    {next_state, leader, Data};

handle_sync_event({ckv_test_and_set, _, _, _, _}, _, Name, Data) ->
    {reply, not_leader, Name, Data};

handle_sync_event({ckv_get = F, Category, Key}, From, leader, Data) ->
    do_log(#{f => F, a => [Category, Key], from => From}, Data),
    {next_state, leader, Data};

handle_sync_event({ckv_get, _, _}, _, Name, Data) ->
    {reply, not_leader, Name, Data};

handle_sync_event({ckv_set = F, Category, Key, Value}, From, leader, Data) ->
    do_log(#{f => F, a => [Category, Key, Value], from => From}, Data),
    {next_state, leader, Data};

handle_sync_event({ckv_set, _, _, _}, _, Name, Data) ->
    {reply, not_leader, Name, Data};

handle_sync_event(last_applied, _From, Name, #{last_applied := LA} = Data) ->
    {reply, LA, Name, Data};
handle_sync_event(commit_index, _From, Name, #{commit_index := CI} = Data) ->
    {reply, CI, Name, Data};
handle_sync_event(id, _From, Name, #{id := Id} = Data) ->
    {reply, Id, Name, Data};
handle_sync_event(stop, _From, _Name, Data) ->
    {stop, normal, ok, Data}.

handle_info({_,
             {mdns, advertisement},
             #{advertiser := raft_tcp_advertiser,
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

handle_info({'DOWN', _, process, Peer, normal}, Name, #{connecting := Connecting, change := #{type := add_server, uri := URI}} = Data) ->
    %% unable to connect to peer, possibly due to connectivity not being present
    case maps:find(Peer, Connecting) of
        {ok, _Path} ->
            %% log as an issue connecting to the remote node
            error_logger:info_report([{module, ?MODULE},
                                      {line, ?LINE},
                                      {uri, URI},
                                      {type, add_server},
                                      {reason, 'DOWN'}]),
            {next_state, Name, maps:without([change], Data#{connecting := maps:without([Peer], Connecting)})};

        error ->
            {stop, error, Data}
    end;

handle_info({'DOWN', _, process, Pid, _}, leader, #{connections := Connections,
                                                    associations := Associations,
                                                    match_indexes := MatchIndexes,
                                                    next_indexes := NextIndexes} = Data) ->
    %% lost connectivity to a node
    case Connections of
        #{Pid := #{association := Association}} ->
            {next_state, leader, Data#{connections := maps:without([Pid], Connections),
                                       match_indexes := maps:without([Association], MatchIndexes),
                                       next_indexes := maps:without([Association], NextIndexes),
                                       associations := maps:without([Association], Associations)}};

        #{Pid := _} ->
            {next_state, leader, Data#{connections := maps:without([Pid], Connections)}};

        #{} ->
            %% lost connectivity before we really knew anything about
            %% this node
            {next_state, leader, Data}
    end;

handle_info({'DOWN', _, process, Pid, _}, Name, #{connections := Connections, associations := Associations} = Data) ->
    %% lost connectivity to a node
    case Connections of
        #{Pid := #{association := Association}} ->
            {next_state, Name, Data#{connections := maps:without([Pid], Connections),
                                     associations := maps:without([Association], Associations)}};

        #{Pid := _} ->
            {next_state, Name, Data#{connections := maps:without([Pid], Connections)}};

        #{} ->
            %% lost connectivity before we really knew anything about
            %% this node
            {next_state, leader, Data}
    end;

handle_info({gun_down, _Peer, ws, _, _, _}, Name, Data) ->
    {next_state, Name, Data};

handle_info({gun_up, Peer, _}, Name, #{id := Id, connecting := Connecting} = Data) ->
    case maps:find(Peer, Connecting) of
        {ok, Path} ->
            gun:ws_upgrade(
              Peer, Path, [{<<"raft-id">>, Id},
                           {<<"raft-host">>, any:to_binary(net_adm:localhost())},
                           {<<"raft-port">>, any:to_binary(raft_config:port(http))}]),
            {next_state, Name, Data};

        error ->
            {stop, error, Data}
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
                       gun:ws_send(Peer, {binary, raft_rpc:encode(Message)})
               end,
               fun
                   () ->
                       gun:close(Peer)
               end,
               maps:without([change], Data#{connecting := maps:without([Peer], C)}))};

        error ->
            {stop, error, Data}
    end;

handle_info({gun_ws, Peer, {binary, Message}}, Name, Data) ->
    {next_state, Name, do_demarshall(Peer, raft_rpc:decode(Message), Data)};

handle_info({gun_ws, _, {close, _, _}}, Name, Data) ->
    {next_state, Name, Data}.


terminate(_Reason, _State, _Data) ->
    gproc:goodbye().

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.


follower({Event, Detail}, Data) ->
    raft_consensus_follower:Event(Detail, Data);
follower(Event, Data) when is_atom(Event) ->
    raft_consensus_follower:Event(Data).


candidate({#{term := T0} = Event, Detail}, #{term := T1} = Data) when T0 > T1 ->
    %%  Current terms are exchanged when- ever servers communicate; if
    %%  one server’s current term is smaller than the other’s, then it
    %%  updates its current term to the larger value. If a candidate
    %%  or leader discovers that its term is out of date, it
    %%  immediately reverts to follower state
    raft_consensus_follower:Event(Detail, Data);
candidate({Event, Detail}, Data) ->
    raft_consensus_candidate:Event(Detail, Data);
candidate(Event, Data) when is_atom(Event) ->
    raft_consensus_candidate:Event(Data).


leader({#{term := T0} = Event, Detail}, #{term := T1} = Data) when T0 > T1 ->
    %%  Current terms are exchanged when- ever servers communicate; if
    %%  one server’s current term is smaller than the other’s, then it
    %%  updates its current term to the larger value. If a candidate
    %%  or leader discovers that its term is out of date, it
    %%  immediately reverts to follower state
    raft_consensus_follower:Event(Detail, Data);
leader({Event, Detail}, Data) ->
    raft_consensus_leader:Event(Detail, Data);
leader(Event, Data) when is_atom(Event) ->
    raft_consensus_leader:Event(Data).


do_drop_votes(#{id := Id} = Data) ->
    raft_ps:voted_for(Id, undefined),
    maps:without([voted_for, for, against, leader], Data).


do_end_of_term_after_timeout(State) ->
    after_timeout(end_of_term, raft_timeout:leader(), State).

do_call_election_after_timeout(State) ->
    after_timeout(call_election, raft_timeout:election(), State).

do_rerun_election_after_timeout(State) ->
    after_timeout(rerun_election, raft_timeout:election(), State).

after_timeout(Event, Timeout, #{timer := Timer} = State) ->
    gen_fsm:cancel_timer(Timer),
    after_timeout(Event, Timeout, maps:without([timer], State));
after_timeout(Event, Timeout, State) ->
    State#{timer => gen_fsm:send_event_after(Timeout, Event)}.

do_apply_to_state_machine(LastApplied, CommitIndex, State) ->
    do_apply_to_state_machine(lists:seq(LastApplied, CommitIndex), State).

do_apply_to_state_machine([H | T], undefined) ->
    case raft_log:read(H) of
        #{command := #{f := F, a := A}} ->
            {_, State} = apply(raft_sm, F, A),
            do_apply_to_state_machine(T, State);

        #{command := #{f := F}} ->
            {_, State} = apply(raft_sm, F, []),
            do_apply_to_state_machine(T, State)
    end;

do_apply_to_state_machine([H | T], S0) ->
    case raft_log:read(H) of
        #{command := #{f := F, a := A, from := From}} ->
            {Result, S1}  = apply(raft_sm, F, A ++ [S0]),
            gen_fsm:reply(From, Result),
            do_apply_to_state_machine(T, S1);

        #{command := #{f := F, a := A}} ->
            {_, S1} = apply(raft_sm, F, A ++ [S0]),
            do_apply_to_state_machine(T, S1);

        #{command := #{f := F, from := From}} ->
            {Result, S1} = apply(raft_sm, F, [S0]),
            gen_fsm:reply(From, Result),
            do_apply_to_state_machine(T, S1);

        #{command := #{f := F}} ->
            {_, S1} = apply(raft_sm, F, [S0]),
            do_apply_to_state_machine(T, S1)
    end;

do_apply_to_state_machine([], State) ->
    State.

do_demarshall(Pid,
              #{request_vote := #{term := Term,
                                  candidate := Candidate,
                                  last_log_index := LastLogIndex,
                                  last_log_term := LastLogTerm}},
              Data) ->
    eval_or_drop_duplicate_connection(
      Candidate,
      Pid,
      fun
          () ->
              request_vote(Term, Candidate, LastLogIndex, LastLogTerm)
      end,
      Data);

do_demarshall(Pid,
              #{vote := #{elector := Elector,
                          term := Term,
                          granted := Granted}},
              Data) ->
    eval_or_drop_duplicate_connection(
      Elector,
      Pid,
      fun
          () ->
              vote(Elector, Term, Granted)
      end,
      Data);

do_demarshall(Pid,
              #{append_entries := #{term := LeaderTerm,
                                    leader := Leader,
                                    prev_log_index := LastApplied,
                                    prev_log_term := PrevLogTerm,
                                    entries := Entries,
                                    leader_commit := LeaderCommitIndex}},
              Data) ->
            eval_or_drop_duplicate_connection(
              Leader,
              Pid,
              fun
                  () -> 
                      append_entries(
                        LeaderTerm,
                        Leader,
                        LastApplied,
                        PrevLogTerm,
                        Entries,
                        LeaderCommitIndex)
              end,
              Data);

do_demarshall(Pid,
              #{append_entries_response := #{term := Term,
                                             leader := _Leader,
                                             prev_log_index := PrevLogIndex,
                                             prev_log_term := PrevLogTerm,
                                             follower := Follower,
                                             success := Success}},
              Data) ->
    eval_or_drop_duplicate_connection(
      Follower,
      Pid,
      fun
          () ->
              append_entries_response(
                Follower,
                Term,
                Success,
                PrevLogIndex,
                PrevLogTerm)
      end,
      Data);

do_demarshall(_Pid,
              #{log := Command},
              Data) ->
    log(Command),
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
    #{Recipient := Pid} = Associations,
    #{Pid := #{sender := Sender}} = Connections,
    Sender(Message),
    Data.

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
    LastLogIndex = raft_log:write(Term, Command),
     Data#{
       next_indexes := maps:fold(
                         fun
                             (Follower, Index, A) when LastLogIndex >= Index ->
                                 do_send(
                                   raft_rpc:append_entries(
                                     Term,
                                     Id,
                                     LastLogIndex-1,
                                     raft_log:term_for_index(LastLogIndex-1),
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
    max(raft_config:minimum(quorum), ((map_size(Associations) + 1) div 2) + 1).

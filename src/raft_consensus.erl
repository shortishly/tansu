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
-export([add_connection/3]).
-export([add_server/1]).
-export([append_entries/5]).
-export([append_entries/6]).
-export([commit_index/0]).
-export([demarshall/2]).
-export([id/0]).
-export([last_applied/0]).
-export([log/1]).
-export([remove_server/1]).
-export([request_vote/4]).
-export([start/0]).
-export([start_link/0]).
-export([stop/0]).
-export([trace/1]).
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

add_connection(Pid, Sender, Closer) ->
    send_all_state_event({add_connection, Pid, Sender, Closer}).

demarshall(Pid, Message) ->
    send_all_state_event({demarshall, Pid, Message}).

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
    
    

append_entries(Follower, Term, Success, PrevLogIndex, PrevLogTerm) ->
    send_event({append_entries, #{term => Term, follower => Follower,
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
    {ok, follower, call_election(voted_for(
                                   #{term => raft_ps:term(Id),
                                     env => raft_config:environment(),
                                     associations => #{},
                                     connections => #{},
                                     id => Id,
                                     commit_index => raft_log:commit_index(),
                                     last_applied => 0,
                                     state_machine => undefined,
                                     connecting => #{}}))}.


voted_for(#{id := Id} = Data) ->
    case raft_ps:voted_for(Id) of
        undefined ->
            maps:without([voted_for], Data);
        VotedFor ->
            Data#{voted_for => VotedFor}
    end.

handle_event({demarshall, Pid, Message}, State, Data) ->
    {next_state, State, do_demarshall(Pid, raft_rpc:decode(Message), Data)};

handle_event({add_connection, Peer, Sender, Closer}, State, Data) ->
    {next_state, State, do_add_connection(Peer, Sender, Closer, Data)};

handle_event(
  {mdns_advertisement, #{id := Id}},
  State,
  #{id := Id} = Data) ->
    {next_state, State, Data};

handle_event(
  {mdns_advertisement, #{id := Id,
                         env := Env,
                         port := Port,
                         ttl := TTL,
                         host := Host}},
  State,
  #{associations := Associations,
    env := Env} = Data) when TTL > 0 ->
    case Associations of
        #{Id := _} ->
            {next_state, State, Data};

        #{} ->
            URL = "http://" ++ Host ++ ":" ++ any:to_list(Port) ++ "/api/",
            {next_state, State, do_add_server(URL, Data)}
    end;

handle_event(_, _, Data) ->
    {stop, error, Data}.


handle_sync_event(last_applied, _From, Name, #{last_applied := LA} = Data) ->
    {reply, LA, Name, Data};
handle_sync_event(commit_index, _From, Name, #{commit_index := CI} = Data) ->
    {reply, CI, Name, Data};
handle_sync_event(id, _From, Name, #{id := Id} = Data) ->
    {reply, Id, Name, Data};
handle_sync_event(stop, _From, _Name, Data) ->
    {stop, normal, Data}.

handle_info({_,
             {mdns, advertisement},
             #{advertiser := raft_tcp_advertiser,
               id := Id,
               env := Env,
               port := Port,
               ttl := TTL,
               host := Host}},
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
            error_logger:info_report([{module, ?MODULE},
                                      {line, ?LINE},
                                      {uri, URI},
                                      {type, add_server},
                                      {reason, 'DOWN'}]),
            {next_state, Name, maps:without([connecting, change], Data)};

        error ->
            {stop, error, Data}
    end;

handle_info({gun_down, _Peer, ws, _, _, _}, Name, Data) ->
    {next_state, Name, Data};

handle_info({gun_up, Peer, _}, Name, #{connecting := Connecting} = Data) ->
    case maps:find(Peer, Connecting) of
        {ok, Path} ->
            gun:ws_upgrade(Peer, Path),
            {next_state, Name, Data};

        error ->
            {stop, error, Data}
    end;

handle_info({gun_ws_upgrade, Peer, ok, _}, Name, #{connecting := C, change := #{}} = Data) ->
    case maps:find(Peer, C) of
        {ok, _} ->
            {next_state,
             Name,
             do_add_connection(
               Peer,
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
    {next_state, Name, do_demarshall(Peer, raft_rpc:decode(Message), Data)}.


terminate(_Reason, _State, _Data) ->
    ok.

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.


follower({add_server, _}, #{change := _} = Data) ->
    %% change already in progress
    {next_state, follower, Data};
follower({remove_server, _}, #{change := _} = Data) ->
    %% change already in progress
    {next_state, follower, Data};
follower({add_server, URI}, #{last_applied := 0, commit_index := 0} = Data) ->
    {next_state, follower, do_add_server(URI, Data)};

follower({add_server, _}, Data) ->
    {next_state, follower, Data};
follower({remove_server, _}, Data) ->
    {next_state, follower, Data};

%% If election timeout elapses without receiving AppendEntries RPC
%% from current leader or granting vote to candidate: convert to
%% candidate
follower(call_election, #{term := T0,
                          id := Id,
                          commit_index := CI} = D0) ->
    T1 = raft_ps:increment(Id, T0),
    broadcast(raft_rpc:request_vote(T1, Id, CI, CI), D0),
    D1 = drop_votes(D0),
    D2 = D1#{term => T1,
             voted_for => raft_ps:voted_for(Id, Id),
             for => [Id],
             against => []},
    {next_state, candidate, rerun_election(D2)};

%% Drop obsolete vote responses from earlier terms
follower({vote, #{term := Term}},
         #{term := Current} = Data) when Term < Current ->
    {next_state, follower, Data};


%% Reply false if term < currentTerm (§5.1)
follower({append_entries, #{term := Term,
                            leader := Leader,
                            prev_log_index := PrevLogIndex,
                            prev_log_term := PrevLogTerm,
                            entries := _}},
          #{term := Current, id := Id} = Data) when Term < Current ->
    send(
      raft_rpc:append_entries_response(
        Leader, Id, Current, PrevLogIndex, PrevLogTerm, false),
      Leader,
      Data),
    {next_state, follower, Data};

follower({append_entries, #{entries := Entries,
                            prev_log_index := PrevLogIndex,
                            prev_log_term := PrevLogTerm,
                            leader_commit := LeaderCommit,
                            leader := L, term := T}},
         #{commit_index := Commit0,
           last_applied := LastApplied,
           state_machine := SM,
           id := Id} = D0) ->

    case raft_log:append_entries(PrevLogIndex, PrevLogTerm, Entries) of
        {ok, LastIndex} when LeaderCommit > Commit0 ->
            D1 = case min(LeaderCommit, LastIndex) of
                     Commit1 when Commit1 > LastApplied ->
                         D0#{state_machine => sm_apply(
                                                LastApplied + 1,
                                                Commit1,
                                                SM),
                             commit_index => Commit1,
                             last_applied => Commit1
                            };

                     _ ->
                         D0
                 end,
            send(
              raft_rpc:append_entries_response(
                L, Id, T, LastIndex, raft_log:term_for_index(LastIndex), true),
              L,
              D1),
            {next_state, follower, call_election(
                                     D1#{term => raft_ps:term(Id, T),
                                         leader => L})};

        {ok, LastIndex} ->
            send(
              raft_rpc:append_entries_response(
                L, Id, T, LastIndex, raft_log:term_for_index(LastIndex), true),
              L,
              D0),
            {next_state, follower, call_election(
                                     D0#{term => raft_ps:term(Id, T),
                                         leader => L})};

        {error, unmatched_term} ->
            send(
              raft_rpc:append_entries_response(
                L, Id, T, PrevLogIndex, PrevLogTerm, false),
              L,
              D0),
            {next_state, follower, call_election(
                                     D0#{term => raft_ps:term(Id, T),
                                         leader => L})}
    end;

follower({log, Command}, #{leader := Leader} = Data) ->
    send(
      raft_rpc:log(Command),
      Leader,
      Data),
    {next_state, follower, Data};


%% Reply false if term < currentTerm (§5.1)
follower({request_vote, #{term := Term, candidate := Candidate}},
         #{term := Current, id := Id} = Data) when Term < Current ->
    send(
      raft_rpc:vote(Id, Current, false),
      Candidate,
      Data),
    {next_state, follower, Data};

%% If votedFor is null or candidateId, and candidate’s log is at least
%% as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
follower({request_vote, #{term := T,
                          candidate := Candidate}},
         #{id := Id, term := T, voted_for := Candidate} = Data) ->
    send(
      raft_rpc:vote(Id, T, true),
      Candidate,
      Data),
    {next_state, follower, call_election(Data)};


%% If votedFor is null or candidateId, and candidate’s log is at least
%% as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
follower({request_vote, #{term := Term,
                          candidate := Candidate,
                          last_log_index := LastLogIndex,
                          last_log_term := LastLogTerm}},
         #{term := Current, id := Id} = Data) when (Term >= Current) ->

    %% Raft determines which of two logs is more up-to-date by
    %% comparing the index and term of the last entries in the
    %% logs. If the logs have last entries with different terms, then
    %% the log with the later term is more up-to-date. If the logs end
    %% with the same term, then whichever log is longer is more
    %% up-to-date.
    case raft_log:last() of
        #{term := LogTerm} when LogTerm > LastLogTerm ->
            send(
              raft_rpc:vote(Id, Term, false),
              Candidate,
              Data),
            raft_ps:voted_for(Id, undefined),
            {next_state, follower, maps:without(
                                     [voted_for],
                                     call_election(
                                       Data#{term => raft_ps:term(Id, Term)}))};

        #{term := LogTerm} when LogTerm < LastLogTerm ->
            send(
              raft_rpc:vote(Id, Term, true),
              Candidate,
              Data),
            {next_state, follower, call_election(
                                     Data#{term => raft_ps:term(Id, Term),
                                           voted_for => raft_ps:voted_for(
                                                          Id, Candidate)})};


        #{index := LogIndex} when LastLogIndex >= LogIndex->
            send(
              raft_rpc:vote(Id, Term, true),
              Candidate,
              Data),
            {next_state, follower, call_election(
                                     Data#{term => raft_ps:term(Id, Term),
                                           voted_for => raft_ps:voted_for(
                                                          Id, Candidate)})};

        #{index := LogIndex} when LastLogIndex < LogIndex->
            send(
              raft_rpc:vote(Id, Term, false),
              Candidate,
              Data),
            raft_ps:voted_for(Id, undefined),
            {next_state, follower, maps:without(
                                     [voted_for],
                                     call_election(
                                       Data#{term => raft_ps:term(Id, Term)}))}
    end;

%% If votedFor is null or candidateId, and candidate’s log is at least
%% as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
follower({request_vote, #{term := T, candidate := Candidate}},
         #{id := Id, term := T, voted_for := _} = Data) ->
    send(
      raft_rpc:vote(Id, T, false),
      Candidate,
      Data),
    {next_state, follower, Data};

%% an old vote for when we were a candidate
follower({vote, #{granted := _}}, Data) ->
    {next_state, follower, Data}.





candidate({add_server, _}, #{change := _} = Data) ->
    %% change already in progress
    {next_state, candidate, Data};
candidate({remove_server, _}, #{change := _} = Data) ->
    %% change already in progress
    {next_state, candidate, Data};
candidate({add_server, URI}, #{last_applied := 0, commit_index := 0} = Data) ->
    {next_state, candidate, do_add_server(URI, Data)};
candidate({add_server, _}, Data) ->
    {next_state, candidate, Data};
candidate({remove_server, _}, Data) ->
    {next_state, candidate, Data};

candidate(rerun_election, #{term := T0, id := Id, commit_index := CI} = D0) ->
    T1 = raft_ps:increment(Id, T0),
    broadcast(
      raft_rpc:request_vote(T1, Id, CI, CI),
      D0),
    D1 = D0#{term => T1,
             voted_for => raft_ps:voted_for(Id, Id),
             for => [Id],
             against => []},
    {next_state, candidate, rerun_election(D1)};

%% If RPC request or response contains term T > currentTerm: set
%% currentTerm = T, convert to follower (§5.1)
candidate({_, #{term := T}}, #{id := Id,
                               term := CT} = Data) when T > CT ->
    {next_state, follower, call_election(
                             drop_votes(Data#{term := raft_ps:term(Id, T)}))};

%% Reply false if term < currentTerm (§5.1)
candidate({append_entries, #{term := Term,
                             prev_log_index := PrevLogIndex,
                             prev_log_term := PrevLogTerm,
                             leader := Leader}},
          #{term := Current, id := Id} = Data) when Term < Current ->
    send(
      raft_rpc:append_entries_response(
        Leader, Id, Current, PrevLogIndex, PrevLogTerm, false),
      Leader,
      Data),
    {next_state, candidate, Data};

candidate({append_entries, #{entries := [],
                             prev_log_index := PrevLogIndex,
                             prev_log_term := PrevLogTerm,
                             leader := L, term := T}},
         #{id := Id} = Data) ->
    send(
      raft_rpc:append_entries_response(
        L, Id, T, PrevLogIndex, PrevLogTerm, true),
      L,
      Data),
    {next_state, follower, call_election(Data#{term => raft_ps:term(Id, T),
                                               leader => L})};

candidate({request_vote, #{candidate := C}}, #{term := T, id := Id} = Data) ->
    send(
      raft_rpc:vote(Id, T, false),
      C,
      Data),
    {next_state, candidate, Data};

candidate({vote, #{elector := Elector, term := Term, granted := true}},
          #{for := For, term := Term, id := Id, commit_index := CI,
            connections := Connections,
            last_applied := LA, state_machine := SM} = Data) ->

    case ordsets:add_element(Elector, For) of
        Proposers when length(Proposers) > ((map_size(Connections) + 1) div 2) ->
            broadcast(
              raft_rpc:heartbeat(Term, Id, LA, raft_log:term_for_index(LA), CI),
              Data),

            (SM == undefined) andalso log(#{m => raft_sm, f => new}),
            log(#{m => raft_sm, f => system, a => [elected, #{id => Id, proposers => Proposers, term => Term}]}),
            {next_state, leader,
             end_of_term(
               Data#{for => Proposers,
                     next_indexes => maps:without([Id],
                                                  lists:foldl(
                                                    fun
                                                        (Server, A) ->
                                                            A#{Server => CI+1}
                                                    end,
                                                    #{},
                                                    Proposers)),
                     match_indexes => maps:without([Id],
                                                   lists:foldl(
                                                     fun
                                                         (Server, A) ->
                                                             A#{Server => 0}
                                                     end,
                                                     #{},
                                                     Proposers))})};

        Proposers ->
            {next_state, candidate, Data#{for => Proposers}}
    end;

candidate({vote, #{elector := Elector, term := Term, granted := false}},
          #{against := Against, term := Term} = State) ->
    {next_state, candidate, State#{against => ordsets:add_element(
                                                Elector, Against)}}.

drop_votes(#{id := Id} = Data) ->
    raft_ps:voted_for(Id, undefined),
    maps:without([voted_for, for, against, leader], Data).




leader({add_server, _}, #{change := _} = Data) ->
    %% change already in progress
    {next_state, leader, Data};
leader({remove_server, _}, #{change := _} = Data) ->
    %% change already in progress
    {next_state, leader, Data};
leader({add_server, URI}, Data) ->
    {next_state, leader, do_add_server(URI, Data)};

leader({log, Command}, #{id := Id, term := Term,
                         commit_index := CI, next_indexes := NI} = Data) ->
    LastLogIndex = raft_log:write(Term, Command),
    {next_state, leader,
     Data#{
       next_indexes := maps:fold(
                         fun
                             (Follower, Index, A) when LastLogIndex >= Index ->
                                 send(
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
                         NI)}};

%% If RPC request or response contains term T > currentTerm: set
%% currentTerm = T, convert to follower (§5.1)
leader({_, #{term := Term}},
       #{id := Id, term := Current} = Data) when Term > Current ->
    {next_state, follower, maps:without(
                             [next_indexes],
                             call_election(
                               drop_votes(
                                 Data#{term := raft_ps:term(Id, Term)})))};

%% Reply false if term < currentTerm (§5.1)
leader({append_entries, #{term := Term, leader := Leader}},
       #{term := Current,
         prev_log_index := PrevLogIndex,
         prev_log_term := PrevLogTerm,
         id := Id} = Data) when Term < Current ->
    send(
      raft_rpc:append_entries_response(
        Leader, Id, Current, PrevLogIndex, PrevLogTerm, false),
      Leader,
      Data),
    {next_state, leader, Data};


leader({append_entries, #{success := true,
                          prev_log_index := PrevLogIndex,
                          follower := Follower}},
       #{match_indexes := Match,
         next_indexes := Next,
         last_applied := LastApplied,
         state_machine := SM,
         commit_index := Commit0} = Data) ->

    case commit_index_majority(Match#{Follower => PrevLogIndex}, Commit0) of
        Commit1 when Commit1 > LastApplied ->
            {next_state,
             leader,
             Data#{state_machine => sm_apply(
                                      LastApplied + 1,
                                      Commit1,
                                      SM),
                   match_indexes := Match#{Follower => PrevLogIndex},
                   next_indexes := Next#{Follower => PrevLogIndex + 1},
                   commit_index => Commit1,
                   last_applied => Commit1}};

        _ ->
            {next_state,
             leader,
             Data#{
               match_indexes := Match#{Follower => PrevLogIndex},
               next_indexes := Next#{Follower => PrevLogIndex + 1}}}
    end;

leader(end_of_term, #{term := T0, commit_index := CI, id := Id,
                      next_indexes := NI,
                      last_applied := LA} = D0) ->
    T1 = raft_ps:increment(Id, T0),
    {next_state, leader,
     end_of_term(
       D0#{
         term => T1,
         next_indexes := maps:fold(
                           fun
                               (Follower, Index, A) when LA >= Index ->
                                   Entries = [raft_log:read(I) ||
                                                 I <-lists:seq(Index, LA)],
                                   send(
                                     raft_rpc:append_entries(
                                       T1,
                                       Id,
                                       Index-1,
                                       raft_log:term_for_index(Index-1),
                                       CI,
                                       Entries),
                                     Follower,
                                     D0),
                                   A#{Follower => LA+1};

                               (Follower, Index, A) ->
                                   send(
                                     raft_rpc:append_entries(
                                       T1,
                                       Id,
                                       Index-1,
                                       raft_log:term_for_index(Index-1),
                                       CI,
                                       []),
                                     Follower,
                                     D0),
                                   A#{Follower => Index}
                           end,
                           #{},
                           NI)})};

leader({vote, #{term := T, elector := Elector, granted := true}},
       #{term := T, commit_index := CI, next_indexes := NI} = Data) ->
    {next_state, leader, Data#{next_indexes := NI#{Elector => CI + 1}}};

leader({request_vote, #{term := Term, candidate := Candidate}},
       #{id := Id, commit_index := CI, next_indexes := NI} = Data) ->
    send(
      raft_rpc:vote(Id, Term, false),
      Candidate,
      Data),
    {next_state, leader, Data#{next_indexes := NI#{Candidate => CI + 1}}}.


end_of_term(State) ->
    after_timeout(end_of_term, raft_timeout:leader(), State).

call_election(State) ->
    after_timeout(call_election, raft_timeout:election(), State).

rerun_election(State) ->
    after_timeout(rerun_election, raft_timeout:election(), State).

after_timeout(Event, Timeout, #{timer := Timer} = State) ->
    gen_fsm:cancel_timer(Timer),
    after_timeout(Event, Timeout, maps:without([timer], State));
after_timeout(Event, Timeout, State) ->
    State#{timer => gen_fsm:send_event_after(Timeout, Event)}.


commit_index_majority(Values, CI) ->
    commit_index_majority(
      lists:reverse(ordsets:from_list(maps:values(Values))),
      maps:values(Values),
      CI).

commit_index_majority([H | T], Values, CI) when H > CI ->
    case [I || I <- Values, I >= H] of
        Majority when length(Majority) > (length(Values) / 2) ->
            H;
        _ ->
            commit_index_majority(T, Values, CI)
    end;
commit_index_majority(_, _, CI) ->
    CI.

sm_apply(LastApplied, CommitIndex, State) ->
    sm_apply(lists:seq(LastApplied, CommitIndex), State).

sm_apply([H | T], undefined) ->
    case raft_log:read(H) of
        #{command := #{m := M, f := F, a := A}} ->
            sm_apply(T, apply(M, F, A));

        #{command := #{m := M, f := F}} ->
            sm_apply(T, apply(M, F, []))
    end;

sm_apply([H | T], State) ->
    case raft_log:read(H) of
        #{command := #{m := M, f := F, a := A}} ->
            sm_apply(T, apply(M, F, A ++ [State]));

        #{command := #{m := M, f := F}} ->
            sm_apply(T, apply(M, F, [State]))
    end;

sm_apply([], State) ->
    State.

trace(false) ->
    recon_trace:clear();
trace(Fn) ->
    recon_trace:calls({?MODULE, Fn, '_'},
                      {1000, 500},
                      [{scope, local},
                       {pid, all}]).


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
              append_entries(
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

broadcast(Message, #{connections := Connections}) ->
    maps:fold(
      fun
          (_, #{sender := Sender}, _) ->
              Sender(Message);

          (_, #{}, A) ->
              A
      end,
      ok,
      Connections).
              
send(Message, Recipient, #{associations := Associations, connections := Connections}) ->
    #{Recipient := Pid} = Associations,
    #{Pid := #{sender := Sender}} = Connections,
    Sender(Message).

do_add_connection(Peer, Sender, Closer, #{connections := Connections} = Data) ->
    monitor(process, Peer),
    Data#{connections := Connections#{Peer => #{sender => Sender, closer => Closer}}}.

do_add_server(URI, #{connecting := Connecting} = Data) ->
    case http_uri:parse(URI) of
        {ok, {_, _, Host, Port, Path, _}} ->
            {ok, Peer} = gun:open(Host, Port),
            monitor(process, Peer),
            Data#{connecting := Connecting#{Peer => Path}, change => #{type => add_server, uri => URI}};

        {error, _} ->
            Data
    end.

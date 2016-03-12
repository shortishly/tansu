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
-export([append_entries/5]).
-export([append_entries/6]).
-export([connect/1]).
-export([log/1]).
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

%% states
-export([candidate/2]).
-export([follower/2]).
-export([leader/2]).


start() ->
    gen_fsm:start({local, ?MODULE}, ?MODULE, [], []).

start_link() ->
    gen_fsm:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_fsm:sync_send_all_state_event(?MODULE, stop).


connect(URI) ->
    send_all_state_event({connect, URI}).

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

send_all_state_event(Event) ->
    gen_fsm:send_all_state_event(?MODULE, Event).

send_event(Event) ->
    gen_fsm:send_event(?MODULE, Event).


init([]) ->
    raft_random:seed(),
    Id = raft_ps:id(),
    {ok, follower, call_election(voted_for(
                                   #{term => raft_ps:term(Id),
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


handle_event({connect, URI}, Name, #{connecting := Connecting} = Data) ->
    case http_uri:parse(URI) of
        {ok, {_, _, Host, Port, Path, _}} ->
            {ok, Peer} = gun:open(Host, Port),
            {next_state, Name, Data#{connecting := Connecting#{Peer => Path}}};

        {error, _} = Error ->
            {stop, Error, Data}
    end.


handle_sync_event(stop, _From, _Name, Data) ->
    {stop, normal, Data}.


handle_info({gun_down, Peer, ws, _, _, _}, Name, Data) ->
    raft_connection:delete(Peer),
    {next_state, Name, Data};

handle_info({gun_up, Peer, _}, Name, #{connecting := Connecting} = Data) ->
    case maps:find(Peer, Connecting) of
        {ok, Path} ->
            gun:ws_upgrade(Peer, Path),
            {next_state, Name, Data};

        error ->
            {stop, error, Data}
    end;

handle_info({gun_ws_upgrade, Peer, ok, _}, Name, #{connecting := C} = Data) ->
    case maps:find(Peer, C) of
        {ok, _} ->
            raft_connection:new(Peer, outgoing(Peer)),
            {next_state, Name, Data#{connecting := maps:without([Peer], C)}};

        error ->
            {stop, error, Data}
    end;

handle_info({gun_ws, Peer, {binary, Message}}, Name, Data) ->
    raft_rpc:demarshall(Peer, Message),
    {next_state, Name, Data}.


terminate(_Reason, _State, _Data) ->
    ok.

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

outgoing(Recipient) ->
    fun
        (Message) ->
            gun:ws_send(Recipient, {binary, raft_rpc:encode(Message)}),
            ok
    end.

%% If election timeout elapses without receiving AppendEntries RPC
%% from current leader or granting vote to candidate: convert to
%% candidate
follower(call_election, #{term := T0,
                          id := Id,
                          commit_index := CI} = D0) ->
    T1 = raft_ps:increment(Id, T0),
    raft_rpc:request_vote(T1, Id, CI, CI),
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
    raft_rpc:append_entries(
      Leader, Id, Current, PrevLogIndex, PrevLogTerm, false),
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
            raft_rpc:append_entries(
              L, Id, T, LastIndex, raft_log:term_for_index(LastIndex), true),
            {next_state, follower, call_election(
                                     D1#{term => raft_ps:term(Id, T),
                                         leader => L})};

        {ok, LastIndex} ->
            raft_rpc:append_entries(
              L, Id, T, LastIndex, raft_log:term_for_index(LastIndex), true),
            {next_state, follower, call_election(
                                     D0#{term => raft_ps:term(Id, T),
                                         leader => L})};

        {error, unmatched_term} ->
            raft_rpc:append_entries(L, Id, T, PrevLogIndex, PrevLogTerm, false),
            {next_state, follower, call_election(
                                     D0#{term => raft_ps:term(Id, T),
                                         leader => L})}
    end;

follower({log, Command}, #{leader := Leader} = Data) ->
    raft_rpc:log(Leader, Command),
    {next_state, follower, Data};


%% Reply false if term < currentTerm (§5.1)
follower({request_vote, #{term := Term, candidate := Candidate}},
         #{term := Current, id := Id} = Data) when Term < Current ->
    raft_rpc:vote(Candidate, Id, Current, false),
    {next_state, follower, Data};

%% If votedFor is null or candidateId, and candidate’s log is at least
%% as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
follower({request_vote, #{term := T,
                          candidate := Candidate}},
         #{id := Id, term := T, voted_for := Candidate} = Data) ->
    raft_rpc:vote(Candidate, Id, T, true),
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
            raft_rpc:vote(Candidate, Id, Term, false),
            raft_ps:voted_for(Id, undefined),
            {next_state, follower, maps:without(
                                     [voted_for],
                                     call_election(
                                       Data#{term => raft_ps:term(Id, Term)}))};

        #{term := LogTerm} when LogTerm < LastLogTerm ->
            raft_rpc:vote(Candidate, Id, Term, true),
            {next_state, follower, call_election(
                                     Data#{term => raft_ps:term(Id, Term),
                                           voted_for => raft_ps:voted_for(
                                                          Id, Candidate)})};


        #{index := LogIndex} when LastLogIndex >= LogIndex->
            raft_rpc:vote(Candidate, Id, Term, true),
            {next_state, follower, call_election(
                                     Data#{term => raft_ps:term(Id, Term),
                                           voted_for => raft_ps:voted_for(
                                                          Id, Candidate)})};

        #{index := LogIndex} when LastLogIndex < LogIndex->
            raft_rpc:vote(Candidate, Id, Term, false),
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
    raft_rpc:vote(Candidate, Id, T, false),
    {next_state, follower, Data}.


candidate(rerun_election, #{term := T0, id := Id, commit_index := CI} = D0) ->
    T1 = raft_ps:increment(Id, T0),
    raft_rpc:request_vote(T1, Id, CI, CI),
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
    raft_rpc:append_entries(
      Leader, Id, Current, PrevLogIndex, PrevLogTerm, false),
    {next_state, candidate, Data};

candidate({append_entries, #{entries := [],
                             prev_log_index := PrevLogIndex,
                             prev_log_term := PrevLogTerm,
                             leader := L, term := T}},
         #{id := Id} = Data) ->
    raft_rpc:append_entries(
      L, Id, T, PrevLogIndex, PrevLogTerm, true),
    {next_state, follower, call_election(Data#{term => raft_ps:term(Id, T),
                                               leader => L})};

candidate({request_vote, #{candidate := C}}, #{term := T, id := Id} = Data) ->
    raft_rpc:vote(C, Id, T, false),
    {next_state, candidate, Data};

candidate({vote, #{elector := Elector, term := Term, granted := true}},
          #{for := For, term := Term, id := Id, commit_index := CI,
            last_applied := LA} = Data) ->

    case {ordsets:add_element(Elector, For), raft_connection:size() + 1} of
        {Proposers, Nodes} when length(Proposers) > (Nodes / 2) ->
            raft_rpc:heartbeat(Term, Id, LA, raft_log:term_for_index(LA), CI),
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

        {Proposers, _} ->
            {next_state, candidate, Data#{for => Proposers}}
    end;

candidate({vote, #{elector := Elector, term := Term, granted := false}},
          #{against := Against, term := Term} = State) ->
    {next_state, candidate, State#{against => ordsets:add_element(
                                                Elector, Against)}}.

drop_votes(#{id := Id} = Data) ->
    raft_ps:voted_for(Id, undefined),
    maps:without([voted_for, for, against, leader], Data).




leader({log, Command}, #{id := Id, term := Term,
                         commit_index := CI, next_indexes := NI} = Data) ->
    LastLogIndex = raft_log:write(Term, Command),
    {next_state, leader,
     Data#{
       next_indexes := maps:fold(
                         fun
                             (Follower, Index, A) when LastLogIndex >= Index ->
                                 raft_rpc:append_entries(
                                   Follower,
                                   Term,
                                   Id,
                                   LastLogIndex-1,
                                   raft_log:term_for_index(LastLogIndex-1),
                                   CI,
                                   [#{term => Term, command => Command}]),
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
    raft_rpc:append_entries(
      Leader, Id, Current, PrevLogIndex, PrevLogTerm, false),
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
                                   raft_rpc:append_entries(
                                     Follower,
                                     T1,
                                     Id,
                                     Index-1,
                                     raft_log:term_for_index(Index-1),
                                     CI,
                                     Entries),
                                   A#{Follower => LA+1};

                               (Follower, Index, A) ->
                                   raft_rpc:append_entries(
                                     Follower,
                                     T1,
                                     Id,
                                     Index-1,
                                     raft_log:term_for_index(Index-1),
                                     CI,
                                     []),
                                   A#{Follower => Index}
                           end,
                           #{},
                           NI)})};

leader({vote, #{term := T, elector := Elector, granted := true}},
       #{term := T, commit_index := CI, next_indexes := NI} = Data) ->
    {next_state, leader, Data#{next_indexes := NI#{Elector => CI + 1}}};

leader({request_vote, #{term := Term, candidate := Candidate}},
       #{id := Id, commit_index := CI, next_indexes := NI} = Data) ->
    raft_rpc:vote(Candidate, Id, Term, false),
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
    #{command := #{m := M, f := F, a := A}} = raft_log:read(H),
    sm_apply(T, apply(M, F, A));
sm_apply([H | T], State) ->
    #{command := #{m := M, f := F, a := A}} = raft_log:read(H),
    sm_apply(T, apply(M, F, A ++ [State]));
sm_apply([], State) ->
    State.


trace(false) ->
    recon_trace:clear();
trace(Fn) ->
    recon_trace:calls({?MODULE, Fn, '_'},
                      {1000, 500},
                      [{scope, local},
                       {pid, all}]).

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

-module(raft_consensus_follower).
-export([add_server/2]).
-export([append_entries/2]).
-export([call_election/1]).
-export([log/2]).
-export([remove_server/2]).
-export([request_vote/2]).
-export([vote/2]).


add_server(_, #{change := _} = Data) ->
    %% change already in progress
    {next_state, follower, Data};

add_server(URI, #{last_applied := 0, commit_index := 0} = Data) ->
    {next_state, follower, raft_consensus:do_add_server(URI, Data)};

add_server(_, Data) ->
    {next_state, follower, Data}.


remove_server(_, #{change := _} = Data) ->
    %% change already in progress
    {next_state, follower, Data};
remove_server(_, Data) ->
    {next_state, follower, Data}.

%% If election timeout elapses without receiving AppendEntries RPC
%% from current leader or granting vote to candidate: convert to
%% candidate
call_election(#{term := T0, id := Id} = D0) ->
    T1 = raft_ps:increment(Id, T0),
    #{index := LastLogIndex, term := LastLogTerm} = raft_log:last(),
    raft_consensus:do_broadcast(raft_rpc:request_vote(T1, Id, LastLogIndex, LastLogTerm), D0),
    D1 = raft_consensus:do_drop_votes(D0),
    D2 = D1#{term => T1,
             voted_for => raft_ps:voted_for(Id, Id),
             for => [Id],
             against => []},
    {next_state, candidate, raft_consensus:do_rerun_election_after_timeout(D2)}.

%% Drop obsolete vote responses from earlier terms
vote(#{term := Term}, #{term := Current} = Data) when Term < Current ->
    {next_state, follower, Data};

%% an old vote for when we were a candidate
vote(#{granted := _}, Data) ->
    {next_state, follower, Data}.



%% Reply false if term < currentTerm (§5.1)
append_entries(#{term := Term,
                 leader := Leader,
                 prev_log_index := PrevLogIndex,
                 prev_log_term := PrevLogTerm,
                 entries := _},
               #{term := Current, id := Id} = Data) when Term < Current ->
    raft_consensus:do_send(
      raft_rpc:append_entries_response(
        Leader, Id, Current, PrevLogIndex, PrevLogTerm, false),
      Leader,
      Data),
    {next_state, follower, Data};

append_entries(#{entries := Entries,
                 prev_log_index := PrevLogIndex,
                 prev_log_term := PrevLogTerm,
                 leader_commit := LeaderCommit,
                 leader := L, term := T},
               #{commit_index := Commit0,
                 last_applied := LastApplied,
                 state_machine := SM,
                 id := Id} = D0) ->

    case raft_log:append_entries(PrevLogIndex, PrevLogTerm, Entries) of
        {ok, LastIndex} when LeaderCommit > Commit0 ->
            D1 = case min(LeaderCommit, LastIndex) of
                     Commit1 when Commit1 > LastApplied ->
                         D0#{state_machine => do_apply_to_state_machine(
                                                LastApplied + 1,
                                                Commit1,
                                                SM),
                             commit_index => Commit1,
                             last_applied => Commit1
                            };
                     
                     _ ->
                         D0
                 end,
            raft_consensus:do_send(
              raft_rpc:append_entries_response(
                L, Id, T, LastIndex, raft_log:term_for_index(LastIndex), true),
              L,
              D1),
            {next_state, follower, raft_consensus:do_call_election_after_timeout(
                                     D1#{term => raft_ps:term(Id, T),
                                         leader => L})};

        {ok, LastIndex} ->
            raft_consensus:do_send(
              raft_rpc:append_entries_response(
                L, Id, T, LastIndex, raft_log:term_for_index(LastIndex), true),
              L,
              D0),
            {next_state, follower, raft_consensus:do_call_election_after_timeout(
                                     D0#{term => raft_ps:term(Id, T),
                                         leader => L})};

        {error, unmatched_term} ->
            #{index := LastIndex, term := LastTerm} = raft_log:last(),
            raft_consensus:do_send(
              raft_rpc:append_entries_response(
                L, Id, T, LastIndex, LastTerm, false),
              L,
              D0),
            {next_state, follower, raft_consensus:do_call_election_after_timeout(
                                     D0#{term => raft_ps:term(Id, T),
                                         leader => L})}
    end.

log(Command, #{leader := Leader} = Data) ->
    raft_consensus:do_send(
      raft_rpc:log(Command),
      Leader,
      Data),
    {next_state, follower, Data}.


%% Reply false if term < currentTerm (§5.1)
request_vote(#{term := Term, candidate := Candidate},
             #{term := Current, id := Id} = Data) when Term < Current ->
    raft_consensus:do_send(
      raft_rpc:vote(Id, Current, false),
      Candidate,
      Data),
    {next_state, follower, Data};

%% If votedFor is null or candidateId, and candidate’s log is at least
%% as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
request_vote(#{term := T,
               candidate := Candidate},
             #{id := Id, term := T, voted_for := Candidate} = Data) ->
    raft_consensus:do_send(
      raft_rpc:vote(Id, T, true),
      Candidate,
      Data),
    {next_state, follower, raft_consensus:do_call_election_after_timeout(Data)};


%% If votedFor is null or candidateId, and candidate’s log is at least
%% as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
request_vote(#{term := Term,
               candidate := Candidate,
               last_log_index := LastLogIndex,
               last_log_term := LastLogTerm},
             #{term := Current, id := Id} = Data) when (Term >= Current) ->

    %% Raft determines which of two logs is more up-to-date by
    %% comparing the index and term of the last entries in the
    %% logs. If the logs have last entries with different terms, then
    %% the log with the later term is more up-to-date. If the logs end
    %% with the same term, then whichever log is longer is more
    %% up-to-date.
    case raft_log:last() of
        #{term := LogTerm} when LogTerm > LastLogTerm ->
            raft_consensus:do_send(
              raft_rpc:vote(Id, Term, false),
              Candidate,
              Data),
            raft_ps:voted_for(Id, undefined),
            {next_state, follower, maps:without(
                                     [voted_for],
                                     raft_consensus:do_call_election_after_timeout(
                                       Data#{term => raft_ps:term(Id, Term)}))};

        #{term := LogTerm} when LogTerm < LastLogTerm ->
            raft_consensus:do_send(
              raft_rpc:vote(Id, Term, true),
              Candidate,
              Data),
            {next_state, follower, raft_consensus:do_call_election_after_timeout(
                                     Data#{term => raft_ps:term(Id, Term),
                                           voted_for => raft_ps:voted_for(
                                                          Id, Candidate)})};


        #{index := LogIndex} when LastLogIndex >= LogIndex->
            raft_consensus:do_send(
              raft_rpc:vote(Id, Term, true),
              Candidate,
              Data),
            {next_state, follower, raft_consensus:do_call_election_after_timeout(
                                     Data#{term => raft_ps:term(Id, Term),
                                           voted_for => raft_ps:voted_for(
                                                          Id, Candidate)})};

        #{index := LogIndex} when LastLogIndex < LogIndex->
            raft_consensus:do_send(
              raft_rpc:vote(Id, Term, false),
              Candidate,
              Data),
            raft_ps:voted_for(Id, undefined),
            {next_state, follower, maps:without(
                                     [voted_for],
                                     raft_consensus:do_call_election_after_timeout(
                                       Data#{term => raft_ps:term(Id, Term)}))}
    end;

%% If votedFor is null or candidateId, and candidate’s log is at least
%% as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
request_vote(#{term := T, candidate := Candidate},
             #{id := Id, term := T, voted_for := _} = Data) ->
    raft_consensus:do_send(
      raft_rpc:vote(Id, T, false),
      Candidate,
      Data),
    {next_state, follower, Data}.


do_apply_to_state_machine(LastApplied, CommitIndex, State) ->
    do_apply_to_state_machine(lists:seq(LastApplied, CommitIndex), State).

do_apply_to_state_machine([H | T], undefined) ->
    case raft_log:read(H) of
        #{command := #{f := F, a := A}} ->
            {_, State} = apply(raft_config:sm(), F, A),
            do_apply_to_state_machine(T, State);

        #{command := #{f := F}} ->
            {_, State} = apply(raft_config:sm(), F, []),
            do_apply_to_state_machine(T, State)
    end;

do_apply_to_state_machine([H | T], S0) ->
    case raft_log:read(H) of
        #{command := #{f := F, a := A}} ->
            {_, S1} = apply(raft_config:sm(), F, A ++ [S0]),
            do_apply_to_state_machine(T, S1);

        #{command := #{f := F}} ->
            {_, S1} = apply(raft_config:sm(), F, [S0]),
            do_apply_to_state_machine(T, S1)
    end;

do_apply_to_state_machine([], State) ->
    State.

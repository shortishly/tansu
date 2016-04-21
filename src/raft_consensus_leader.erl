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

-module(raft_consensus_leader).
-export([add_server/2]).
-export([append_entries/2]).
-export([append_entries_response/2]).
-export([end_of_term/1]).
-export([log/2]).
-export([remove_server/2]).
-export([request_vote/2]).
-export([vote/2]).

add_server(_, #{change := _} = Data) ->
    %% change already in progress
    {next_state, leader, Data};
add_server(URI, Data) ->
    {next_state, leader, raft_consensus:do_add_server(URI, Data)}.

remove_server(_, #{change := _} = Data) ->
    %% change already in progress
    {next_state, leader, Data}.

log(Command, Data) ->
    {next_state, leader, raft_consensus:do_log(Command, Data)}.

%% If RPC request or response contains term T > currentTerm: set
%% currentTerm = T, convert to follower (§5.1)
%% TODO
append_entries(#{term := Term}, #{id := Id, term := Current} = Data) when Term > Current ->
    {next_state, follower, maps:without(
                             [match_indexes, next_indexes],
                             raft_consensus:do_call_election_after_timeout(
                               raft_consensus:do_drop_votes(
                                 Data#{term := raft_ps:term(Id, Term)})))};

%% Reply false if term < currentTerm (§5.1)
append_entries(#{term := Term, leader := Leader},
               #{term := Current,
                 prev_log_index := PrevLogIndex,
                 prev_log_term := PrevLogTerm,
                 id := Id} = Data) when Term < Current ->
    raft_consensus:do_send(
      raft_rpc:append_entries_response(
        Leader, Id, Current, PrevLogIndex, PrevLogTerm, false),
      Leader,
      Data),
    {next_state, leader, Data}.


append_entries_response(#{success := false,
                          prev_log_index := LastIndex,
                          follower := Follower},
                        #{next_indexes := NextIndexes} = Data) ->
    {next_state, leader, Data#{next_indexes := NextIndexes#{Follower := LastIndex + 1}}};

append_entries_response(#{success := true,
                          prev_log_index := PrevLogIndex,
                          follower := Follower},
                        #{match_indexes := Match,
                          next_indexes := Next,
                          last_applied := LastApplied,
                          state_machine := SM,
                          commit_index := Commit0} = Data) ->

    case commit_index_majority(Match#{Follower => PrevLogIndex}, Commit0) of
        Commit1 when Commit1 > LastApplied ->
            {next_state,
             leader,
             Data#{state_machine => raft_consensus:do_apply_to_state_machine(
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
    end.

end_of_term(#{term := T0, commit_index := CI, id := Id,
              next_indexes := NI,
              last_applied := LA} = D0) ->
    T1 = raft_ps:increment(Id, T0),
    {next_state, leader,
     raft_consensus:do_end_of_term_after_timeout(
       D0#{
         term => T1,
         next_indexes := maps:fold(
                           fun
                               (Follower, Index, A) when LA >= Index ->
                                   Entries = [raft_log:read(I) ||
                                                 I <-lists:seq(Index, LA)],
                                   raft_consensus:do_send(
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
                                   raft_consensus:do_send(
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
                           NI)})}.

vote(#{term := T, elector := Elector, granted := true},
     #{term := T, commit_index := CI, next_indexes := NI} = Data) ->
    {next_state, leader, Data#{next_indexes := NI#{Elector => CI + 1}}}.

request_vote(#{term := Term, candidate := Candidate},
             #{id := Id, commit_index := CI, next_indexes := NI} = Data) ->
    raft_consensus:do_send(
      raft_rpc:vote(Id, Term, false),
      Candidate,
      Data),
    {next_state, leader, Data#{next_indexes := NI#{Candidate => CI + 1}}}.





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

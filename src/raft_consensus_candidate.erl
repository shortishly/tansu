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

-module(raft_consensus_candidate).
-export([add_server/2]).
-export([append_entries/2]).
-export([remove_server/2]).
-export([request_vote/2]).
-export([rerun_election/1]).
-export([vote/2]).


add_server(_, #{change := _} = Data) ->
    %% change already in progress
    {next_state, candidate, Data};
add_server(URI, #{last_applied := 0, commit_index := 0} = Data) ->
    {next_state, candidate, raft_consensus:do_add_server(URI, Data)};
add_server(_, Data) ->
    {next_state, candidate, Data}.


remove_server(_, #{change := _} = Data) ->
    %% change already in progress
    {next_state, candidate, Data};
remove_server(_, Data) ->
    {next_state, candidate, Data}.

rerun_election(#{term := T0, id := Id, commit_index := CI} = D0) ->
    T1 = raft_ps:increment(Id, T0),
    raft_consensus:do_broadcast(
      raft_rpc:request_vote(T1, Id, CI, CI),
      D0),
    D1 = D0#{term => T1,
             voted_for => raft_ps:voted_for(Id, Id),
             for => [Id],
             against => []},
    {next_state, candidate, raft_consensus:do_rerun_election_after_timeout(D1)}.

%% If RPC request or response contains term T > currentTerm: set
%% currentTerm = T, convert to follower (§5.1)
append_entries(#{term := T}, #{id := Id,
                               term := CT} = Data) when T > CT ->
    {next_state, follower, raft_consensus:do_call_election_after_timeout(
                             raft_consensus:do_drop_votes(Data#{term := raft_ps:term(Id, T)}))};

%% Reply false if term < currentTerm (§5.1)
append_entries(#{term := Term,
                 prev_log_index := PrevLogIndex,
                 prev_log_term := PrevLogTerm,
                 leader := Leader},
               #{term := Current, id := Id} = Data) when Term < Current ->
    raft_consensus:do_send(
      raft_rpc:append_entries_response(
        Leader, Id, Current, PrevLogIndex, PrevLogTerm, false),
      Leader,
      Data),
    {next_state, candidate, Data};

append_entries(#{entries := [],
                 prev_log_index := PrevLogIndex,
                 prev_log_term := PrevLogTerm,
                 leader := L, term := T},
               #{id := Id} = Data) ->
    raft_consensus:do_send(
      raft_rpc:append_entries_response(
        L, Id, T, PrevLogIndex, PrevLogTerm, true),
      L,
      Data),
    {next_state, follower, raft_consensus:do_call_election_after_timeout(
                             Data#{term => raft_ps:term(Id, T), leader => L})}.

request_vote(#{candidate := C}, #{term := T, id := Id} = Data) ->
    raft_consensus:do_send(
      raft_rpc:vote(Id, T, false),
      C,
      Data),
    {next_state, candidate, Data}.


%% If RPC request or response contains term T > currentTerm: set
%% currentTerm = T, convert to follower (§5.1)
vote(#{term := T}, #{id := Id, term := CT} = Data) when T > CT ->
    {next_state, follower, raft_consensus:do_call_election_after_timeout(
                             raft_consensus:do_drop_votes(Data#{term := raft_ps:term(Id, T)}))};

vote(#{elector := Elector, term := Term, granted := true},
     #{for := For, against := Against, term := Term} = Data) ->

    Quorum = raft_consensus:quorum(Data),

    case ordsets:add_element(Elector, For) of
        Proposers when (length(Proposers) >= Quorum) andalso (length(Proposers) > length(Against)) ->
            {next_state, leader, appoint_leader(Data#{for := Proposers})};

        Proposers ->
            {next_state, candidate, Data#{for := Proposers}}
    end;

vote(#{elector := Elector, term := Term, granted := false},
     #{against := Against, term := Term} = State) ->
    {next_state, candidate, State#{against => ordsets:add_element(
                                                Elector, Against)}}.


appoint_leader(#{term := Term,
                 for := For,
                 id := Id,
                 commit_index := CI,
                 last_applied := LA} = Data) ->
    raft_consensus:do_broadcast(
      raft_rpc:heartbeat(Term, Id, LA, raft_log:term_for_index(LA), CI),
      Data),
    raft_consensus:do_end_of_term_after_timeout(
      maybe_init_log(
        Data#{next_indexes => maps:without([Id],
                                         lists:foldl(
                                           fun
                                               (Server, A) ->
                                                   A#{Server => CI+1}
                                           end,
                                           #{},
                                           For)),
            match_indexes => maps:without([Id],
                                          lists:foldl(
                                            fun
                                                (Server, A) ->
                                                    A#{Server => 0}
                                            end,
                                            #{},
                                            For))})).


maybe_init_log(#{state_machine := undefined} = Data) ->
    raft_consensus:do_log(#{m => raft_sm,
                            f => ckv_set,
                            a => [system, cluster, raft_uuid:new()]},
                          raft_consensus:do_log(#{m => raft_sm,
                                                  f => new}, Data));

maybe_init_log(#{state_machine := _} = Data) ->
    Data.


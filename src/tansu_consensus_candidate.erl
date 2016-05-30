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

-module(tansu_consensus_candidate).
-export([add_server/2]).
-export([append_entries/2]).
-export([append_entries_response/2]).
-export([ping/2]).
-export([remove_server/2]).
-export([request_vote/2]).
-export([rerun_election/1]).
-export([transition_to_follower/1]).
-export([vote/2]).


add_server(_, #{change := _} = Data) ->
    %% change already in progress
    {next_state, candidate, Data};

add_server(URI, Data) ->
    {next_state, candidate, tansu_consensus:do_add_server(URI, Data)}.

ping(_, Data) ->
    {next_state, candidate, Data}.


remove_server(_, #{change := _} = Data) ->
    %% change already in progress
    {next_state, candidate, Data};
remove_server(_, Data) ->
    {next_state, candidate, Data}.

rerun_election(#{term := T0, id := Id, commit_index := CI} = D0) ->
    T1 = tansu_ps:increment(Id, T0),
    tansu_consensus:do_broadcast(
      tansu_rpc:request_vote(T1, Id, CI, CI),
      D0),
    D1 = D0#{term => T1,
             voted_for => tansu_ps:voted_for(Id, Id),
             for => [Id],
             against => []},
    {next_state, candidate, tansu_consensus:do_rerun_election_after_timeout(D1)}.

%% If RPC request or response contains term T > currentTerm: set
%% currentTerm = T, convert to follower (§5.1)
append_entries(#{term := T}, #{id := Id,
                               term := CT} = Data) when T > CT ->
    {next_state, follower, tansu_consensus:do_call_election_after_timeout(
                             tansu_consensus:do_drop_votes(Data#{term := tansu_ps:term(Id, T)}))};

%% Reply false if term < currentTerm (§5.1)
append_entries(#{term := Term,
                 prev_log_index := PrevLogIndex,
                 prev_log_term := PrevLogTerm,
                 leader := Leader},
               #{term := Current, id := Id} = Data) when Term < Current ->
    tansu_consensus:do_send(
      tansu_rpc:append_entries_response(
        Leader, Id, Current, PrevLogIndex, PrevLogTerm, false),
      Leader,
      Data),
    {next_state, candidate, Data};

append_entries(#{entries := [],
                 prev_log_index := PrevLogIndex,
                 prev_log_term := PrevLogTerm,
                 leader := L, term := T},
               #{id := Id} = Data) ->
    tansu_consensus:do_send(
      tansu_rpc:append_entries_response(
        L, Id, T, PrevLogIndex, PrevLogTerm, true),
      L,
      Data),
    {next_state, follower, maps:without([voted_for,
                                         for,
                                         against],
                                        tansu_consensus:do_call_election_after_timeout(
                                          tansu_consensus:do_drop_votes(
                                            Data#{term => tansu_ps:term(Id, T), leader => L})))}.


append_entries_response(#{term := Term}, #{term := Current} = Data) when Term < Current ->
    {next_state, candidate, Data}.


request_vote(#{term := T0}, #{term := T1, id := Id} = Data) when T0 > T1 ->
    {next_state,
     follower,
     tansu_consensus:do_call_election_after_timeout(
       tansu_consensus:do_drop_votes(Data#{term := tansu_ps:term(Id, T0)}))};

request_vote(#{candidate := C}, #{term := T, id := Id} = Data) ->
    {next_state,
     candidate,
     tansu_consensus:do_send(
       tansu_rpc:vote(Id, T, false),
       C,
       Data)}.


%% Ignore votes from earlier terms.
vote(#{term := T}, #{term := CT} = Data) when T < CT ->
    {next_state, candidate, Data};

%% If RPC request or response contains term T > currentTerm: set
%% currentTerm = T, convert to follower (§5.1)
vote(#{term := T}, #{id := Id, term := CT} = Data) when T > CT ->
    {next_state, follower, tansu_consensus:do_call_election_after_timeout(
                             tansu_consensus:do_drop_votes(Data#{term := tansu_ps:term(Id, T)}))};

vote(#{elector := Elector, term := Term, granted := true},
     #{for := For,
       connections := Connections,
       associations := Associations,
       term := Term} = Data) when map_size(Connections) == map_size(Associations) ->

    Members = map_size(Associations) + 1,
    Quorum = tansu_consensus:quorum(Data),

    case ordsets:add_element(Elector, For) of
        Proposers when (Members >= Quorum) andalso (length(Proposers) > (Members div 2)) ->
            {next_state, leader, appoint_leader(Data#{for := Proposers})};

        Proposers ->
            {next_state, candidate, Data#{for := Proposers}}
    end;

vote(#{elector := Elector, term := Term, granted := true},
     #{for := For,
       term := Term} = Data) ->
    {next_state, candidate, Data#{for := ordsets:add_element(Elector, For)}};


vote(#{elector := Elector, term := Term, granted := false},
     #{against := Against, term := Term} = State) ->
    {next_state, candidate, State#{against => ordsets:add_element(
                                                Elector, Against)}}.



appoint_leader(#{term := Term,
                 associations := Associations,
                 id := Id,
                 commit_index := CI,
                 last_applied := LA} = Data) ->
    tansu_consensus:do_broadcast(
      tansu_rpc:heartbeat(Term, Id, LA, tansu_log:term_for_index(LA), CI),
      Data),
    tansu_consensus:do_end_of_term_after_timeout(
      maybe_init_log(
        Data#{next_indexes => lists:foldl(
                                fun
                                    (Server, A) ->
                                        A#{Server => CI+1}
                                end,
                                #{},
                                maps:keys(Associations)),
            match_indexes => lists:foldl(
                               fun
                                   (Server, A) ->
                                       A#{Server => 0}
                               end,
                               #{},
                               maps:keys(Associations))})).


maybe_init_log(#{state_machine := undefined} = Data) ->
    tansu_consensus:do_log(
      #{m => tansu_sm,
        f => ckv_set,
        a => [system, [<<"cluster">>], tansu_uuid:new(), #{}]},
      tansu_consensus:do_log(
        #{m => tansu_sm,
          f => new}, Data));

maybe_init_log(#{state_machine := _} = Data) ->
    Data.


transition_to_follower(Data) ->
    tansu_consensus:do_call_election_after_timeout(
      tansu_consensus:do_drop_votes(Data)).

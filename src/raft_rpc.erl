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

-module(raft_rpc).
-export([append_entries/6]).
-export([append_entries/7]).
-export([demarshall/2]).
-export([heartbeat/5]).
-export([log/2]).
-export([request_vote/4]).
-export([vote/4]).



request_vote(Term, Candidate, LastLogIndex, LastLogTerm) ->
    raft_connection:broadcast(
      #{request_vote => #{
          term => Term,
          candidate => Candidate,
          last_log_index => LastLogIndex,
          last_log_term => LastLogTerm}}).


append_entries(Leader, Follower, Term, PrevLogIndex, PrevLogTerm, Success) ->
    raft_connection:send(Leader, #{append_entries => #{
                                     term => Term,
                                     prev_log_index => PrevLogIndex,
                                     prev_log_term => PrevLogTerm,
                                     follower => Follower,
                                     leader => Leader,
                                     success => Success}}).

append_entries(Follower, LeaderTerm, Leader, LastApplied, PrevLogTerm,
               LeaderCommitIndex, Entries) ->
    raft_connection:send(Follower,
                         #{append_entries => #{
                             term => LeaderTerm,
                             leader => Leader,
                             prev_log_index =>LastApplied,
                             prev_log_term => PrevLogTerm,
                             entries => Entries,
                             leader_commit => LeaderCommitIndex}}).





log(Leader, Command) ->
    raft_connection:send(Leader, #{log => Command}).


vote(Candidate, Elector, Term, Granted) ->
    raft_connection:send(Candidate, #{vote => #{elector => Elector,
                                                term => Term,
                                                granted => Granted}}).


heartbeat(LeaderTerm, Leader, LastApplied, PrevLogTerm, LeaderCommitIndex) ->
    raft_connection:broadcast(
      #{append_entries => #{term => LeaderTerm,
                            leader => Leader,
                            prev_log_index =>LastApplied,
                            prev_log_term => PrevLogTerm,
                            entries => [],
                            leader_commit => LeaderCommitIndex}}).


demarshall(Pid, Message) ->
    case decode(Message) of
        #{request_vote := #{term := Term,
                            candidate := Candidate,
                            last_log_index := LastLogIndex,
                            last_log_term := LastLogTerm}} ->

            raft_connection:associate(Pid, Candidate),
            raft_consensus:request_vote(
              Term, Candidate, LastLogIndex, LastLogTerm);

        #{vote := #{elector := Elector,
                    term := Term,
                    granted := Granted}} ->
            raft_connection:associate(Pid, Elector),
            raft_consensus:vote(Elector, Term, Granted);

        #{append_entries := #{term := LeaderTerm,
                              leader := Leader,
                              prev_log_index := LastApplied,
                              prev_log_term := PrevLogTerm,
                              entries := Entries,
                              leader_commit := LeaderCommitIndex}} ->
            raft_connection:associate(Pid, Leader),
            raft_consensus:append_entries(
              LeaderTerm,
              Leader,
              LastApplied,
              PrevLogTerm,
              Entries,
              LeaderCommitIndex);

        #{append_entries := #{term := Term,
                              leader := _Leader,
                              prev_log_index := PrevLogIndex,
                              prev_log_term := PrevLogTerm,
                              follower := Follower,
                              success := Success}} ->
            raft_connection:associate(Pid, Follower),
            raft_consensus:append_entries(
              Follower, Term, Success, PrevLogIndex, PrevLogTerm);

        #{log := Command} ->
            raft_consensus:log(Command)

    end.

decode(Message) ->
    jsx:decode(Message, [return_maps, {labels, existing_atom}]).

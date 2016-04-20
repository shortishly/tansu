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
-export([append_entries_response/6]).
-export([decode/1]).
-export([encode/1]).
-export([heartbeat/5]).
-export([log/1]).
-export([request_vote/4]).
-export([vote/3]).


request_vote(Term, Candidate, LastLogIndex, LastLogTerm) ->
    #{request_vote => #{
        term => Term,
        candidate => Candidate,
        last_log_index => LastLogIndex,
        last_log_term => LastLogTerm}}.


append_entries_response(Leader, Follower, Term, PrevLogIndex, PrevLogTerm, Success) ->
    #{append_entries_response => #{
        term => Term,
        prev_log_index => PrevLogIndex,
        prev_log_term => PrevLogTerm,
        follower => Follower,
        leader => Leader,
        success => Success}}.

append_entries(LeaderTerm, Leader, LastApplied, PrevLogTerm,
               LeaderCommitIndex, Entries) ->
    #{append_entries => #{
        term => LeaderTerm,
        leader => Leader,
        prev_log_index =>LastApplied,
        prev_log_term => PrevLogTerm,
        entries => Entries,
        leader_commit => LeaderCommitIndex}}.

log(Command) ->
    #{log => Command}.


vote(Elector, Term, Granted) ->
    #{vote => #{elector => Elector,
                term => Term,
                granted => Granted}}.


heartbeat(LeaderTerm, Leader, LastApplied, PrevLogTerm, LeaderCommitIndex) ->
    #{append_entries => #{term => LeaderTerm,
                          leader => Leader,
                          prev_log_index =>LastApplied,
                          prev_log_term => PrevLogTerm,
                          entries => [],
                          leader_commit => LeaderCommitIndex}}.


decode(<<Size:32, BERT:Size/bytes>>) ->
    binary_to_term(BERT).


encode(Term) ->
    BERT = term_to_binary(Term),
    <<(byte_size(BERT)):32, BERT/binary>>.

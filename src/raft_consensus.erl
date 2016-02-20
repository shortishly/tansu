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
-export([append_entries/6]).
-export([connect/1]).
-export([request_vote/4]).
-export([start_link/0]).
-export([vote/3]).

%% gen_server.
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


-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_fsm:start_link({local, ?MODULE}, ?MODULE, [], []).

connect(URI) ->
    gen_fsm:send_all_state_event(?MODULE, {connect, URI}).

append_entries(LeaderTerm,
               Leader,
               LastApplied,
               PrevLogTerm,
               Entries,
               LeaderCommitIndex) ->
    gen_fsm:send_event(?MODULE,
                       {append_entries,
                        #{term => LeaderTerm,
                          leader => Leader,
                          prev_log_index => LastApplied,
                          prev_log_term => PrevLogTerm,
                          entries => Entries,
                          leader_commit => LeaderCommitIndex}}).

request_vote(Term, Candidate, LastLogIndex, LastLogTerm) ->
    gen_fsm:send_event(
      ?MODULE,
      {request_vote,
       #{term => Term,
         candidate => Candidate,
         last_log_index => LastLogIndex,
         last_log_term => LastLogTerm}}).

vote(Elector, Term, Granted) ->
    gen_fsm:send_event(
      ?MODULE,
      {vote, #{elector => Elector,
               term => Term,
               granted => Granted}}).


init([]) ->
    raft_random:seed(),
    Id = raft_ps:id(),
    D0 = #{term => raft_ps:term(Id),
           id => Id,
           commit_index => raft_log:commit_index(),
           last_applied => 0,
           connecting => #{}},
    D1 = case raft_ps:voted_for(Id) of
             undefined ->
                 D0;
             VotedFor ->
                 D0#{voted_for => VotedFor}
         end,
    {ok, follower, call_election(D1)}.




handle_event({connect, URI}, Name, #{connecting := Connecting} = Data) ->
    case http_uri:parse(URI) of
        {ok, {_, _, Host, Port, Path, _}} ->
            {ok, Peer} = gun:open(Host, Port),
            {next_state, Name, Data#{connecting := Connecting#{Peer => Path}}};

        {error, _} = Error ->
            {stop, Error, Data}
    end.


handle_sync_event(_Event, _From, _Name, Data) ->
    {stop, error, Data}.


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

handle_info({gun_ws, Peer, {text, Message}}, Name, Data) ->
    raft_rpc:demarshall(Peer, Message),
    {next_state, Name, Data}.


terminate(_Reason, _State, _Data) ->
    ok.

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

outgoing(Recipient) ->
    fun
        (Message) ->
            gun:ws_send(Recipient, {text, jsx:encode(Message)}),
            ok
    end.


%% if RPC request or response contains term > current_term, set
%% current_term = term, convert to follower.
follower({_, #{term := T}}, #{term := C, id := Id} = D0) when T > C ->
    {next_state, follower, drop_votes(D0#{term => raft_ps:term(Id, T)})};

%% If election timeout elapses without receiving AppendEntries RPC
%% from current leader or granting vote to candidate: convert to
%% candidate
follower(call_election, #{term := T0, id := Id, commit_index := CI} = D0) ->
    T1 = raft_ps:increment(Id, T0),
    raft_rpc:request_vote(T1, Id, CI, CI),
    D1 = D0#{term => T1,
             voted_for => Id,
             for => [Id],
             against => []},
    {next_state, candidate, rerun_election(D1)};

%% Reply false if term < currentTerm (§5.1)
follower({append_entries, #{term := Term, leader := Leader}},
         #{term := Current} = Data) when Term < Current ->
    raft_rpc:append_entries(Leader, Current, false),
    {next_state, follower, Data};

follower({append_entries, #{entries := [], leader := L, term := T}},
         #{id := Id} = Data) ->
    raft_rpc:append_entries(L, T, true),
    {next_state, follower, call_election(Data#{term => raft_ps:term(Id, T),
                                               leader => L})};


%% Reply false if term < currentTerm (§5.1)
follower({request_vote, #{term := Term, candidate := Candidate}},
         #{term := Current, id := Id} = Data) when Term < Current ->
    raft_rpc:vote(Candidate, Id, Current, false),
    {next_state, follower, Data};

%% If votedFor is null or candidateId, and candidate’s log is at least
%% as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
follower({request_vote, #{term := Term, candidate := Candidate}},
         #{id := Id, voted_for := Candidate} = Data) ->
    raft_rpc:vote(Candidate, Id, Term, true),
    {next_state, follower, call_election(Data)};

%% If votedFor is null or candidateId, and candidate’s log is at least
%% as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
follower({request_vote, #{term := Term, candidate := Candidate}},
         #{id := Id, voted_for := _} = Data) ->
    raft_rpc:vote(Candidate, Id, Term, false),
    {next_state, follower, Data};

%% If votedFor is null or candidateId, and candidate’s log is at least
%% as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
follower({request_vote, #{term := Term, candidate := Candidate}},
         #{id := Id} = Data) ->
    raft_rpc:vote(Candidate, Id, Term, true),
    {next_state, follower, call_election(
                             Data#{voted_for => raft_ps:voted_for(
                                                  Id, Candidate)})}.




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
%%
candidate({_, #{term := T}}, #{id := Id, term := CT} = Data) when T > CT ->
    {next_state, follower, call_election(
                             drop_votes(Data#{term := raft_ps:term(Id, T)}))};

candidate({request_vote, #{candidate := C}}, #{term := T, id := Id} = Data) ->
    raft_rpc:vote(C, Id, T, false),
    {next_state, candidate, Data};

candidate({vote, #{elector := Elector, term := Term, granted := true}},
          #{for := For, term := Term, id := Id, commit_index := CI,
            last_applied := LA} = Data) ->

    case {ordsets:add_element(Elector, For), raft_connection:size()} of
        {Proposers, Nodes} when length(Proposers) > (Nodes / 2) ->
            raft_rpc:heartbeat(Term, Id, LA, Term, CI),
            {next_state, leader, end_of_term(drop_votes(Data))};

        {Proposers, _} ->
            {next_state, candidate, Data#{for => Proposers}}
    end;

candidate({vote, #{elector := Elector, term := Term, granted := false}},
          #{against := Against, term := Term} = State) ->
    {next_state, candidate, State#{against => ordsets:add_element(
                                                Elector, Against)}}.

drop_votes(#{id := Id} = Data) ->
    raft_ps:voted_for(Id, undefined),
    maps:without([voted_for, for, against], Data).



%% If RPC request or response contains term T > currentTerm: set
%% currentTerm = T, convert to follower (§5.1)
%%
leader({_, #{term := Term}},
       #{id := Id, term := Current} = Data) when Term > Current ->
    {next_state, follower, call_election(
                             drop_votes(
                               Data#{term := raft_ps:term(Id, Term)}))};


leader(end_of_term, #{term := T0, commit_index := CI, id := Id,
                      last_applied := LA} = D0) ->
    T1 = raft_ps:increment(Id, T0),
    raft_rpc:heartbeat(T1, Id, LA, T0, CI),
    D1 = D0#{term => T1},
    {next_state, leader, end_of_term(D1)};

leader({vote, #{term := T, granted := true}}, #{term := T,
                                                voted_for := _} = Data) ->
    {next_state, leader, Data};

leader({request_vote, #{term := Term, candidate := Candidate}},
       #{id := Id, voted_for := _} = Data) ->
    raft_rpc:vote(Candidate, Id, Term, false),
    {next_state, leader, Data}.


end_of_term(State) ->
    after_timeout(end_of_term, State).

call_election(State) ->
    after_timeout(call_election, State).


rerun_election(State) ->
    after_timeout(rerun_election, State).

after_timeout(Event, #{timer := Timer} = State) ->
    gen_fsm:cancel_timer(Timer),
    after_timeout(Event, maps:without([timer], State));
after_timeout(Event, State) ->
    State#{timer => gen_fsm:send_event_after(
                      raft_timeout:election(),
                      Event)}.

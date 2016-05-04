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

-module(kv_simple).

-export([initial_state/0]).
-export([initial_state_data/0]).
-export([kv/1]).
-export([next_state_data/5]).
-export([postcondition/5]).
-export([precondition/4]).
-export([prop_kv/1]).
-export([set/2]).

-include_lib("triq/include/triq.hrl").

initial_state() ->
    kv.

initial_state_data() ->
    #{}.

kv(_State) ->
    [{kv, {call, ?MODULE, set, [key(), value()]}}].

key() ->
    atom(1).

value() ->
    choose(0,9).


set(Key, Value) ->
    kvc:set(Key, Value).

precondition(_From, _Target, _StateData, {call, _, _, _}) ->
    true.

postcondition(_From, _Target, _StateData, {call, _, _, _} = _Op, ok = Result) ->
    Result =:= ok;
postcondition(From, Target, StateData, {call, _, _, _} = Op, Result) ->
    ct:log("~p", [{From, Target, StateData, Op, Result}]),
    Result =:= ok.

next_state_data(_From, _Target, StateData, _Result, {call, ?MODULE, set, [Key, Value]}) ->
    StateData#{Key => Value}.


prop_kv(_Config) ->
    ?FORALL(
       Cmds, triq_fsm:commands(?MODULE),
       begin
           ?WHENFAIL(ct:log("Cmds: ~p", [Cmds]), begin
           {History, State, Result} = triq_fsm:run_commands(?MODULE, Cmds),
           ?WHENFAIL(ct:log("History: ~p~nState: ~p~nResult: ~p~n",
                            [History, State, Result]),
                     Result =:= ok)
                                                 end)
       end).

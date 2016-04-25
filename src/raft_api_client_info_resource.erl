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

-module(raft_api_client_info_resource).

-export([init/2]).

init(Req, Opts) ->
    [Major, Minor, Patch] = string:tokens(raft:vsn(), "."),
    {ok,
     cowboy_req:reply(
       200,
       [{<<"content-type">>, <<"application/json">>}],
       jsx:encode(
         #{applications => lists:foldl(
                             fun
                                 ({Application, _, VSN}, A) ->
                                     A#{Application => any:to_binary(VSN)}
                             end,
                             #{},
                             application:which_applications()),
           cluster => raft_consensus:info(),
           version => #{major => any:to_integer(Major),
                        minor => any:to_integer(Minor),
                        patch => any:to_integer(Patch)}}),
       Req),
     Opts}.



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

-module(tansu_api_info_resource).

-export([allowed_methods/2]).
-export([content_types_provided/2]).
-export([init/2]).
-export([options/2]).
-export([to_json/2]).


init(Req, _) ->
    {cowboy_rest, cors:allow_origin(Req), #{}}.

allowed_methods(Req, State) ->
    {allowed(), Req, State}.

options(Req, State) ->
    cors:options(Req, State, allowed()).

content_types_provided(Req, State) ->
    {[{{<<"application">>, <<"json">>, '*'}, to_json}], Req, State}.

allowed() ->
    [<<"GET">>,
     <<"HEAD">>,
     <<"OPTIONS">>].

to_json(Req, State) ->
    [Major, Minor, Patch] = string:tokens(tansu:vsn(), "."),
    {jsx:encode(
       #{applications => lists:foldl(
                           fun
                               ({Application, _, VSN}, A) ->
                                   A#{Application => any:to_binary(VSN)}
                           end,
                           #{},
                           application:which_applications()),
         consensus => tansu_consensus:info(),
         version => #{major => any:to_integer(Major),
                      minor => any:to_integer(Minor),
                      patch => any:to_integer(Patch)}}),
     Req,
     State}.



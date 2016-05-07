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

-module(tansu_api_server_resource).

-export([init/2]).
-export([terminate/3]).
-export([websocket_handle/3]).
-export([websocket_info/3]).


init(Req, State) ->
    TansuId = cowboy_req:header(<<"tansu-id">>, Req),
    {PeerIP, _} = cowboy_req:peer(Req),
    TansuPort = cowboy_req:header(<<"tansu-port">>, Req),
    init(Req, TansuId, inet:ntoa(PeerIP), TansuPort, State).


init(Req, TansuId, TansuHost, TansuPort, State) when TansuId == undefined orelse
                                                     TansuHost == undefined orelse
                                                     TansuPort == undefined ->
    {ok, cowboy_req:reply(400, Req), State};

init(Req, TansuId, TansuHost, TansuPort, State) ->
    tansu_consensus:add_connection(
      self(),
      TansuId,
      TansuHost,
      any:to_integer(TansuPort),
      outgoing(self()),
      closer(self())),
    {cowboy_websocket, Req, State}.


websocket_handle({binary, Message}, Req, State) ->
    tansu_consensus:demarshall(self(), tansu_rpc:decode(Message)),
    {ok, Req, State}.

websocket_info({message, Message}, Req, State) ->
    {reply, {binary, tansu_rpc:encode(Message)}, Req, State};

websocket_info(close, Req, State) ->
    {stop, Req, State}.

terminate(_Reason, _Req, _State) ->
    ok.

outgoing(Recipient) ->
    fun(Message) ->
            Recipient ! {message, Message},
            ok
    end.

closer(Recipient) ->
    fun() ->
            Recipient ! close,
            ok
    end.

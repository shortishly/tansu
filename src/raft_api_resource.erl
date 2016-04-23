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

-module(raft_api_resource).


-export([init/2]).
-export([terminate/3]).
-export([websocket_handle/3]).
-export([websocket_info/3]).


init(Req, State) ->
    RaftHost = cowboy_req:header(<<"raft-host">>, Req),
    RaftId = cowboy_req:header(<<"raft-id">>, Req),
    RaftPort = cowboy_req:header(<<"raft-port">>, Req),
    init(Req, RaftId, RaftHost, RaftPort, State).


init(Req, RaftId, RaftHost, RaftPort, State) when RaftId == undefined orelse
                                                  RaftHost == undefined orelse
                                                  RaftPort == undefined ->
    {ok, cowboy_req:reply(400, Req), State};

init(Req, RaftId, RaftHost, RaftPort, State) ->
    raft_consensus:add_connection(
      self(),
      RaftId,
      any:to_list(RaftHost),
      any:to_integer(RaftPort),
      outgoing(self()),
      closer(self())),
    {cowboy_websocket, Req, State}.


websocket_handle({binary, Message}, Req, State) ->
    raft_consensus:demarshall(self(), raft_rpc:decode(Message)),
    {ok, Req, State}.

websocket_info({message, Message}, Req, State) ->
    {reply, {binary, raft_rpc:encode(Message)}, Req, State};

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

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
-export([websocket_handle/3]).
-export([websocket_info/3]).

-export([init/2]).

init(Req, State) ->
    raft_connection:new(self(), outgoing(self())),
    {cowboy_websocket, Req, State}.

websocket_handle({text, Message}, Req, State) ->
    raft_rpc:demarshall(self(), Message),
    {ok, Req, State}.

websocket_info({message, Message}, Req, State) ->
    {reply, {text, jsx:encode(Message)}, Req, State}.

outgoing(Recipient) ->
    fun(Message) ->
            Recipient ! {message, Message},
            ok
    end.

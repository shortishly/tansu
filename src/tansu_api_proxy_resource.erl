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


-module(tansu_api_proxy_resource).

-export([init/2]).
-export([info/3]).
-export([terminate/3]).


init(Req, #{host := Host, port := Port}) ->
    {ok, Origin} = gun:open(Host, Port, #{transport => tcp}),
    {cowboy_loop, Req, #{monitor => erlang:monitor(process, Origin),
                         proxy => ?MODULE,
                         origin => Origin,
                         qs => cowboy_req:qs(Req),
                         path => cowboy_req:path(Req)}}.


info({gun_up, Origin, _}, Req, #{path := Path, qs := QS, origin := Origin} = State) ->
    %% A http connection to origin is up and available, proxy
    %% client request through to the origin.
    {ok, Req, maybe_request_body(Req, State, Origin, Path, QS)};

info({gun_response, _, _, nofin, Status, Headers}, Req, State) ->
    %% We have an initial http response from the origin together with
    %% some headers to forward to the client.
    {ok, cowboy_req:chunked_reply(Status, lists:keydelete(<<"content-length">>, 1, Headers), Req), State};

info({gun_response, _, _, fin, Status, Headers}, Req, State) ->
    %% short and sweet, we have final http response from the origin
    %% with just status and headers and no response body.
    {stop, cowboy_req:reply(Status, Headers, Req), State};

info({gun_data, _, _, nofin, Data}, Req, State) ->
    ok = cowboy_req:chunk(Data, Req),
    {ok, Req, State};

info({gun_data, _, _, fin, Data}, Req, State) ->
    %% we received a final response body chunk from the origin,
    %% chunk and forward to the client - and then hang up the
    %% connection.
    cowboy_req:chunk(Data, Req),
    {stop, Req, State};

info({request_body, #{complete := Data}}, Req, #{origin := Origin,
                                                 request := Request} = State) ->
    %% the client has streamed the http request body to us, and we
    %% have received the last chunk, forward the chunk to the origin
    %% letting them know not to expect any more
    gun:data(Origin, Request, fin, Data),
    {ok, Req, State};

info({request_body, #{more := More}}, Req, #{origin := Origin,
                                             request := Request} = State) ->
    %% the client is streaming the http request body to us, forward
    %% the chunk to the origin letting them know that we're not done
    %% yet.
    gun:data(Origin, Request, nofin, More),
    {ok, Req, request_body(Req, State)};

info({'DOWN', Monitor, _, _, _}, Req, #{monitor := Monitor} = State) ->
    %% whoa, our monitor has noticed the http connection to the origin
    %% is emulating a Norwegian Blue parrot, time to declare to the
    %% client that the gateway has turned bad.
    bad_gateway(Req, State).


terminate(_Reason, _Req, #{origin := Origin, monitor := Monitor}) ->
    %% we are terminating and have a monitored connection to the
    %% origin, try and be a nice citizen and demonitor and pull the
    %% plug on the connection to the origin.
    erlang:demonitor(Monitor),
    gun:close(Origin);

terminate(_Reason, _Req, #{origin := Origin}) ->
    %% we are terminating and just have a connection to the origin,
    %% try and pull the plug on it.
    gun:close(Origin).


maybe_request_body(Req, State, Origin, Path, QS) ->
    %% Proxy the http request through to the origin, and start
    %% streaming the request body from the client is there is one.
    maybe_request_body(Req, State#{request => proxy(Req, Origin, Path, QS)}).


proxy(Req, Origin, Path, <<>>) ->
    %% Act as a proxy for a http request to the origin from the
    %% client.
    Method = cowboy_req:method(Req),
    Headers = cowboy_req:headers(Req),
    gun:request(Origin, Method, Path, Headers);

proxy(Req, Origin, Path, QS) ->
    %% Act as a proxy for a http request to the origin from the
    %% client.
    Method = cowboy_req:method(Req),
    Headers = cowboy_req:headers(Req),
    gun:request(Origin, Method, <<Path/bytes, "?", QS/bytes>>, Headers).


maybe_request_body(Req, State) ->
    case cowboy_req:has_body(Req) of
        true ->
            %% We have a http request body, start streaming it from
            %% the client.
            request_body(Req, State);

        false ->
            %% There is no request body from the client, time to move
            %% on
            State
    end.


request_body(Req, State) ->
    %% We are streaming the request body from the client
    case cowboy_req:body(Req) of
        {ok, Data, _} ->
            %% We have streamed all of the request body from the
            %% client.
            self() ! {request_body, #{complete => Data}},
            State;

        {more, Data, _} ->
            %% We have part of the request body, but there is still
            %% more waiting for us.
            self() ! {request_body, #{more => Data}}
    end.


bad_gateway(Req, State) ->
    stop_with_code(502, Req, State).
                
stop_with_code(Code, Req, State) ->
    {stop, cowboy_req:reply(Code, Req), State}.
    



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


-module(raft_api_client_keys_resource).

-export([allowed_methods/2]).
-export([content_types_accepted/2]).
-export([content_types_provided/2]).
-export([from_form_urlencoded/2]).
-export([from_json/2]).
-export([info/3]).
-export([init/2]).
-export([resource_exists/2]).
-export([terminate/3]).
-export([to_json/2]).

init(Req, _) ->
    case raft_consensus:info() of
        #{follower := #{connections := Connections, leader := Leader}} ->
            case Connections of
                #{Leader := #{host := Host, port := Port}} ->
                    {ok, Origin} = gun:open(binary_to_list(Host), Port, #{transport => tcp}),
                    {cowboy_loop, Req, #{monitor => erlang:monitor(process, Origin),
                                         origin => Origin,
                                         path => cowboy_req:path(Req)}};
                #{} ->
                    service_unavailable(Req, #{})
            end;

        #{leader := _} = Info ->
            {cowboy_rest, Req, #{info => Info,
                                 path => cowboy_req:path(Req),
                                 key => cowboy_req:path_info(Req)}};

        #{} ->
            service_unavailable(Req, #{})
    end.

allowed_methods(Req, State) ->
    {[<<"GET">>, <<"HEAD">>, <<"OPTIONS">>, <<"POST">>, <<"PUT">>], Req, State}.

content_types_accepted(Req, State) ->
    {[{{<<"application">>, <<"x-www-form-urlencoded">>, []}, from_form_urlencoded},
      {{<<"application">>, <<"json">>, []}, from_json}], Req, State}.

content_types_provided(Req, State) ->
    {[{{<<"application">>, <<"json">>, '*'}, to_json}], Req, State}.

to_json(Req, #{value := Value} = State) ->
    {jsx:encode(#{value => Value}), Req, State}.


from_json(Req, State) ->
    from_json(cowboy_req:body(Req), <<>>, State).

from_json({ok, Final, Req}, Partial, #{key := Key} = State) ->
    kv_set(Req, Key, jsx:decode(<<Partial/binary, Final/binary>>), State);
from_json({more, Part, Req}, Partial, State) ->
    from_json(cowboy_req:body(Req), <<Partial/binary, Part/binary>>, State).

from_form_urlencoded(Req0, State) ->
    case cowboy_req:body_qs(Req0) of
        {ok, KVS, Req1} ->
            from_form_url_encoded(Req1, maps:from_list(KVS), State);

        _ ->
            bad_request(Req0, State)
    end.

from_form_url_encoded(Req, #{<<"value">> := Value}, #{key := Key} = State) ->
    kv_set(Req, Key, Value, State);
from_form_url_encoded(Req, _, State) ->
    bad_request(Req, State).

kv_set(Req, Key, Value, State) ->
    case raft_api:kv_set(Key, Value) of
        ok ->
            {true, Req, State};
        
        not_leader ->
            service_unavailable(Req, State)
    end.


resource_exists(Req, #{info := #{leader := _}, key := Key} = State) ->
    case cowboy_req:method(Req) of
        <<"GET">> ->
            case raft_api:kv_get(Key) of
                {ok, Value} ->
                    {true, Req, State#{value => Value}};

                {error, not_found} ->
                    {false, Req, State};

                {error, not_leader} ->
                    %% whoa, we were the leader, but we're not now
                    service_unavailable(Req, State)
            end;
        _ ->
            {true, Req, State}
    end.


info({gun_up, Origin, _}, Req, #{path := Path, origin := Origin} = State) ->
    %% A http connection to origin is up and available, proxy
    %% client request through to the origin.
    {ok, Req, maybe_request_body(Req, State, Origin, Path)};

info({gun_response, _, _, nofin, Status, Headers}, Req, State) ->
    %% We have an initial http response from the origin together with
    %% some headers to forward to the client.
    {ok, cowboy_req:chunked_reply(Status, Headers, Req), State};

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
    gun:close(Origin);

terminate(_Reason, _Req, _) ->
    %% nothing to clean up here.
    ok.


maybe_request_body(Req, State, Origin, Path) ->
    %% Proxy the http request through to the origin, and start
    %% streaming the request body from the client is there is one.
    maybe_request_body(Req, State#{request => proxy(Req, Origin, Path)}).


proxy(Req, Origin, Path) ->
    %% Act as a proxy for a http request to the origin from the
    %% client.
    Method = cowboy_req:method(Req),
    Headers = cowboy_req:headers(Req),
    gun:request(Origin, Method, Path, Headers).


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
                
bad_request(Req, State) ->                        
    stop_with_code(400, Req, State).
                
service_unavailable(Req, State) ->
    stop_with_code(503, Req, State).

stop_with_code(Code, Req, State) ->
    {stop, cowboy_req:reply(Code, Req), State}.
    

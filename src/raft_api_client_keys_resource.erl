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
-export([delete_resource/2]).
-export([from_form_urlencoded/2]).
-export([from_json/2]).
-export([info/3]).
-export([init/2]).
-export([resource_exists/2]).
-export([terminate/3]).
-export([to_json/2]).

init(Req, _) ->
    case {cowboy_req:method(Req), raft_consensus:info(), maps:from_list(cowboy_req:parse_qs(Req))} of
        {<<"GET">>, Info, #{<<"stream">> := <<"true">>}} ->
            %% An event stream can be established with any member of
            %% the cluster.
	    Headers = [{<<"content-type">>, <<"text/event-stream">>},
		       {<<"cache-control">>, <<"no-cache">>}],
            raft_api:kv_subscribe(cowboy_req:path_info(Req)),
	    {cowboy_loop,
             cowboy_req:chunked_reply(200, Headers, Req),
             #{info => Info}};

        {<<"GET">>, #{follower := #{leader := _}} = Info, _} ->
            %% followers with an established leader can handle simple
            %% KV GET requests.
            {cowboy_rest,
             Req,
             #{info => Info,
               path => cowboy_req:path(Req),
               key => cowboy_req:path_info(Req)}};

        {_, #{follower := #{connections := Connections, leader := Leader}}, _} ->
            %% Requests other than GETs should be proxied to the
            %% leader.
            case Connections of
                #{Leader := #{host := Host, port := Port}} ->
                    raft_api_client_proxy_resource:init(
                      Req, #{host => binary_to_list(Host), port => Port});

                #{} ->
                    service_unavailable(Req, #{})
            end;

        {_, #{leader := _} = Info, _} ->
            %% The leader can deal directly with any request.
            {cowboy_rest,
             Req,
             #{info => Info,
               path => cowboy_req:path(Req),
               key => cowboy_req:path_info(Req)}};

        {_, #{}, _} ->
            %% Neither a leader nor a follower with an established
            %% leader then the service is unavailable.
            service_unavailable(Req, #{})
    end.

allowed_methods(Req, State) ->
    {[<<"DELETE">>,
      <<"GET">>,
      <<"HEAD">>,
      <<"OPTIONS">>,
      <<"POST">>,
      <<"PUT">>], Req, State}.

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
    case cowboy_req:header(<<"ttl">>, Req) of
        undefined ->
            kv_set(
              Req,
              Key,
              jsx:decode(<<Partial/binary, Final/binary>>),
              State);

        TTL ->
            try
                kv_set(
                  Req,
                  Key,
                  jsx:decode(<<Partial/binary, Final/binary>>),
                  binary_to_integer(TTL),
                  State)
            catch
                error:badarg ->
                    kv_set(
                      Req,
                      Key,
                      jsx:decode(<<Partial/binary, Final/binary>>),
                      State)
            end
    end;

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
    case cowboy_req:header(<<"ttl">>, Req) of
        undefined ->
            kv_set(Req, Key, Value, State);

        TTL ->
            try
                kv_set(
                  Req,
                  Key,
                  Value,
                  binary_to_integer(TTL),
                  State)
            catch
                error:badarg ->
                    kv_set(
                      Req,
                      Key,
                      Value,
                      State)
            end
    end;

from_form_url_encoded(Req, _, State) ->
    bad_request(Req, State).

kv_set(Req, Key, Value, TTL, State) ->
    case raft_api:kv_set(Key, Value, TTL) of
        ok ->
            {true, Req, State};
        
        not_leader ->
            service_unavailable(Req, State)
    end.

kv_set(Req, Key, Value, State) ->
    case raft_api:kv_set(Key, Value) of
        ok ->
            {true, Req, State};
        
        not_leader ->
            service_unavailable(Req, State)
    end.

delete_resource(Req, #{key := Key} = State) ->
    {raft_api:kv_delete(Key) == ok, Req, State}.

resource_exists(Req, #{key := Key} = State) ->
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


info(#{id := Id, event := Event, data := Data,module := raft_sm}, Req, State) ->
    {cowboy_req:chunk(
       ["id: ",
        any:to_list(Id),
        "\nevent: ",
        any:to_list(Event),
        "\ndata: ",
        jsx:encode(Data), "\n\n"],
       Req),
     Req,
     State};

info(Event, Req, #{proxy := Proxy} = State) ->
    Proxy:info(Event, Req, State).


terminate(Reason, Req, #{proxy := Proxy} = State) ->
    Proxy:terminate(Reason, Req, State);

terminate(_Reason, _Req, _) ->
    %% nothing to clean up here.
    raft_sm:goodbye().
                
bad_request(Req, State) ->                        
    stop_with_code(400, Req, State).
                
service_unavailable(Req, State) ->
    stop_with_code(503, Req, State).

stop_with_code(Code, Req, State) ->
    {stop, cowboy_req:reply(Code, Req), State}.
    

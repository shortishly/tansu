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


-module(tansu_api_keys_resource).

-export([allowed_methods/2]).
-export([content_types_accepted/2]).
-export([content_types_provided/2]).
-export([delete_resource/2]).
-export([from_form_urlencoded/2]).
-export([from_identity/2]).
-export([info/3]).
-export([init/2]).
-export([resource_exists/2]).
-export([terminate/3]).
-export([to_identity/2]).

init(Req, _) ->
    init(
      Req,
      cowboy_req:method(Req),
      tansu_consensus:info(),
      maps:merge(#{<<"role">> => <<"any">>}, maps:from_list(cowboy_req:parse_qs(Req))),
      cowboy_req:header(<<"ttl">>, Req),
      cowboy_req:header(<<"content-type">>, Req)).


init(Req, <<"GET">>, Info, #{<<"stream">> := <<"true">>}, _, _) ->
    %% An event stream can be established with any member of
    %% the cluster.
    Headers = [{<<"content-type">>, <<"text/event-stream">>},
               {<<"cache-control">>, <<"no-cache">>} | headers(Info)],
    tansu_api:kv_subscribe(key(Req)),
    {cowboy_loop,
     cowboy_req:chunked_reply(200, Headers, Req),
     #{info => Info}};

init(Req, <<"GET">>, #{role := follower, leader := _, cluster := _} = Info, #{<<"role">> := <<"any">>} = QS, _, _) ->
    %% followers with an established leader and cluster can
    %% handle simple KV GET requests.
    {cowboy_rest,
     headers(Info, Req),
     #{info => Info,
       path => cowboy_req:path(Req),
       key => key(Req),
       qs => QS,
       parent => parent(Req)}};

init(Req, _, #{role := follower, connections := Connections, leader := #{id := Leader}, cluster := _}, _, _, _) ->
    %% Requests other than GETs should be proxied to the
    %% leader.
    case Connections of
        #{Leader := #{host := Host, port := Port}} ->
            tansu_api_proxy_resource:init(
              Req, #{host => binary_to_list(Host), port => Port});
        
        #{} ->
            {ok, service_unavailable(Req), #{}}
    end;

init(Req, _, #{role := leader} = Info, QS, undefined, undefined) ->
    %% The leader can deal directly with any request.
    {cowboy_rest,
     headers(Info, Req),
     #{info => Info,
       path => cowboy_req:path(Req),
       key => key(Req),
       qs => QS,
       parent => parent(Req)}};

init(Req, _, #{role := leader} = Info, QS, undefined, ContentType) ->
    %% The leader can deal directly with any request.
    {cowboy_rest,
     headers(Info, Req),
     #{info => Info,
       content_type => ContentType,
       path => cowboy_req:path(Req),
       qs => QS,
       key => key(Req),
       parent => parent(Req)}};

init(Req, _, #{role := leader} = Info, QS, TTL, undefined) ->
    %% The leader can deal directly with any request.
    {cowboy_rest,
     headers(Info, Req),
     #{info => Info,
       path => cowboy_req:path(Req),
       ttl => binary_to_integer(TTL),
       key => key(Req),
       qs => QS,
       parent => parent(Req)}};

init(Req, _, #{role := leader} = Info, QS, TTL, ContentType) ->
    %% The leader can deal directly with any request.
    {cowboy_rest,
     headers(Info, Req),
     #{info => Info,
       content_type => ContentType,
       path => cowboy_req:path(Req),
       ttl => binary_to_integer(TTL),
       key => key(Req),
       qs => QS,
       parent => parent(Req)}};

init(Req, _, _, _, _, _) ->
    %% Neither a leader nor a follower with an established
    %% leader then the service is unavailable.
    {ok, service_unavailable(Req), #{}}.

allowed_methods(Req, State) ->
    {[<<"DELETE">>,
      <<"GET">>,
      <<"HEAD">>,
      <<"OPTIONS">>,
      <<"POST">>,
      <<"PUT">>], Req, State}.

content_types_accepted(Req, #{content_type := <<"application/x-www-form-urlencoded">> = ContentType} = State) ->
    {[{ContentType, from_form_urlencoded}], Req, State};

content_types_accepted(Req, #{content_type := ContentType} = State) ->
    {[{ContentType, from_identity}], Req, State}.

content_types_provided(Req, #{key := Key, qs := #{<<"children">> := <<"true">>}} = State) ->
    case {tansu_api:kv_get(Key), tansu_api:kv_get_children_of(Key)} of
        {{ok, Value, Metadata}, Children} when map_size(Children) == 0 ->
            {[{<<"application/json">>, to_identity}], Req, State#{value => #{data => jsx:encode(#{value => Value, children => #{}}), metadata => Metadata}}};
        
        {{ok, ParentValue, #{content_type := <<"application/json">>} = ParentMetadata}, Children} ->
            {[{<<"application/json">>, to_identity}],
             Req,
             State#{value => #{
                      data => 
                          jsx:encode(
                            #{
                               value => jsx:decode(ParentValue),
                               children => 
                                   maps:fold(
                                     fun
                                         (Child, {Value, #{content_type := <<"application/json">>} = Metadata}, A) ->
                                             A#{Child => #{value => jsx:decode(Value), metadata => without_reserved(Metadata)}};
                                         
                                         (Child, {Value, Metadata}, A) ->
                                             A#{Child => #{value => Value, metadata => without_reserved(Metadata)}}
                                     end,
                                     #{},
                                     Children)
                             }),
                      metadata => ParentMetadata
                     }}};
        
        {{ok, ParentValue, ParentMetadata}, Children} ->
            {[{<<"application/json">>, to_identity}],
             Req,
             State#{value => #{
                      data => 
                          jsx:encode(
                            #{
                               value => ParentValue,
                               children => 
                                   maps:fold(
                                     fun
                                         (Child, {Value, #{content_type := <<"application/json">>} = Metadata}, A) ->
                                             A#{Child => #{value => jsx:decode(Value), metadata => without_reserved(Metadata)}};
                                         
                                         (Child, {Value, Metadata}, A) ->
                                             A#{Child => #{value => Value, metadata => without_reserved(Metadata)}}
                                     end,
                                     #{},
                                     Children)
                             }),
                      metadata => ParentMetadata#{content_type => <<"application/json">>}
                     }}};

        {{error, _} = Error, _} ->
            {[{<<"text/plain">>, dummy_to_text_plain}], Req, State#{value => Error}}
    end;

content_types_provided(Req, #{key := Key} = State) ->
    case {cowboy_req:method(Req), tansu_api:kv_get(Key)} of
        {<<"PUT">>, {ok, Value, Metadata}} ->
            {[{<<"application/json">>, to_identity}], Req, State#{value => #{data => Value, metadata => Metadata}}};

        {_, {ok, Value, #{tansu := #{content_type := ContentType}} = Metadata}} ->
            {[{ContentType, to_identity}], Req, State#{value => #{data => Value, metadata => Metadata}}};

        {<<"PUT">>, {error, _}} = Error ->
            {[{<<"application/json">>, dummy_to_text_plain}], Req, State#{value => Error}};

        {_, {error, _} = Error} ->
            {[{<<"text/plain">>, dummy_to_text_plain}], Req, State#{value => Error}}
    end.

without_reserved(Metadata) ->
    maps:without(reserved(), Metadata).

reserved() ->
    [parent, ttl].

to_identity(Req, #{value := #{data := Data, metadata := #{tansu := Tansu}}} = State) ->
    {Data, add_metadata_headers(Req, Tansu), State}.

add_metadata_headers(Req, Metadata) ->
    maps:fold(
      fun
          (created, Index, A) ->
              cowboy_req:set_resp_header(<<"created-index">>, any:to_binary(Index), A);
          (updated, Index, A) ->
              cowboy_req:set_resp_header(<<"updated-index">>, any:to_binary(Index), A);
          (ttl, TTL, A) ->
              cowboy_req:set_resp_header(<<"ttl">>, any:to_binary(TTL), A);
          (_, _, A) ->
              A
      end,
      Req,
      Metadata).

key(Req) ->
    slash_separated(cowboy_req:path_info(Req)).

parent(Req) ->
    slash_separated(lists:droplast(cowboy_req:path_info(Req))).

slash_separated([]) ->
    <<"/">>;
slash_separated(PathInfo) ->
    lists:foldl(
      fun
          (Path, <<>>) ->
              <<"/", Path/bytes>>;
          (Path, A) ->
              <<A/bytes, "/", Path/bytes>>
      end,
      <<>>,
      PathInfo).

from_form_urlencoded(Req, State) ->
    case cowboy_req:has_body(Req) of
        true ->
            from_form_urlencoded_body(cowboy_req:body(Req), <<>>, State);

        false ->
            {stop, bad_request(Req), State}
    end.

from_form_urlencoded_body({ok, Remainder, Req}, Partial, State) ->
    from_form_url_encoded(Req, maps:from_list(cow_qs:parse_qs(<<Partial/bytes, Remainder/bytes>>)), State);
from_form_urlencoded_body({more, Remainder, Req}, Partial, State) ->
    from_form_urlencoded_body(cowboy_req:body(Req), <<Partial/bytes, Remainder/bytes>>, State).

from_form_url_encoded(Req, #{<<"value">> := Value}, State) ->
    from_identity({ok, Value, Req}, <<>>, State).

from_identity(Req, State) ->
    from_identity(cowboy_req:body(Req), <<>>, State).

from_identity({ok, Final, Req}, Partial, #{qs := #{<<"prevExist">> := <<"false">>}, key := Key, content_type := <<"application/x-www-form-urlencoded">>} = State) ->
    kv_test_and_set(
      Req,
      Key,
      undefined,
      <<Partial/binary, Final/binary>>,
      State#{content_type := <<"text/plain">>});

from_identity({ok, Final, Req}, Partial, #{qs := #{<<"prevValue">> := ExistingValue}, key := Key, content_type := <<"application/x-www-form-urlencoded">>} = State) ->
    kv_test_and_set(
      Req,
      Key,
      ExistingValue,
      <<Partial/binary, Final/binary>>,
      State#{content_type := <<"text/plain">>});

from_identity({ok, Final, Req}, Partial, #{key := Key, content_type := <<"application/x-www-form-urlencoded">>} = State) ->
    kv_set(
      Req,
      Key,
      <<Partial/binary, Final/binary>>,
      State#{content_type := <<"text/plain">>});

from_identity({ok, Final, Req}, Partial, #{key := Key} = State) ->
    kv_set(
      Req,
      Key,
      <<Partial/binary, Final/binary>>,
      State);

from_identity({more, Part, Req}, Partial, State) ->
    from_identity(cowboy_req:body(Req), <<Partial/binary, Part/binary>>, State).

kv_test_and_set(Req, Key, ExistingValue, NewValue, State) ->
    case tansu_api:kv_test_and_set(
           Key,
           ExistingValue,
           NewValue,
           #{tansu => maps:with([content_type, parent, ttl], State)}) of

        {ok, #{current := Current} = Detail} ->
            {true,
             add_metadata_headers(
               cowboy_req:set_resp_body(
                 jsx:encode(Detail),
                 Req),
               Current),
             State};

        error ->
            {stop, conflict(Req), State};

        {error, not_leader} ->
            {stop, service_unavailable(Req), State}
    end.

kv_set(Req, Key, Value, State) ->
    case tansu_api:kv_set(
           Key,
           Value,
           #{tansu => maps:with([content_type, parent, ttl], State)}) of

        {ok, #{current := Current} = Detail} ->
            {true,
             add_metadata_headers(
               cowboy_req:set_resp_body(
                 jsx:encode(Detail),
                 Req),
               Current),
             State};
        
        {error, not_leader} ->
            {stop, service_unavailable(Req), State}
    end.

delete_resource(Req, #{qs := #{<<"prevValue">> := ExistingValue}, key := Key} = State) ->
    case tansu_api:kv_test_and_delete(Key, ExistingValue) of
        {ok, _} ->
            {true, Req, State};

        error ->
            {stop, conflict(Req), State}
    end;

delete_resource(Req, #{key := Key} = State) ->
    case tansu_api:kv_delete(Key) of
        {ok, _} ->
            {true, Req, State};

        error ->
            {false, Req, State}
    end.


resource_exists(Req, State) ->
    resource_exists(Req, cowboy_req:method(Req), State).

resource_exists(Req, _, #{value := #{data := _, metadata := #{ttl := TTL}}} = State) ->
    {true, cowboy_req:set_resp_header(<<"ttl">>, any:to_binary(TTL), Req), State};
resource_exists(Req, _, #{value := #{data := _, metadata := _}} = State) ->
    {true, Req, State};
resource_exists(Req, _, #{value := {error, not_found}} = State) ->
    {false, Req, State};
resource_exists(Req, _, #{value := {error, not_leader}} = State) ->
    %% whoa, we were the leader, but we're not now
    {stop, service_unavailable(Req), State};
resource_exists(Req, _, State) ->
    {true, Req, State}.


info(#{id := Id, event := Event, data := #{metadata := #{tansu := #{content_type := <<"application/json">>}}, value := Value} = Data, module := tansu_sm}, Req, State) ->
    {tansu_stream:chunk(Id, Event, Data#{value := jsx:decode(Value)}, Req), Req, State};

info(#{id := Id, event := Event, data := Data, module := tansu_sm}, Req, State) ->
    {tansu_stream:chunk(Id, Event, Data, Req), Req, State};

info(Event, Req, #{proxy := Proxy} = State) ->
    Proxy:info(Event, Req, State).


terminate(Reason, Req, #{proxy := Proxy} = State) ->
    Proxy:terminate(Reason, Req, State);

terminate(_Reason, _Req, _) ->
    %% nothing to clean up here.
    tansu_sm:goodbye().
                
bad_request(Req) ->
    stop_with_code(400, Req).

conflict(Req) ->
    stop_with_code(409, Req).

service_unavailable(Req) ->
    stop_with_code(503, Req).

stop_with_code(Code, Req) ->
    cowboy_req:reply(Code, Req).

headers(Info, Req) ->
    lists:foldl(
      fun
          ({Key, Value}, A) ->
              cowboy_req:set_resp_header(Key, Value, A)
      end,
      Req,
      headers(Info)).

headers(Info) ->
    maps:fold(
      fun
          (cluster, Cluster, A) ->
              [{<<"cluster-id">>, Cluster} |  A];

          (commit_index, CI, A) ->
              [{<<"raft-ci">>, any:to_binary(CI)} |  A];

          (env, Env, A) ->
              [{<<"env">>, Env} |  A];

          (id, Id, A) ->
              [{<<"node-id">>, Id} | A];

          (last_applied, LA, A) ->
              [{<<"raft-la">>, any:to_binary(LA)} |  A];

          (term, Term, A) ->
              [{<<"raft-term">>, any:to_binary(Term)} | A];

          (role, Role, A) ->
              [{<<"role">>, any:to_binary(Role)} |  A];

          (_, _, A) ->
              A
      end,
      [],
      Info).
    

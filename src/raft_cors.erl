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

-module(raft_cors).
-export([allow_origin/1]).
-export([options/3]).


allow_origin(Req) ->
    case headers(Req) of
        #{<<"origin">> := _} ->
	    cowboy_req:set_resp_header(<<"Access-Control-Allow-Origin">>, <<"*">>, Req);

	_Elsewhere ->
	    Req
    end.

options(Req, State, Allowed) ->
    case headers(Req) of
        #{<<"origin">> := Origin,
          <<"access-control-request-method">> := RequestMethod,
          <<"access-control-request-headers">> := RequestHeaders} ->
            add_headers(Req, State, Allowed, Origin, RequestMethod, RequestHeaders);

	#{} ->
	    {ok, Req, State}
    end.

add_headers(Req, State, Allowed, Origin, RequestMethod, RequestHeaders) ->
    case ordsets:is_element(RequestMethod, Allowed) of
        true ->
            
            {ok, cowboy_req:set_resp_header(
                   <<"Access-Control-Allow-Methods">>,
                   methods(Allowed),
                   cowboy_req:set_resp_header(
                     <<"Access-Control-Allow-Origin">>,
                     Origin,
                     cowboy_req:set_resp_header(
                       <<"Access-Control-Allow-Headers">>,
                       RequestHeaders,
                       Req))),
             State};
        
        false ->
            {ok, Req, State}
    end.

methods(Allowed) ->
    lists:foldr(
      fun
          (Method, <<>>) ->
              <<Method>>;

          (Method, A) ->
              <<Method/bytes, ", ", A/binary>>
      end,
      <<>>,
      Allowed).

headers(Req) ->
    maps:from_list(cowboy_req:headers(Req)).




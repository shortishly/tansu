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

-module(tansu_oapi_resource).

-export([allowed_methods/2]).
-export([content_types_provided/2]).
-export([init/2]).
-export([options/2]).
-export([to_json/2]).

init(Req, _) ->
    {cowboy_rest, tansu_cors:allow_origin(Req), #{}}.

allowed_methods(Req, State) ->
    {allowed(), Req, State}.

options(Req, State) ->
    tansu_cors:options(Req, State, allowed()).

content_types_provided(Req, State) ->
    {[{{<<"application">>, <<"json">>, '*'}, to_json}], Req, State}.

allowed() ->
    [<<"GET">>,
     <<"HEAD">>,
     <<"OPTIONS">>].


to_json(Req, State) ->
    {jsx:encode(root(Req)), Req, State}.

root(Req) ->
    #{
      swagger => <<"2.0">>,
      info => info(),
      host => <<(cowboy_req:host(Req))/bytes, ":", (any:to_binary(tansu_config:port(http)))/bytes>>,
      <<"basePath">> => any:to_binary(tansu_config:endpoint(api)),
      schemes => [<<"http">>],
      consumes => [json()],
      produces => [json()],
      paths => paths()
     }.

json() ->
    <<"application/json">>.

info() ->
    #{
      title => tansu,
      description => any:to_binary(tansu:description()),
      <<"termsOfService">> => <<"http://swagger.io/terms">>,
      contact => #{
        name => <<"API Support">>,
        url => <<"http://swagger.io/support">>,
        email => <<"support@swagger.io">>
       },
      license => #{
        name => <<"Apache 2.0">>,
        url => <<"http://www.apache.org/licenses/LICENSE-2.0.html">>
       },
      version => any:to_binary(tansu:vsn())
     }.

paths() ->
    path(<<"/keys/{key}">>).

path(<<"/keys/{key}">> = Keys) ->
    #{Keys => #{
        get => #{
          description => <<"Returns the value of the supplied key">>,
          <<"operationId">> => <<"getValue">>,
          produces => [json()],
          responses => #{
            200 => #{
              description => <<"Value of the supplied key">>
             },
            
            404 => #{
              description => <<"The supplied key does not exist">>
             }
           }
         },
        
        post => #{
          description => <<"Sets the value of the supplied key">>,
          <<"operationId">> => <<"setValue">>,
          responses => #{
            204 => #{
              description => <<"Value has been accepted">>
             }
           }
         },
        
        delete => #{
          description => <<"Remove the key from the store">>,
          <<"operationId">> => <<"deleteKey">>,
          responses => #{
            204 => #{
              description => <<"The key has been deleted from the store">>
             }
           }
         },

        parameters => [#{
          name => key,
          in => path,
          description => <<"The key">>,
          required => true,
          type => string
         }]
       }
     }.
                          
                        
                           

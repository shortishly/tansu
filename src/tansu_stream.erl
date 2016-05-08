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


-module(tansu_stream).

-export([chunk/3]).
-export([chunk/4]).


chunk(Id, Event, Req) ->
    cowboy_req:chunk(
      ["id: ",
       any:to_list(Id),
       "\nevent: ",
       any:to_list(Event),
       "\n\n"],
      Req).

chunk(Id, Event, Data, Req) ->
    cowboy_req:chunk(
      ["id: ",
       any:to_list(Id),
       "\nevent: ",
       any:to_list(Event),
       "\ndata: ",
       jsx:encode(Data), "\n\n"],
      Req).

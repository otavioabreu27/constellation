-module(constellation_notifier_ffi).
-export([run_safely/1]).

run_safely(Callback) ->
    try Callback() catch _:_ -> nil end.

-module(parallel_benchmark_ffi).
-export([monotonic_milliseconds/0, schedulers_online/0]).

monotonic_milliseconds() ->
    erlang:monotonic_time(millisecond).

schedulers_online() ->
    erlang:system_info(schedulers_online).

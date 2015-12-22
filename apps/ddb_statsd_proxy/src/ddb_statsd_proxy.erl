-module(ddb_statsd_proxy).

-export([decode/1]).

decode(Metric) ->
    case decode_name(Metric, [<<>>]) of
        error ->
            {incomplete, Metric};
        R ->
            R
    end.

decode_name(<<".", R/binary>>, Acc) ->
    decode_name(R, [<<>> | Acc]);
decode_name(<<":", R/binary>>, Acc) ->
    M = dproto:metric_from_list(lists:reverse(Acc)),
    decode_int(M, R, <<>>);
decode_name(<<C, R/binary>>, [H | T]) ->
    decode_name(R, [<<H/binary, C>> | T]).

decode_int(M, <<"|", Type/binary>>, Acc) ->
    V = binary_to_integer(Acc),
    parse_type(M, V, Type);
decode_int(M, <<N, R/binary>>, Acc) when N >= $0, N =< $9 ->
    decode_int(M, R, <<Acc/binary, N>>);
decode_int(M, <<".", R/binary>>, Acc) ->
    decode_float(M, R, <<Acc/binary, ".">>);
decode_int(M, <<"e+", R/binary>>, Acc) ->
    decode_float(M, R, <<Acc/binary, "e+">>);
decode_int(M, <<"e-", R/binary>>, Acc) ->
    decode_float(M, R, <<Acc/binary, "e-">>);
decode_int(M, <<"e", R/binary>>, Acc) ->
    decode_float(M, R, <<Acc/binary, "e">>);
decode_int(_, _, _) ->
    error.




decode_float(M, <<"|", Type/binary>>, Acc) ->
    V = binary_to_float(Acc),
    parse_type(M, V, Type);
decode_float(M, <<N, R/binary>>, Acc) when N >= $0, N =< $9 ->
    decode_float(M, R, <<Acc/binary, N>>);
decode_float(M, <<"e+", R/binary>>, Acc) ->
    decode_float(M, R, <<Acc/binary, "e+">>);
decode_float(M, <<"e-", R/binary>>, Acc) ->
    decode_float(M, R, <<Acc/binary, "e-">>);
decode_float(M, <<"e", R/binary>>, Acc) ->
    decode_float(M, R, <<Acc/binary, "e">>);
decode_float(_, _, _) ->
    error.

parse_type(M, V, <<"c@", Rate/binary>>) ->
    parse_rate(M, V, Rate, <<>>);
parse_type(M, V, <<"c\n", R/binary>>) ->
    {{counter, M, V}, R};
parse_type(M, V, <<"g\n", R/binary>>) ->
    {{gauge, M, V}, R};
parse_type(M, V, <<"ms\n", R/binary>>) ->
    {{timer, M, V}, R};
parse_type(M, V, <<"h\n", R/binary>>) ->
    {{histogram, M, V}, R};
parse_type(M, V, <<"m\n", R/binary>>) ->
    {{meter, M, V}, R};
parse_type(_, _, _) ->
    error.

parse_rate(M, V, <<"\n", R/binary>>, Rate) ->
    {{counter, M, V, binary_to_integer(Rate)}, R};
parse_rate(M, V, <<N, R/binary>>, Rate) when N >= $0, N =< $9 ->
    parse_rate(M, V, R, <<Rate/binary, N>>);
parse_rate(M, V, <<".", R/binary>>, Rate) ->
    parse_rate_f(M, V, R, <<Rate/binary, ".">>);
parse_rate(_, _, _, _) ->
    error.


parse_rate_f(M, V, <<"\n", R/binary>>, Rate) ->
    {{counter, M, V, binary_to_float(Rate)}, R};
parse_rate_f(M, V, <<N, R/binary>>, Rate) when N >= $0, N =< $9 ->
    parse_rate_f(M, V, R, <<Rate/binary, N>>);
parse_rate_f(_, _, _, _) ->
    error.



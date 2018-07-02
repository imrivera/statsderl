-module(statsderl_server).
-include("statsderl.hrl").

-export([
    init/2,
    start_link/1
]).

-record(state, {
    base_key :: iodata(),
    addresses :: {{inet:ip_address(), inet:port_number()}},
    socket :: inet:socket()
}).

%% public
-spec init(pid(), atom()) -> no_return().

init(Parent, Name) ->
    BaseKey = ?ENV(?ENV_BASEKEY, ?DEFAULT_BASEKEY),
    Hostname = ?ENV(?ENV_HOSTNAME, ?DEFAULT_HOSTNAME),
    {ok, Addresses} = case is_list(Hostname)
                          andalso Hostname /= []
                          andalso is_tuple(hd(Hostname)) of
                              true ->
                                  generate_addresses(Hostname, []);
                              false ->
                                  Port = ?ENV(?ENV_PORT, ?DEFAULT_PORT),
                                  generate_addresses([{Hostname, Port}], [])
                    end,

    case gen_udp:open(0, [{active, false}]) of
        {ok, Socket} ->
            register(Name, self()),
            proc_lib:init_ack(Parent, {ok, self()}),

            loop(#state {
                socket = Socket,
                base_key = statsderl_utils:base_key(BaseKey),
                addresses = list_to_tuple(Addresses)
            });
        {error, Reason} ->
            exit(Reason)
    end.

-spec start_link(atom()) -> {ok, pid()}.

start_link(Name) ->
    proc_lib:start_link(?MODULE, init, [self(), Name]).

%% private
handle_msg({cast, KeyHash, Packet}, #state {
        addresses = Addresses,
        base_key = BaseKey,
        socket = Socket
    } = State) ->

    {Ip, Port} = element((KeyHash rem tuple_size(Addresses)) + 1, Addresses),
    gen_udp:send(Socket, Ip, Port, [BaseKey, Packet]),
    {ok, State};
handle_msg({inet_reply, _Socket, ok}, State) ->
    {ok, State};
handle_msg({inet_reply, _Socket, {error, Reason}}, State) ->
    statsderl_utils:error_msg("inet_reply error: ~p~n", [Reason]),
    {ok, State}.

loop(State) ->
    receive Msg ->
        {ok, State2} = handle_msg(Msg, State),
        loop(State2)
    end.

generate_addresses([], Acc) ->
    {ok, lists:reverse(Acc)};
generate_addresses([{Hostname, Port} | Rest], Acc) ->
    case statsderl_utils:getaddrs(Hostname) of
        {ok, Ip} ->
            generate_addresses(Rest, [{Ip, Port} | Acc]);
        Error ->
            Error
    end.

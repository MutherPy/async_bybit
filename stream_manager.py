import json
from typing import ClassVar, Union, Iterable, Generator, Optional

from websockets import InvalidStatus
from websockets.asyncio.client import ClientConnection, connect
from dataclasses import dataclass, field
import asyncio
from choices import OpTypes
from uuid import uuid4
from itertools import count


@dataclass
class Symbol:
    symbol: str
    topic: str
    connection_meta: "ClientConnectionMeta"
    sub_req_id: str


@dataclass
class ClientConnectionMeta:
    id: str
    connection: ClientConnection
    _max_symbols_per_stream: ClassVar
    symbols: dict[str, Symbol] = field(default_factory=dict)
    _sub_buffer: set[str] = field(default_factory=list)
    _unsub_buffer: set[str] = field(default_factory=list)
    is_alive: bool = True

    @property
    def free_space(self):
        return (
            self._max_symbols_per_stream
            - len(self._sub_buffer)
            + len(self._unsub_buffer)
            - len(self.symbols)
        )

    @property
    def real_tickers(self) -> list:
        return list((set(self.symbols.keys()) - self._unsub_buffer) | self._sub_buffer)


def get_url():
    ws_url = "wss://{SUBDOMAIN}.{DOMAIN}.com/v5/public/{CHANNEL_TYPE}"
    CHANNEL_TYPE = 'linear'
    SUBDOMAIN_MAINNET = "stream"
    DOMAIN_MAIN = "bybit"
    return ws_url.format(
        SUBDOMAIN=SUBDOMAIN_MAINNET,
        DOMAIN=DOMAIN_MAIN,
        CHANNEL_TYPE=CHANNEL_TYPE
    )


class SubscriptionsManagerComponent:
    def __init__(self):
        self.__connections: dict[ClientConnection, ClientConnectionMeta] = {}
        self.__subscribed_symbols: dict[str, Symbol] = {}

    def add_connection(self, conn_id: str, connection: ClientConnection):
        self.__connections[connection] = ClientConnectionMeta(id=conn_id, connection=connection)

    def get_connection(self) -> Optional[ClientConnectionMeta]:
        try:
            return next(iter(self.__connections.values()))
        except StopIteration:
            return None

    def remove_connection(self, connection: ClientConnection):
        to_remove_symbols: dict[str, Symbol] = self.__connections[connection].symbols
        self.__subscribed_symbols = {s: S for s, S in self.__subscribed_symbols.items() if s not in to_remove_symbols}
        del self.__connections[connection]

    def set_connection_dead(self, connection: ClientConnection):
        self.__connections[connection].is_alive = False

    def add_conn_symbol(self, connection: ClientConnection, symbol: str, topic: str, sub_req_id: str):
        current_connection_meta: ClientConnectionMeta = self.__connections[connection]
        s = Symbol(symbol=symbol, topic=topic, connection_meta=current_connection_meta, sub_req_id=sub_req_id)
        self.__subscribed_symbols[symbol] = s
        current_connection_meta.symbols[symbol] = self.__subscribed_symbols[symbol]

    def re_add_conn_symbol(self, connection: ClientConnection, symbols: list[Symbol], req_id: str):
        conn_meta = self.__connections[connection]
        for s in symbols:
            s.connection_meta = conn_meta
            s.sub_req_id = req_id
        conn_meta.symbols = symbols

    def get_conn_symbol(self, connection: ClientConnection, symbol: Optional[str] = None) -> Union[Symbol, list[Symbol]]:
        if symbol:
            return self.__connections[connection].symbols[symbol]
        else:
            return list(self.__connections[connection].symbols.values())

    def remove_conn_symbol(self, connection: ClientConnection, symbol: Union[str, Iterable[str]]):
        if isinstance(symbol, str):
            symbol = list[symbol]
        self.__subscribed_symbols = {s: S for s, S in self.__subscribed_symbols.items() if s not in symbol}
        conn_symbols = list(self.__connections[connection].symbols.items())
        self.__connections[connection].symbols = {s: S for s, S in conn_symbols if s not in symbol}

    @property
    def subscribed_symbols_list(self):
        return list(self.__subscribed_symbols.keys())

    def filter_symbols(self, symbols: list[str]) -> list[str]:
        return [symbol for symbol in symbols if symbol not in self.__subscribed_symbols]


def process_exception(exc: Exception):
    """
    Determine whether a connection error is retryable or fatal.

    When reconnecting automatically with ``async for ... in connect(...)``, if a
    connection attempt fails, :func:`process_exception` is called to determine
    whether to retry connecting or to raise the exception.

    This function defines the default behavior, which is to retry on:

    * :exc:`EOFError`, :exc:`OSError`, :exc:`asyncio.TimeoutError`: network
      errors;
    * :exc:`~websockets.exceptions.InvalidStatus` when the status code is 500,
      502, 503, or 504: server or proxy errors.

    All other exceptions are considered fatal.

    You can change this behavior with the ``process_exception`` argument of
    :func:`connect`.

    Return :obj:`None` if the exception is retryable i.e. when the error could
    be transient and trying to reconnect with the same parameters could succeed.
    The exception will be logged at the ``INFO`` level.

    Return an exception, either ``exc`` or a new exception, if the exception is
    fatal i.e. when trying to reconnect will most likely produce the same error.
    That exception will be raised, breaking out of the retry loop.

    """
    print(exc)
    if isinstance(exc, (EOFError, OSError, asyncio.TimeoutError)):
        return None
    if isinstance(exc, InvalidStatus) and exc.response.status_code in [
        500,  # Internal Server Error
        502,  # Bad Gateway
        503,  # Service Unavailable
        504,  # Gateway Timeout
    ]:
        return None
    return exc


class NatsBybitStreamManager:
    def __init__(self, symbols_per_stream, normal_callback):
        self.symbols_per_stream = symbols_per_stream
        ClientConnectionMeta.max_symbols_per_stream = self.symbols_per_stream
        self._process_normal_message = normal_callback
        self.subs_component = SubscriptionsManagerComponent()
        # TODO add conn-task container for killing streams

    async def sub_processor(self, formed_msg: dict, symbol_topic_map: dict[str, str]):
        conn = 0
        if conn_meta := self.subs_component.get_connection():
            conn = conn_meta.connection
        else:
            asyncio.create_task(self.__connection_handler())
            while not conn:
                if conn_meta := self.subs_component.get_connection():
                    conn = conn_meta.connection
                    break
                await asyncio.sleep(0)
        self.subs_component.add_conn_symbol()
        await conn.send(json.dumps(formed_msg))

    def subscribe(self, topic_tmpl, symbol):
        def prepare_subscription_args(list_of_symbols) -> list[str]:
            """
            Prepares the topic for subscription by formatting it with the
            desired symbols.
            """

            topics_map = []
            for single_symbol in list_of_symbols:
                topics_map.append(topic_tmpl.format(symbol=single_symbol))
            return topics_map

        if isinstance(symbol, str):
            symbol = [symbol]

        allowed_to_sub: list[str] = self.subs_component.filter_symbols(symbols=symbol)
        if not allowed_to_sub:
            print(allowed_to_sub)
            return
        prepared_sub_args: list[str] = prepare_subscription_args(allowed_to_sub)
        formed_msg: dict = self._form_message(OpTypes.SUB, list(prepared_sub_args.values()))
        # TODO record symbols in component
        asyncio.create_task(self.sub_processor(formed_msg))

    def _form_message(self, op_type: OpTypes, args: list[str]):
        if op_type not in OpTypes:
            raise ValueError(f'{op_type=}')
        if not args:
            raise ValueError(f'{args=}')
        req_id = str(uuid4())
        msg = {
            'op': op_type.value,
            'req_id': req_id,
            'args': args,
        }
        return msg

    async def __resubscribe(self, last_connection, current_connection):
        if current_connection and last_connection:
            print(f'resubscribe {current_connection=} {last_connection=}')
            # FIXME retunr []
            last_conn_symbols: list[Symbol] = self.subs_component.get_conn_symbol(last_connection)
            print(last_conn_symbols)
            last_topics = [s.topic for s in last_conn_symbols]
            print(last_topics)
            formed_msg: dict = self._form_message(OpTypes.SUB, last_topics)
            print(formed_msg)
            await current_connection.send(json.dumps(formed_msg))
            print('resub sent')
            self.subs_component.set_connection_dead(last_connection)
            # self.subs_component.remove_connection(last_connection)
            self.subs_component.re_add_conn_symbol(current_connection, last_conn_symbols, formed_msg['req_id'])

    async def __connection_handler(self):
        last_connection: Optional[ClientConnection] = None
        async for connection in connect(uri=get_url(), open_timeout=2, process_exception=process_exception):
            print('conn', connection)
            await connection.send(json.dumps({"op": "ping"}))
            resp = json.loads((await connection.recv()))
            if not resp['success']:
                raise RuntimeError('ne sucses')
            self.subs_component.add_connection(conn_id=resp['conn_id'], connection=connection)
            current_connection = connection
            try:
                print(self.subs_component.get_connection())
                task = asyncio.create_task(self._handle_incoming_message(current_connection))
                await self.__resubscribe(last_connection, current_connection)
                await task
            except Exception as e:
                last_connection = connection
                task.cancel()
                print('crush', str(e), type(e))
            else:
                print('regular')

    async def _process_subscription_message(self, message: dict, connection: ClientConnection):
        if message['success']:
            req_id = message['req_id']

    async def _process_unsubscription_message(self, message: dict, connection: ClientConnection):
        ...

    async def _handle_incoming_message(self, connection: ClientConnection):
        while True:
            try:
                msg = await connection.recv()
                message = json.loads(msg)

                def is_normal_message():
                    if message.get('topic') and message.get('data'):
                        return True
                    else:
                        return False

                def is_subscription_message():
                    if (
                            message.get("op") == "subscribe"
                    ):
                        return True
                    else:
                        return False

                def is_unsubscription_message():
                    if message.get('op') == 'unsubscribe':
                        return True
                    else:
                        return False

                if is_normal_message():
                    await self._process_normal_message(message)
                elif is_subscription_message():
                    await self._process_subscription_message(message, connection)
                elif is_unsubscription_message():
                    await self._process_unsubscription_message(message, connection)
            except ConnectionError as e:
                print('ConnectionError')
                raise e
            except Exception as e:
                print('Exception')
                raise e

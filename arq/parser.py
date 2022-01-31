#  type: ignore
# aioredis-py misses lots of type annotations, and there are no external stubs for it

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Dict, Generator, Optional, Type, Union

from aioredis.connection import (
    SERVER_CLOSED_CONNECTION_ERROR,
    Connection,
    EncodableT,
    EncodedT,
    Encoder,
    HiredisParser,
    PythonParser,
)

try:
    import hiredis  # noqa: F401

    HIREDIS_AVAILABLE = True
except ImportError:
    HIREDIS_AVAILABLE = False


encoder_options_var: ContextVar[Optional[Dict[str, Any]]] = ContextVar('encoder_kwargs', default=None)


class ContextAwareEncoder(Encoder):
    """
    Encoder subclass, that is aware of encoder_options_var and can encode/decode on per-request basis
    """

    @contextmanager
    def _encoder_context(self) -> Generator[None, None, None]:
        """
        Reads `encoder_options_var` context variable and changes encoding parameters according to it.

        This way we can encode/decode values on per-request basis, which is not support in aioredis v2 currently
        """
        encoding = self.encoding
        encoding_errors = self.encoding_errors
        decode_responses = self.decode_responses
        encoder_options = encoder_options_var.get()
        if encoder_options:
            self.encoding = encoder_options.get('encoding', encoding)
            self.encoding_errors = encoder_options.get('encoding_errors', encoding_errors)
            self.decode_responses = encoder_options.get('decode_responses', decode_responses)

        yield

        if encoder_options:
            self.encoding = encoding
            self.encoding_errors = encoding_errors
            self.decode_responses = decode_responses

    def encode(self, value: EncodableT) -> EncodedT:
        """
        Calls encode with `_encoder_context`
        """
        with self._encoder_context():
            return super().encode(value)

    def decode(self, value: EncodableT, force=False) -> EncodableT:
        """
        Calls decode with `_encoder_context`
        """
        with self._encoder_context():
            return super().decode(value, force)


class ContextAwareHiredisParser(HiredisParser):
    """
    HiredisParser works differently from PythonParser, because it uses C hiredis Reader to read responses.

    HiredisParser does not store encoder (PythonParser does),
    so we need to subclass almost every method of the parent class to support `_encoder` property

    This way we can use ContextAwareEncoder and update encoding parameters on per-request basis
    """

    __slots__ = HiredisParser.__slots__ + ('_encoder',)

    def __init__(self, socket_read_size: int):
        super().__init__(socket_read_size=socket_read_size)
        self._encoder: Optional[ContextAwareEncoder] = None

    def on_connect(self, connection: 'Connection'):
        super().on_connect(connection)
        self._encoder = connection.encoder

    def on_disconnect(self):
        super().on_disconnect()
        self._encoder = None

    @contextmanager
    def _encoder_context(self):
        """
        `set_encoding` allows us to update hiredis encoding parameters on runtime,
        was added by https://github.com/redis/hiredis-py/pull/96
        """
        if self._encoder:
            with self._encoder._encoder_context():
                self._reader.set_encoding(
                    encoding=self._encoder.encoding if self._encoder.decode_responses else None,
                    errors=self._encoder.encoding_errors,
                )

        yield

        if self._encoder:
            self._reader.set_encoding(
                encoding=self._encoder.encoding if self._encoder.decode_responses else None,
                errors=self._encoder.encoding_errors,
            )

    async def read_response(self) -> EncodableT:
        """
        Basically we port the same method from parent class, but with `_encoder_context` support
        """
        if not self._stream or not self._reader:
            self.on_disconnect()
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR) from None

        # _next_response might be cached from a can_read() call
        if self._next_response is not False:
            response = self._next_response
            self._next_response = False
            return response

        with self._encoder_context():
            response = self._reader.gets()
        while response is False:
            await self.read_from_socket()
            with self._encoder_context():
                response = self._reader.gets()

        # if the response is a ConnectionError or the response is a list and
        # the first item is a ConnectionError, raise it as something bad
        # happened
        if isinstance(response, ConnectionError):
            raise response
        elif isinstance(response, list) and response and isinstance(response[0], ConnectionError):
            raise response[0]
        return response


ContextAwareDefaultParser: Type[Union[PythonParser, ContextAwareHiredisParser]]
if HIREDIS_AVAILABLE:
    ContextAwareDefaultParser = ContextAwareHiredisParser
else:
    ContextAwareDefaultParser = PythonParser

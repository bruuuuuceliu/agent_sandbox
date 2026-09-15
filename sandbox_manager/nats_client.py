"""A minimal asyncio Core NATS client for the manager transports.

The client has deliberate limitations: basic authentication, no TLS, no
JetStream management
convenience layer, no cluster failover, and at-most-once delivery. Slow
consumers drop rather than grow: each subscription holds a bounded queue and
counts what it discarded. Header messages (``HPUB``/``HMSG``) are supported
because JetStream publish acknowledgments, message-ID deduplication, and
pull-delivery status replies all require them; the JetStream semantics
themselves live in ``nats_lifecycle.py``.

The authenticated username of this connection is its transport principal;
payload fields are never evidence of identity.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import secrets
from collections.abc import Mapping
from dataclasses import dataclass

from .identifiers import is_identifier
from .relay_protocol import MAX_FRAME_BYTES

_CONNECT_TIMEOUT = 5.0

_MAX_CONTROL_LINE_BYTES = 64 * 1024

_INBOX_PREFIX = "sandbox.inbox"

_HEADERS_VERSION_LINE = b"NATS/1.0"


class NatsTransportError(RuntimeError):
    """Raised when the Core NATS transport misbehaves."""


class NatsAuthError(NatsTransportError):
    """Raised when the server refuses this connection's credentials."""


class NatsClosedError(NatsTransportError):
    """Raised when operating on a closed connection."""


@dataclass(frozen=True, slots=True)
class NatsMessage:
    subject: str
    reply: str | None
    payload: bytes
    headers: Mapping[str, str] | None = None


class NatsSubscription:
    def __init__(self, subject: str, pending_limit: int) -> None:
        if type(pending_limit) is not int or pending_limit <= 0:
            raise NatsTransportError("pending limit must be a positive integer")
        self.subject = subject
        self.queue: asyncio.Queue[NatsMessage] = asyncio.Queue()
        self.pending_limit = pending_limit
        self.dropped = 0

    def offer(self, message: NatsMessage) -> None:
        if self.queue.qsize() >= self.pending_limit:
            self.dropped += 1
            return
        self.queue.put_nowait(message)


class AsyncNatsConnection:
    """One authenticated Core NATS connection driven by asyncio tasks."""

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        name: str,
    ) -> None:
        if not is_identifier(username):
            raise NatsTransportError(
                "NATS username must be a short single subject token"
            )
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._name = name
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._subs: dict[int, NatsSubscription] = {}
        self._next_sid = 0
        self._buf = b""
        self._max_payload = MAX_FRAME_BYTES
        self._pong = asyncio.Event()
        self._closed = False
        self._error: str | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._write_lock = asyncio.Lock()

    @property
    def principal(self) -> str:
        """The transport-authenticated identity of this connection."""

        return self._username

    @property
    def max_payload(self) -> int:
        """The server-announced maximum publish size."""

        return self._max_payload

    @property
    def closed(self) -> bool:
        return self._closed

    async def connect(self, timeout: float = _CONNECT_TIMEOUT) -> None:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self._host, self._port), timeout=timeout
            )
            self._reader = reader
            self._writer = writer

            info_line = await self._read_line()
            if not info_line.startswith("INFO "):
                raise NatsTransportError("server did not send INFO first")
            try:
                info = json.loads(info_line[5:])
                if not isinstance(info, dict):
                    raise TypeError
                max_payload = info.get("max_payload", MAX_FRAME_BYTES)
                if type(max_payload) is not int or max_payload <= 0:
                    raise TypeError
                self._max_payload = max_payload
            except (TypeError, json.JSONDecodeError) as error:
                raise NatsTransportError("server sent invalid INFO") from error

            self._reader_task = asyncio.create_task(
                self._read_loop(), name=f"nats-read-{self._name}"
            )

            options = {
                "user": self._username,
                "pass": self._password,
                "verbose": False,
                "pedantic": False,
                "tls_required": False,
                "headers": True,
                "no_responders": True,
                "name": self._name,
                "protocol": 1,
                "lang": "py-sandbox-manager",
                "version": "0.1",
            }
            await self._send(
                f"CONNECT {json.dumps(options, separators=(',', ':'))}\r\nPING\r\n".encode(
                    "ascii"
                )
            )

            try:
                await asyncio.wait_for(self._pong.wait(), timeout=timeout)
            except TimeoutError as error:
                raise NatsAuthError("no PONG after CONNECT") from error
            if self._error is not None:
                raise NatsAuthError(self._error)
        except BaseException:
            self._closed = True
            await self._stop_reader()
            await self._teardown()
            raise

    async def subscribe(
        self, subject: str, pending_limit: int = 64
    ) -> NatsSubscription:
        if self._closed:
            raise NatsClosedError("connection is closed")
        _validate_subject(subject, allow_wildcards=True)
        self._next_sid += 1
        sid = self._next_sid
        subscription = NatsSubscription(subject, pending_limit)
        self._subs[sid] = subscription
        await self._send(f"SUB {subject} {sid}\r\n".encode("ascii"))
        return subscription

    async def unsubscribe(self, subscription: NatsSubscription) -> None:
        for sid, candidate in list(self._subs.items()):
            if candidate is subscription:
                del self._subs[sid]
                await self._send(f"UNSUB {sid}\r\n".encode("ascii"))
                return

    async def publish(
        self,
        subject: str,
        payload: bytes,
        reply: str | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        _validate_subject(subject, allow_wildcards=False)
        if reply is not None:
            _validate_subject(reply, allow_wildcards=False)
        if len(payload) > self._max_payload:
            raise NatsTransportError("payload exceeds server max_payload")
        if len(payload) > MAX_FRAME_BYTES:
            raise NatsTransportError("payload exceeds relay frame bound")
        reply_part = f" {reply}" if reply else ""
        if headers is None:
            header = f"PUB {subject}{reply_part} {len(payload)}\r\n".encode(
                "ascii"
            )
            await self._send(header + payload + b"\r\n")
            return
        encoded_headers = encode_headers(headers)
        total = len(encoded_headers) + len(payload)
        if total > self._max_payload:
            raise NatsTransportError("message exceeds server max_payload")
        if total > MAX_FRAME_BYTES:
            raise NatsTransportError("message exceeds relay frame bound")
        header = (
            f"HPUB {subject}{reply_part} {len(encoded_headers)} {total}\r\n"
        ).encode("ascii")
        await self._send(header + encoded_headers + payload + b"\r\n")

    async def flush(self, timeout: float = _CONNECT_TIMEOUT) -> None:
        self._pong.clear()
        await self._send(b"PING\r\n")
        try:
            await asyncio.wait_for(self._pong.wait(), timeout=timeout)
        except TimeoutError as error:
            raise NatsTransportError("no PONG during flush") from error
        if self._closed:
            raise NatsClosedError("connection closed during flush")

    async def request(
        self,
        subject: str,
        payload: bytes,
        timeout: float,
        headers: Mapping[str, str] | None = None,
    ) -> NatsMessage:
        """Publish one request and wait for a single reply on a fresh inbox."""

        inbox = f"{_INBOX_PREFIX}.{self._username}.{secrets.token_urlsafe(12)}"
        subscription = await self.subscribe(inbox, pending_limit=8)
        try:
            await self.publish(subject, payload, reply=inbox, headers=headers)
            return await self.next_message(subscription, timeout)
        finally:
            # Cleanup must never clobber the exception already unwinding:
            # on a closed connection ``unsubscribe`` raises, and an exception
            # raised in ``finally`` replaces the original — including a
            # cancellation, which a consumer loop's fault tolerance must
            # not swallow.
            with contextlib.suppress(NatsClosedError, NatsTransportError):
                await self.unsubscribe(subscription)

    async def next_message(
        self, subscription: NatsSubscription, timeout: float
    ) -> NatsMessage:
        try:
            return await asyncio.wait_for(
                subscription.queue.get(), timeout=timeout
            )
        except TimeoutError as error:
            raise NatsTransportError(
                f"no message on {subscription.subject}"
            ) from error

    async def close(self) -> None:
        self._closed = True
        await self._stop_reader()
        await self._teardown()

    async def _read_loop(self) -> None:
        try:
            while True:
                line = await self._read_line()
                if not line:
                    continue
                operator = line.split(" ", 1)[0]
                if operator == "PING":
                    await self._send(b"PONG\r\n")
                elif operator == "PONG":
                    self._pong.set()
                elif operator in ("MSG", "HMSG"):
                    await self._dispatch(line)
                    if self._error is not None:
                        return
                elif operator.startswith("-ERR"):
                    self._error = line
                    self._pong.set()
                    return
        except asyncio.CancelledError:
            return
        except (OSError, NatsClosedError):
            return
        except Exception as error:  # noqa: BLE001 - protocol parsing fails closed
            self._error = f"protocol_error:{type(error).__name__}"
            self._pong.set()
        finally:
            self._closed = True
            if self._reader_task is asyncio.current_task():
                self._reader_task = None
            await self._teardown()

    async def _dispatch(self, line: str) -> None:
        parts = line.split()
        headers: Mapping[str, str] | None = None
        if parts[0] == "MSG":
            if len(parts) == 4:
                _, subject, sid_text, size_text = parts
                reply = None
            elif len(parts) == 5:
                _, subject, sid_text, reply, size_text = parts
            else:
                self._error = f"malformed MSG line with {len(parts)} fields"
                return
            header_size = 0
        elif parts[0] == "HMSG":
            if len(parts) == 6:
                _, subject, sid_text, reply, header_text, size_text = parts
            elif len(parts) == 5:
                _, subject, sid_text, header_text, size_text = parts
                reply = None
            else:
                self._error = f"malformed HMSG line with {len(parts)} fields"
                return
            header_size = int(header_text)
        else:
            self._error = f"unsupported server operator {parts[0]}"
            return
        sid = int(sid_text)
        size = int(size_text)
        if sid <= 0 or size < 0 or size > MAX_FRAME_BYTES:
            raise NatsTransportError("invalid inbound message size or subscription ID")
        if header_size < 0 or header_size > size:
            raise NatsTransportError("invalid inbound header size")
        if header_size:
            encoded_headers = await self._read_exact(header_size)
            headers = parse_headers(encoded_headers)
        payload = await self._read_exact(size - header_size)
        subscription = self._subs.get(sid)
        if subscription is None:
            return
        subscription.offer(NatsMessage(subject, reply, payload, headers))

    async def _read_line(self) -> str:
        assert self._reader is not None
        while True:
            delimiter = self._buf.find(b"\r\n")
            if delimiter >= 0:
                if delimiter > _MAX_CONTROL_LINE_BYTES:
                    raise NatsTransportError("NATS control line exceeds bound")
                line, self._buf = self._buf[:delimiter], self._buf[delimiter + 2 :]
                return line.decode("ascii", "replace")
            if len(self._buf) > _MAX_CONTROL_LINE_BYTES:
                raise NatsTransportError("NATS control line exceeds bound")
            chunk = await self._reader.read(65536)
            if not chunk:
                raise NatsClosedError("connection closed")
            self._buf += chunk

    async def _read_exact(self, size: int) -> bytes:
        assert self._reader is not None
        if size < 0 or size > MAX_FRAME_BYTES:
            raise NatsTransportError("NATS payload size exceeds bound")
        while len(self._buf) < size:
            chunk = await self._reader.read(65536)
            if not chunk:
                raise NatsClosedError("connection closed mid-payload")
            self._buf += chunk
        data, self._buf = self._buf[:size], self._buf[size:]
        return data

    async def _send(self, data: bytes) -> None:
        if self._closed:
            raise NatsClosedError("connection is closed")
        assert self._writer is not None
        async with self._write_lock:
            self._writer.write(data)
            await self._writer.drain()

    async def _teardown(self) -> None:
        writer = self._writer
        self._reader = None
        self._writer = None
        self._buf = b""
        if writer is None:
            return
        writer.close()
        with contextlib.suppress(OSError, asyncio.CancelledError):
            await writer.wait_closed()

    async def _stop_reader(self) -> None:
        task = self._reader_task
        self._reader_task = None
        if task is None or task is asyncio.current_task():
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def encode_headers(headers: Mapping[str, str]) -> bytes:
    """Encode one NATS header block (``NATS/1.0`` plus key/value lines)."""

    lines = [_HEADERS_VERSION_LINE]
    for key, value in headers.items():
        if not key or ":" in key or "\r" in key or "\n" in key:
            raise NatsTransportError("invalid header name")
        if "\r" in value or "\n" in value:
            raise NatsTransportError("invalid header value")
        lines.append(f"{key}: {value}".encode("ascii"))
    lines.append(b"")
    return b"\r\n".join(lines) + b"\r\n"


def _validate_subject(subject: str, *, allow_wildcards: bool) -> None:
    if not isinstance(subject, str) or not subject:
        raise NatsTransportError("NATS subject is required")
    try:
        subject.encode("ascii")
    except UnicodeEncodeError as error:
        raise NatsTransportError("NATS subject must be ASCII") from error
    tokens = subject.split(".")
    for index, token in enumerate(tokens):
        if not token or any(ord(character) < 33 or ord(character) > 126 for character in token):
            raise NatsTransportError("NATS subject contains an invalid token")
        if token == "*" and allow_wildcards:
            continue
        if token == ">" and allow_wildcards and index == len(tokens) - 1:
            continue
        if "*" in token or ">" in token:
            raise NatsTransportError("NATS subject contains an invalid wildcard")


def parse_headers(block: bytes) -> dict[str, str]:
    """Parse one NATS header block, exposing any status line as ``Status``."""

    lines = block.split(b"\r\n")
    if not lines or not lines[0].startswith(_HEADERS_VERSION_LINE):
        raise NatsTransportError("malformed header block")
    parsed: dict[str, str] = {}
    status = lines[0][len(_HEADERS_VERSION_LINE) :].strip()
    if status:
        parts = status.split(b" ", 1)
        parsed["Status"] = parts[0].decode("ascii", "replace")
        if len(parts) == 2 and parts[1].strip():
            parsed["Description"] = parts[1].strip().decode("ascii", "replace")
    for line in lines[1:]:
        if not line:
            continue
        key, separator, value = line.partition(b":")
        if not separator:
            raise NatsTransportError("malformed header line")
        parsed[key.strip().decode("ascii", "replace")] = (
            value.strip().decode("ascii", "replace")
        )
    return parsed

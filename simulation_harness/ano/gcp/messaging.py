"""``Publisher`` / ``Subscriber`` — the Pub/Sub adapter (contract C4).

Stands in for
-------------
**Google Cloud Pub/Sub** (``google-cloud-pubsub``). In production the ``gcp``
backend wraps ``pubsub_v1.PublisherClient`` and ``pubsub_v1.SubscriberClient``:
:meth:`Publisher.publish` becomes ``publisher.publish(topic_path, data,
**attrs)`` and :meth:`Subscriber.pull` becomes ``subscriber.pull(subscription,
max_messages=...)`` with ``acknowledge()`` for the ack ids.

What changes on deployment
--------------------------
Configuration only: ``messaging_backend`` becomes ``"gcp"``, ``project_id`` is
set, and topic/subscription names gain the project path. Application code keeps
publishing ``bytes`` with a ``dict[str, str]`` of attributes and keeps pulling
and acking messages.

Why an in-process shim rather than the emulator
-----------------------------------------------
The recon survey found the gcloud Pub/Sub emulator is *not installed* and needs
a 50 MiB download plus a JVM — so it cannot be part of a clean offline
checkout (``.agents/explorer_survey_env/handoff.md`` §1.6). The shim is a
``collections.deque``-backed broker with the same method surface.

Fidelity: what is mirrored and what is not
------------------------------------------
Mirrored:

* topics and subscriptions are separate objects; a subscription is attached to
  exactly one topic;
* **a message published to a topic with no subscriptions is dropped** — the
  same footgun as the real service, so a pipeline that forgets to create its
  subscription fails locally the same way it would in production;
* fan-out: every subscription on a topic gets its own copy;
* attributes are ``str -> str``;
* explicit ack/nack, with a nacked message redelivered;
* ``message_id`` and ``publish_time`` are assigned by the broker, not the
  caller.

Deliberately **not** mirrored, and why: real Pub/Sub gives at-least-once
delivery with *no ordering guarantee* (unless ordering keys are used) and
redelivers on an ack deadline. This shim delivers strictly **FIFO per
subscription** and only redelivers on an explicit :meth:`Message.nack`. That is
a deliberate determinism choice — "same seed produces identical scores" is an
acceptance criterion, and a non-deterministic transport would make it
unachievable. The consequence is documented rather than hidden: code that
relies on FIFO here could see reordering against real Pub/Sub, so consumers
must remain order-insensitive (or set an ordering key when deployed).

``message_id`` is a blake2b digest of ``(topic, sequence, payload)`` — stable
across runs, unlike the real service's server-assigned id. Anything that
embeds a ``message_id`` in an artifact therefore stays reproducible.
"""

from __future__ import annotations

import abc
import datetime as _dt
from collections import deque
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from ano.config import LOCAL, Config
from ano.contracts.determinism import stable_hash_hex
from ano.gcp.registry import register_backend

__all__ = [
    "Message",
    "Publisher",
    "Subscriber",
    "LocalBroker",
    "LocalPublisher",
    "LocalSubscriber",
    "TopicNotFoundError",
    "SubscriptionNotFoundError",
    "build_publisher",
    "build_subscriber",
    "get_broker",
    "reset_broker",
]


class TopicNotFoundError(KeyError):
    """Raised when publishing to a topic that does not exist."""


class SubscriptionNotFoundError(KeyError):
    """Raised when pulling from a subscription that does not exist."""


@dataclass(slots=True)
class Message:
    """One delivered Pub/Sub message.

    Attributes:
        data: The payload, as ``bytes``.
        attributes: String attributes, mirroring Pub/Sub message attributes.
        message_id: Broker-assigned id. Deterministic in this shim.
        publish_time: Publish time, or ``None`` if the publisher supplied no
            clock (the offline broker has no wall clock by design).
        ack_id: Opaque id used to ack or nack this delivery.
        delivery_attempt: 1 on first delivery, incremented on redelivery after
            a nack — same semantics as Pub/Sub's ``delivery_attempt``.
    """

    data: bytes
    attributes: Mapping[str, str] = field(default_factory=dict)
    message_id: str = ""
    publish_time: _dt.datetime | None = None
    ack_id: str = ""
    delivery_attempt: int = 1
    _subscriber: "LocalSubscriber | None" = None
    _acked: bool = False

    def ack(self) -> None:
        """Acknowledge this message. Idempotent."""
        if self._acked:
            return
        self._acked = True
        if self._subscriber is not None:
            self._subscriber._ack(self.ack_id)

    def nack(self) -> None:
        """Negative-acknowledge: return the message for redelivery."""
        if self._acked:
            raise RuntimeError(
                f"message {self.message_id} has already been acked and cannot "
                "be nacked"
            )
        self._acked = True
        if self._subscriber is not None:
            self._subscriber._nack(self.ack_id)

    @property
    def acked(self) -> bool:
        """True once :meth:`ack` or :meth:`nack` has been called."""
        return self._acked

    def text(self, encoding: str = "utf-8") -> str:
        """Decode the payload as text."""
        return self.data.decode(encoding)


class Publisher(abc.ABC):
    """Publish messages to a topic."""

    @abc.abstractmethod
    def create_topic(self, topic: str) -> str:
        """Create ``topic`` if absent; return its resolved name. Idempotent."""

    @abc.abstractmethod
    def publish(
        self,
        topic: str,
        data: bytes,
        attributes: Mapping[str, str] | None = None,
        **kwargs: str,
    ) -> str:
        """Publish ``data`` to ``topic``.

        Args:
            topic: Topic name (unqualified; the adapter applies the prefix).
            data: Payload bytes.
            attributes: String attributes.
            **kwargs: Additional attributes, mirroring the real client's
                ``publish(topic, data, **attrs)`` signature.

        Returns:
            The message id.
        """

    def close(self) -> None:
        """Release resources. No-op unless overridden."""

    def __enter__(self) -> "Publisher":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


class Subscriber(abc.ABC):
    """Pull messages from a subscription."""

    @abc.abstractmethod
    def create_subscription(self, subscription: str, topic: str) -> str:
        """Attach ``subscription`` to ``topic``; return its resolved name."""

    @abc.abstractmethod
    def pull(self, subscription: str, max_messages: int = 10) -> list[Message]:
        """Pull up to ``max_messages`` messages. Never blocks."""

    def close(self) -> None:
        """Release resources. No-op unless overridden."""

    def __enter__(self) -> "Subscriber":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


@dataclass(slots=True)
class _Subscription:
    name: str
    topic: str
    queue: deque[tuple[str, bytes, Mapping[str, str], _dt.datetime | None, int]]
    outstanding: dict[str, tuple[str, bytes, Mapping[str, str], _dt.datetime | None, int]]


class LocalBroker:
    """In-process topic/subscription broker.

    Holds all offline messaging state. One broker is shared per process by
    default (see :func:`get_broker`); construct your own for isolation in tests.
    """

    def __init__(self) -> None:
        self._topics: dict[str, list[str]] = {}
        self._subscriptions: dict[str, _Subscription] = {}
        self._sequence: dict[str, int] = {}
        self._ack_sequence = 0

    # -- administration ----------------------------------------------------
    def create_topic(self, topic: str) -> str:
        """Create a topic if absent. Idempotent, like the real API."""
        if not topic:
            raise ValueError("topic name must not be empty")
        self._topics.setdefault(topic, [])
        self._sequence.setdefault(topic, 0)
        return topic

    def create_subscription(self, subscription: str, topic: str) -> str:
        """Attach a subscription to a topic. Idempotent for the same topic."""
        if not subscription:
            raise ValueError("subscription name must not be empty")
        if topic not in self._topics:
            raise TopicNotFoundError(
                f"topic {topic!r} does not exist; create it before subscribing"
            )
        existing = self._subscriptions.get(subscription)
        if existing is not None:
            if existing.topic != topic:
                raise ValueError(
                    f"subscription {subscription!r} is already attached to topic "
                    f"{existing.topic!r} and cannot be re-attached to {topic!r}"
                )
            return subscription
        self._subscriptions[subscription] = _Subscription(
            name=subscription, topic=topic, queue=deque(), outstanding={}
        )
        self._topics[topic].append(subscription)
        return subscription

    def topics(self) -> tuple[str, ...]:
        """Every topic, sorted."""
        return tuple(sorted(self._topics))

    def subscriptions(self, topic: str | None = None) -> tuple[str, ...]:
        """Every subscription, optionally restricted to ``topic``. Sorted."""
        if topic is None:
            return tuple(sorted(self._subscriptions))
        return tuple(sorted(self._topics.get(topic, ())))

    def backlog(self, subscription: str) -> int:
        """Number of undelivered messages on ``subscription``."""
        return len(self._require_subscription(subscription).queue)

    def outstanding(self, subscription: str) -> int:
        """Number of delivered-but-unacked messages on ``subscription``."""
        return len(self._require_subscription(subscription).outstanding)

    def reset(self) -> None:
        """Drop all topics, subscriptions and messages."""
        self._topics.clear()
        self._subscriptions.clear()
        self._sequence.clear()
        self._ack_sequence = 0

    # -- data plane --------------------------------------------------------
    def publish(
        self,
        topic: str,
        data: bytes,
        attributes: Mapping[str, str] | None = None,
        publish_time: _dt.datetime | None = None,
    ) -> str:
        """Publish to every subscription on ``topic``.

        Returns:
            The deterministic message id.

        Raises:
            TopicNotFoundError: if the topic does not exist.
            TypeError: if ``data`` is not bytes-like or an attribute is not a
                string.
        """
        if topic not in self._topics:
            raise TopicNotFoundError(
                f"topic {topic!r} does not exist; call create_topic first"
            )
        if isinstance(data, str):
            raise TypeError(
                "Pub/Sub payloads are bytes; encode your string explicitly "
                "(data.encode('utf-8'))"
            )
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(f"data must be bytes-like, got {type(data).__name__}")
        payload = bytes(data)
        attrs: dict[str, str] = {}
        for key, value in (attributes or {}).items():
            if not isinstance(key, str) or not isinstance(value, str):
                raise TypeError(
                    "Pub/Sub attributes must be string to string; got "
                    f"{key!r}: {value!r}"
                )
            attrs[key] = value

        sequence = self._sequence[topic]
        self._sequence[topic] = sequence + 1
        message_id = stable_hash_hex(
            b"\x1f".join((topic.encode("utf-8"), str(sequence).encode("ascii"), payload)),
            "pubsub",
            "message-id",
            digest_size=12,
        )
        frozen_attrs: Mapping[str, str] = dict(sorted(attrs.items()))
        for subscription in self._topics[topic]:
            self._subscriptions[subscription].queue.append(
                (message_id, payload, frozen_attrs, publish_time, 1)
            )
        return message_id

    def pull(
        self, subscription: str, max_messages: int, owner: "LocalSubscriber"
    ) -> list[Message]:
        """Deliver up to ``max_messages`` messages, FIFO."""
        if max_messages < 1:
            raise ValueError(f"max_messages must be >= 1, got {max_messages}")
        record = self._require_subscription(subscription)
        out: list[Message] = []
        while record.queue and len(out) < max_messages:
            message_id, payload, attrs, publish_time, attempt = record.queue.popleft()
            self._ack_sequence += 1
            ack_id = f"{subscription}:{self._ack_sequence}"
            record.outstanding[ack_id] = (
                message_id,
                payload,
                attrs,
                publish_time,
                attempt,
            )
            out.append(
                Message(
                    data=payload,
                    attributes=attrs,
                    message_id=message_id,
                    publish_time=publish_time,
                    ack_id=ack_id,
                    delivery_attempt=attempt,
                    _subscriber=owner,
                )
            )
        return out

    def ack(self, subscription: str, ack_id: str) -> None:
        """Acknowledge a delivery, removing it permanently."""
        self._require_subscription(subscription).outstanding.pop(ack_id, None)

    def nack(self, subscription: str, ack_id: str) -> None:
        """Return a delivery to the *front* of the queue for redelivery.

        Front, not back, so redelivery order is deterministic and a nacked
        message is retried immediately rather than after everything else.
        """
        record = self._require_subscription(subscription)
        entry = record.outstanding.pop(ack_id, None)
        if entry is None:
            return
        message_id, payload, attrs, publish_time, attempt = entry
        record.queue.appendleft((message_id, payload, attrs, publish_time, attempt + 1))

    def _require_subscription(self, subscription: str) -> _Subscription:
        record = self._subscriptions.get(subscription)
        if record is None:
            raise SubscriptionNotFoundError(
                f"subscription {subscription!r} does not exist; call "
                "create_subscription first"
            )
        return record


_DEFAULT_BROKER = LocalBroker()


def get_broker() -> LocalBroker:
    """The process-wide default broker."""
    return _DEFAULT_BROKER


def reset_broker() -> None:
    """Clear the process-wide default broker. Intended for tests."""
    _DEFAULT_BROKER.reset()


class LocalPublisher(Publisher):
    """Offline Pub/Sub publisher.

    Args:
        broker: Broker to publish into; defaults to the process-wide one.
        topic_prefix: Prefix applied to topic names, mirroring the project-level
            namespacing real topics have.
        clock: Optional zero-argument callable returning an aware ``datetime``
            used as ``publish_time``. **Omitted by default**: the offline
            pipeline has no wall clock, and stamping one would make artifacts
            differ between runs. Scenario replay should pass an event-time clock.
    """

    def __init__(
        self,
        broker: LocalBroker | None = None,
        *,
        topic_prefix: str = "",
        clock: Any = None,
    ) -> None:
        self._broker = broker if broker is not None else get_broker()
        self._prefix = topic_prefix
        self._clock = clock

    @property
    def broker(self) -> LocalBroker:
        """The broker this publisher writes to."""
        return self._broker

    def topic_path(self, topic: str) -> str:
        """Resolve an unqualified topic name to its prefixed form."""
        return f"{self._prefix}-{topic}" if self._prefix else topic

    def create_topic(self, topic: str) -> str:
        """Create the topic if absent; returns the resolved name."""
        return self._broker.create_topic(self.topic_path(topic))

    def publish(
        self,
        topic: str,
        data: bytes,
        attributes: Mapping[str, str] | None = None,
        **kwargs: str,
    ) -> str:
        """Publish to ``topic``; see :meth:`Publisher.publish`."""
        merged = dict(attributes or {})
        merged.update(kwargs)
        publish_time = self._clock() if self._clock is not None else None
        return self._broker.publish(
            self.topic_path(topic), data, merged, publish_time=publish_time
        )

    def subscriber(self, subscription_prefix: str = "") -> "LocalSubscriber":
        """Return a subscriber bound to the *same* broker as this publisher.

        Messaging has two roles behind one registry entry; this is how
        :func:`build_subscriber` obtains the matching reader without creating a
        second, empty broker.
        """
        return LocalSubscriber(
            self._broker,
            topic_prefix=self._prefix,
            subscription_prefix=subscription_prefix,
        )

    @classmethod
    def from_config(cls, config: Config) -> "LocalPublisher":
        """Build the offline publisher from a :class:`ano.config.Config`."""
        return cls(topic_prefix=config.topic_prefix)


class LocalSubscriber(Subscriber):
    """Offline Pub/Sub subscriber.

    Args:
        broker: Broker to pull from; defaults to the process-wide one.
        topic_prefix: Must match the publisher's.
        subscription_prefix: Prefix applied to subscription names.
    """

    def __init__(
        self,
        broker: LocalBroker | None = None,
        *,
        topic_prefix: str = "",
        subscription_prefix: str = "",
    ) -> None:
        self._broker = broker if broker is not None else get_broker()
        self._topic_prefix = topic_prefix
        self._subscription_prefix = subscription_prefix

    @property
    def broker(self) -> LocalBroker:
        """The broker this subscriber reads from."""
        return self._broker

    def topic_path(self, topic: str) -> str:
        """Resolve an unqualified topic name."""
        return f"{self._topic_prefix}-{topic}" if self._topic_prefix else topic

    def subscription_path(self, subscription: str) -> str:
        """Resolve an unqualified subscription name."""
        return (
            f"{self._subscription_prefix}-{subscription}"
            if self._subscription_prefix
            else subscription
        )

    def create_subscription(self, subscription: str, topic: str) -> str:
        """Attach ``subscription`` to ``topic``; returns the resolved name."""
        return self._broker.create_subscription(
            self.subscription_path(subscription), self.topic_path(topic)
        )

    def pull(self, subscription: str, max_messages: int = 10) -> list[Message]:
        """Pull up to ``max_messages`` messages in FIFO order."""
        return self._broker.pull(
            self.subscription_path(subscription), max_messages, self
        )

    def pull_all(self, subscription: str, batch: int = 100) -> list[Message]:
        """Drain the subscription, pulling repeatedly until it is empty."""
        out: list[Message] = []
        while True:
            page = self.pull(subscription, batch)
            if not page:
                return out
            out.extend(page)

    def ack_all(self, messages: Iterable[Message]) -> int:
        """Acknowledge every message in ``messages``.

        Returns:
            The number acked.
        """
        count = 0
        for message in messages:
            message.ack()
            count += 1
        return count

    def _ack(self, ack_id: str) -> None:
        subscription = ack_id.rsplit(":", 1)[0]
        self._broker.ack(subscription, ack_id)

    def _nack(self, ack_id: str) -> None:
        subscription = ack_id.rsplit(":", 1)[0]
        self._broker.nack(subscription, ack_id)

    @classmethod
    def from_config(cls, config: Config) -> "LocalSubscriber":
        """Build the offline subscriber from a :class:`ano.config.Config`."""
        return cls(
            topic_prefix=config.topic_prefix,
            subscription_prefix=config.subscription_prefix,
        )


def build_publisher(config: Config) -> Publisher:
    """Build the configured publisher (see :func:`ano.gcp.registry.build`)."""
    from ano.gcp.registry import build

    return build("messaging", config)


def build_subscriber(config: Config) -> Subscriber:
    """Build the configured subscriber.

    The registry keys adapters by service and messaging has two roles, so a
    messaging factory may return either a :class:`Subscriber` or an object
    exposing ``subscriber(subscription_prefix)`` that returns one bound to the
    same broker/connection. Both shapes are supported, which is what lets a
    ``gcp`` plugin register a single messaging entry.
    """
    from ano.gcp.registry import build

    built = build("messaging", config)
    if isinstance(built, Subscriber):
        return built
    factory = getattr(built, "subscriber", None)
    if callable(factory):
        return factory(config.subscription_prefix)
    raise TypeError(
        "the registered messaging factory returned "
        f"{type(built).__name__}, which is neither a Subscriber nor exposes "
        "subscriber(subscription_prefix)"
    )


register_backend("messaging", LOCAL, LocalPublisher.from_config)

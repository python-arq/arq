"""Methods shared between producers and consumers."""

from typing import Dict


def shared_messaging_attributes(
    redis_host: str, redis_port: int
) -> Dict[str, str | int]:
    """Get semantic attributes that apply to all spans."""
    # TODO: I think resource attributes should be included by the tracer already
    # but I need to check
    return {
        "server.address": redis_host,
        "server.port": redis_port,
        "messaging.system": "redis",
        "messaging.destination.kind": "queue",
        "messaging.protocol": "RESP",
    }

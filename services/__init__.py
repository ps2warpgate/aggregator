"""Abstraction for external services"""

from .AlertService import Alert, Alert2
from .RabbitService import Rabbit


__all__ = [
    'Alert',
    'Alert2',
    'Rabbit',
]
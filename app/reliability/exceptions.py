class TransientTaskError(Exception):
    """Signals a transient failure that should be retried."""


class NonRetryableTaskError(Exception):
    """Signals a failure that must not be retried."""


class CircuitOpenError(Exception):
    """Raised when the circuit breaker is open for a task type."""

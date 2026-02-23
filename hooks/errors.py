from __future__ import annotations

from typing import Any, Optional, Type, final


class AppError(Exception):
    """
    Base application error with structured metadata.
    """

    error_code: str = "APP_ERROR"
    http_status: int = 500

    def __init__(
        self,
        message: str = "",
        *,
        context: dict[str, Any] | None = None,
        cause: BaseException | None = None,
        error_code: str | None = None,
        http_status: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(message)
        self.message = message or self.__class__.__name__
        self.context: dict[str, Any] = dict(context or {})
        self.cause = cause
        if error_code is not None:
            self.error_code = error_code
        if http_status is not None:
            self.http_status = http_status
        if cause is not None:
            self.__cause__ = cause  # keep chaining

    def with_context(self, **kwargs: Any) -> AppError:
        self.context.update(kwargs)
        return self

    def with_cause(self, cause: BaseException) -> AppError:
        self.cause = cause
        self.__cause__ = cause  # type: ignore[attr-defined]
        return self

    def to_dict(self, *, include_cause_type: bool = True) -> dict[str, Any]:
        data = {
            "type": self.__class__.__name__,
            "message": self.message,
            "error_code": self.error_code,
            "http_status": self.http_status,
            "context": self._safe_context(self.context),
        }
        if include_cause_type and self.cause is not None:
            data["cause"] = type(self.cause).__name__
        return data

    @classmethod
    def from_exc(
        cls: Type[AppError],
        exc: BaseException,
        *,
        message: Optional[str] = None,
        context: Optional[dict[str, Any]] = None,
    ) -> AppError:
        return cls(message or str(exc), context=context, cause=exc)

    def __str__(self) -> str:
        base = f"{self.__class__.__name__}({self.error_code}): {self.message}"
        if self.context:
            base += f" | context={self._compact_context(self.context)}"
        if self.cause is not None:
            base += f" | cause={type(self.cause).__name__}"
        return base

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} code={self.error_code} status={self.http_status}>"

    @staticmethod
    def _safe_context(ctx: dict[str, Any]) -> dict[str, Any]:
        REDACT_KEYS = {"password", "secret", "token", "api_key", "authorization"}
        out: dict[str, Any] = {}
        for k, v in ctx.items():
            if any(s in k.lower() for s in REDACT_KEYS):
                out[k] = "***REDACTED***"
            else:
                out[k] = v
        return out

    @staticmethod
    def _compact_context(ctx: dict[str, Any]) -> str:
        def _short(x: Any) -> str:
            s = str(x)
            return s if len(s) <= 120 else s[:117] + "..."

        parts = [f"{k}={_short(v)}" for k, v in ctx.items()]
        return "{" + ", ".join(parts) + "}"


# ------------------------------
# Configuration & infrastructure
# ------------------------------


class ConfigError(AppError):
    error_code: str = "CONFIG_ERROR"
    http_status: int = 500


class InfrastructureError(AppError):
    error_code: str = "INFRASTRUCTURE_ERROR"
    http_status: int = 500


# ----------------------
# Validation & schema
# ----------------------


class ValidationError(AppError):
    error_code: str = "VALIDATION_ERROR"
    http_status: int = 400


@final
class DataValidationError(ValidationError):
    error_code = "DATA_VALIDATION_ERROR"
    http_status = 400


@final
class SchemaMismatchError(ValidationError):
    error_code = "SCHEMA_MISMATCH"
    http_status = 400


@final
class DataParsingError(ValidationError):
    error_code = "DATA_PARSING_ERROR"
    http_status = 400


@final
class EmptyDatasetError(ValidationError):
    error_code = "EMPTY_DATASET"
    http_status = 400


# -------------
# I/O & files
# -------------


@final
class FileSystemError(AppError):
    error_code = "FILESYSTEM_ERROR"
    http_status = 500


@final
class SerializationError(AppError):
    error_code = "SERIALIZATION_ERROR"
    http_status = 500


# ----------
# Databases
# ----------


class DatabaseError(AppError):
    error_code: str = "DATABASE_ERROR"
    http_status: int = 500


@final
class DatabaseConnectionError(DatabaseError):
    error_code = "DB_CONNECTION_FAILED"
    http_status = 503


@final
class DatabaseTimeoutError(DatabaseError):
    error_code = "DB_TIMEOUT"
    http_status = 504


class DatabaseWriteError(DatabaseError):
    error_code: str = "DB_WRITE_FAILED"
    http_status: int = 500


@final
class DuplicateKeyError(DatabaseWriteError):
    error_code = "DB_DUPLICATE_KEY"
    http_status = 409


@final
class NotFoundError(DatabaseError):
    error_code = "DB_NOT_FOUND"
    http_status = 404


@final
class QueryError(DatabaseError):
    error_code = "DB_QUERY_ERROR"
    http_status = 500


@final
class PerformanceError(DatabaseError):
    error_code = "DB_PERFORMANCE_ISSUE"
    http_status = 500


# ------------------
# External services
# ------------------


class ExternalServiceError(AppError):
    error_code: str = "EXTERNAL_SERVICE_ERROR"
    http_status: int = 502


@final
class AuthError(ExternalServiceError):
    error_code = "AUTH_ERROR"
    http_status = 401


@final
class PermissionError(ExternalServiceError):
    error_code = "PERMISSION_DENIED"
    http_status = 403


@final
class RateLimitError(ExternalServiceError):
    error_code = "RATE_LIMITED"
    http_status = 429


@final
class TimeoutError(ExternalServiceError):
    error_code = "UPSTREAM_TIMEOUT"
    http_status = 504


# -------------
# Compute / ML
# -------------


@final
class ComputationError(AppError):
    error_code = "COMPUTATION_ERROR"
    http_status = 500


@final
class TrainingError(AppError):
    error_code = "TRAINING_ERROR"
    http_status = 500


# ----------------------
# Concurrency & logic
# ----------------------


@final
class ConcurrencyError(AppError):
    error_code = "CONCURRENCY_ERROR"
    http_status = 500


@final
class LogicalError(AppError):
    error_code = "LOGICAL_ERROR"
    http_status = 500


@final
class ConfigurationError(AppError):
    error_code = "CONFIGURATION_ERROR"
    http_status = 500

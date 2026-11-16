from __future__ import annotations

from airflow.exceptions import AirflowException


class CidpApiError(AirflowException):
    """An error answer from the data API: HTTP status, the API's error ``code`` and ``x-request-id``."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        code: str | None = None,
        request_id: str | None = None,
    ):
        super().__init__(message)
        self.status_code: int | None = status_code  # type: ignore[assignment]
        self.code = code
        self.request_id = request_id


class CidpAuthError(CidpApiError):
    """401/403, or the connection cannot sign in (bad config, token endpoint refused)."""


class CidpNotFoundError(CidpApiError):
    """404: the workspace, project, job, run or statement does not exist (or is not visible)."""


class CidpConflictError(CidpApiError):
    """409: e.g. an ``Idempotency-Key`` reused for another request, or a project that already exists."""


class CidpUnavailableError(CidpApiError):
    """503: the service or a feature is unavailable."""

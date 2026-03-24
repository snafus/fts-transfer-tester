"""
fts_framework.exceptions

.. note::
   ``Optional`` is imported here solely for use in PEP 484 type comments on
   ``__init__`` signatures.  No runtime import of ``typing`` is required.

~~~~~~~~~~~~~~~~~~~~~~~~
All framework-level exception types.

Hierarchy::

    FTSFrameworkError
    ├── ConfigError
    ├── InventoryError
    ├── ChecksumFetchError
    ├── SubmissionError
    ├── PollingTimeoutError
    ├── TokenExpiredError
    ├── PersistenceError
    ├── CleanupError          (non-fatal by default)
    ├── ResumeError
    └── _TransientHTTPError   (internal; not raised to callers)
"""



class FTSFrameworkError(Exception):
    """Base class for all fts_framework exceptions."""


class ConfigError(FTSFrameworkError):
    """Raised for malformed or missing configuration values."""


class InventoryError(FTSFrameworkError):
    """Raised for an unreadable, empty, or malformed PFN inventory file."""


class ChecksumFetchError(FTSFrameworkError):
    """Raised when a Want-Digest HEAD request fails after all retries.

    Attributes:
        pfn (str): The source PFN that could not be checksummed.
        reason (str): Human-readable failure reason.
    """

    def __init__(self, pfn, reason):
        # type: (str, str) -> None
        self.pfn = pfn
        self.reason = reason
        super(ChecksumFetchError, self).__init__(
            "Failed to fetch checksum for {!r}: {}".format(pfn, reason)
        )


class SubmissionError(FTSFrameworkError):
    """Raised when an FTS3 job submission fails and cannot be recovered.

    Attributes:
        chunk_index (int): Zero-based index of the chunk that failed.
        status_code (int): HTTP status code returned by FTS3.
        detail (str): Response body or additional context.
    """

    def __init__(self, chunk_index, status_code, detail):
        # type: (int, int, str) -> None
        self.chunk_index = chunk_index
        self.status_code = status_code
        self.detail = detail
        super(SubmissionError, self).__init__(
            "Submission failed for chunk {} (HTTP {}): {}".format(
                chunk_index, status_code, detail
            )
        )


class PollingTimeoutError(FTSFrameworkError):
    """Raised when ``campaign_timeout_s`` is exceeded before all jobs reach a terminal state.

    Attributes:
        active_job_ids (list): Job IDs still in a non-terminal state at timeout.
    """

    def __init__(self, active_job_ids):
        # type: (list) -> None
        self.active_job_ids = list(active_job_ids)
        super(PollingTimeoutError, self).__init__(
            "Campaign timeout exceeded with {} job(s) still active: {}".format(
                len(self.active_job_ids), self.active_job_ids
            )
        )


class TokenExpiredError(FTSFrameworkError):
    """Raised when FTS3 returns HTTP 401, indicating the bearer token has expired.

    FTS3 manages token refresh; this error signals the operator must re-acquire
    a token and resume the run.

    Attributes:
        job_id (str or None): Job ID being polled when expiry was detected, if known.
    """

    def __init__(self, job_id=None):
        # type: (Optional[str]) -> None
        self.job_id = job_id
        msg = "Bearer token expired"
        if job_id:
            msg += " while polling job {!r}".format(job_id)
        msg += ". Re-acquire a token and resume the run."
        super(TokenExpiredError, self).__init__(msg)


class PersistenceError(FTSFrameworkError):
    """Raised on a disk write failure in the persistence layer.

    Attributes:
        path (str): File path that could not be written.
        reason (str): Underlying OS error message.
    """

    def __init__(self, path, reason):
        # type: (str, str) -> None
        self.path = path
        self.reason = reason
        super(PersistenceError, self).__init__(
            "Failed to write {!r}: {}".format(path, reason)
        )


class CleanupError(FTSFrameworkError):
    """Raised when a WebDAV DELETE returns an unexpected status code.

    Non-fatal by default: the runner logs this and continues.

    Attributes:
        url (str): Destination URL that could not be deleted.
        status_code (int): HTTP status code returned.
    """

    def __init__(self, url, status_code):
        # type: (str, int) -> None
        self.url = url
        self.status_code = status_code
        super(CleanupError, self).__init__(
            "DELETE {!r} returned HTTP {}".format(url, status_code)
        )


class ResumeError(FTSFrameworkError):
    """Raised when a run cannot be resumed due to a corrupt or missing manifest.

    Attributes:
        path (str): Path to the manifest file that could not be read or parsed.
        reason (str): Underlying error message.
    """

    def __init__(self, path, reason):
        # type: (str, str) -> None
        self.path = path
        self.reason = reason
        super(ResumeError, self).__init__(
            "Cannot resume run from manifest {!r}: {}".format(path, reason)
        )


class _TransientHTTPError(FTSFrameworkError):
    """Internal signal raised by the HTTP retry wrapper for retryable status codes.

    Not intended to propagate to callers; caught and retried within the client layer.

    Attributes:
        status_code (int): The HTTP status code that triggered the retry.
    """

    def __init__(self, status_code):
        # type: (int) -> None
        self.status_code = status_code
        super(_TransientHTTPError, self).__init__(
            "Transient HTTP error: {}".format(status_code)
        )

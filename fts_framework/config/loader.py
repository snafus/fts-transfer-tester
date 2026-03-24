"""
fts_framework.config.loader
~~~~~~~~~~~~~~~~~~~~~~~~~~~
YAML configuration loader and validator for the FTS3 test framework.

Loads a YAML file, applies defaults for all optional fields, validates
required fields and value constraints, and returns a normalised config dict.

Usage::

    from fts_framework.config.loader import load
    config = load("/path/to/config.yaml")

The returned dict is the single config object passed through the entire
framework pipeline.  It is never mutated after construction.

Token safety contract
---------------------
Token values from ``config["tokens"]`` must never be logged, written to disk,
or included in exception messages.  This module does not log token values.
Callers must uphold the same contract.  Any log of the full ``config`` dict
at any level would violate it.
"""

import logging
import os

import yaml

from fts_framework.exceptions import ConfigError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Allowed constant values for enum-like fields
# ---------------------------------------------------------------------------

_ALLOWED_CHECKSUM_ALGORITHMS = ("adler32",)
_ALLOWED_VERIFY_CHECKSUM = ("both", "source", "target", "none")

# ---------------------------------------------------------------------------
# Defaults for optional fields only.
# Required sections (fts, tokens) have no defaults — absence is an error.
# ---------------------------------------------------------------------------

_DEFAULTS = {
    "run": {
        "run_id": None,
    },
    "transfer": {
        "preserve_extension": False,
        "checksum_algorithm": "adler32",
        "verify_checksum": "both",
        "overwrite": False,
        "chunk_size": 200,
        "priority": 3,
        "activity": "default",
        "job_metadata": {},
    },
    "concurrency": {
        "want_digest_workers": 8,
    },
    "submission": {
        "scan_window_s": 300,
    },
    "polling": {
        "initial_interval_s": 30,
        "backoff_multiplier": 1.5,
        "max_interval_s": 300,
        "campaign_timeout_s": 86400,
    },
    "cleanup": {
        "before": False,
        "after": False,
    },
    "retry": {
        "fts_retry_max": 2,
        "framework_retry_max": 0,
        "min_success_threshold": 0.95,
    },
    "output": {
        "base_dir": "runs",
        "reports": {
            "console": True,
            "json": True,
            "markdown": True,
            "html": False,
        },
    },
}

# Sections that must be dicts — validated before merging
_DICT_SECTIONS = set(_DEFAULTS.keys()) | {"fts", "tokens"}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load(path):
    # type: (str) -> dict
    """Load, validate, and return the framework config from a YAML file.

    Applies defaults for all optional fields before validation, so the
    returned dict always contains every key defined in the schema.

    Args:
        path (str): Absolute or relative path to the YAML config file.

    Returns:
        dict: Fully populated and validated config dict.

    Raises:
        ConfigError: If the file cannot be read or parsed, or if any
            required field is missing or fails a value constraint.
    """
    logger.info("Loading config from: %s", path)

    raw = _read_yaml(path)
    _check_section_types(raw)
    config = _apply_defaults(raw)
    _validate(config)

    logger.info(
        "Config loaded — endpoint=%s ssl_verify=%s test_label=%s chunk_size=%d",
        config["fts"]["endpoint"],
        config["fts"]["ssl_verify"],
        config["run"]["test_label"],
        config["transfer"]["chunk_size"],
    )
    return config


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _read_yaml(path):
    # type: (str) -> dict
    """Read and parse the YAML file at *path*.

    Raises:
        ConfigError: On I/O failure or YAML parse error.
    """
    try:
        with open(path, "r") as fh:
            raw = yaml.safe_load(fh)
    except IOError as exc:
        raise ConfigError("Cannot read config file {!r}: {}".format(path, exc))
    except yaml.YAMLError as exc:
        raise ConfigError("YAML parse error in {!r}: {}".format(path, exc))

    if not isinstance(raw, dict):
        raise ConfigError(
            "Config file must contain a YAML mapping at the top level, got: {!r}".format(
                type(raw).__name__
            )
        )
    return raw


def _check_section_types(raw):
    # type: (dict) -> None
    """Verify that every known section present in *raw* is a dict.

    Catches mistakes like ``transfer: null`` or ``cleanup: false`` before
    they silently corrupt the merge.

    Raises:
        ConfigError: If any known section is present but not a dict.
    """
    for section in _DICT_SECTIONS:
        val = raw.get(section)
        if val is not None and not isinstance(val, dict):
            raise ConfigError(
                "Config section {!r} must be a YAML mapping, got {!r}. "
                "Check for accidental scalar value (e.g. 'section: null').".format(
                    section, type(val).__name__
                )
            )


def _deep_merge(defaults, override):
    # type: (dict, dict) -> dict
    """Return a new dict with *override* values merged into *defaults*.

    Recursively merges nested dicts.  Values in *override* always take
    precedence.  Neither input dict is mutated.
    """
    result = dict(defaults)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _apply_defaults(raw):
    # type: (dict) -> dict
    """Return a new config dict with all default values filled in.

    Sections present in *raw* are deep-merged over their defaults.
    Sections absent from *raw* receive their full default subtree.
    Sections not covered by ``_DEFAULTS`` (future extensions, required-only
    sections like ``fts`` and ``tokens``) are passed through unchanged.

    A ``None`` value for a section is treated as absent (no override).
    Non-dict section values are rejected by ``_check_section_types`` before
    this function is called.
    """
    result = {}

    for section, defaults in _DEFAULTS.items():
        raw_section = raw.get(section)
        # None means the key was absent or explicitly null — use defaults as-is
        if raw_section is None:
            raw_section = {}
        result[section] = _deep_merge(defaults, raw_section)

    # Pass through sections not covered by _DEFAULTS (e.g. fts, tokens,
    # and any unknown sections for forward-compatibility)
    for section in raw:
        if section not in result:
            result[section] = raw[section]

    return result


def _validate(config):
    # type: (dict) -> None
    """Validate required fields, types, and value constraints.

    Calls each section validator in dependency order; raises on the first
    failure to produce focused, actionable error messages.

    Raises:
        ConfigError: On the first validation failure encountered.
    """
    _validate_run(config)
    _validate_fts(config)
    _validate_tokens(config)
    _validate_transfer(config)
    _validate_concurrency(config)
    _validate_submission(config)
    _validate_polling(config)
    _validate_retry(config)
    _validate_output(config)
    logger.debug("Config validation passed")


def _require_str(config, section, key):
    # type: (dict, str, str) -> str
    """Return ``config[section][key]`` as a non-empty string.

    Raises:
        ConfigError: If the key is absent or its value is ``None`` or ``""``.
    """
    val = config.get(section, {}).get(key)
    if val is None or val == "":
        raise ConfigError("Missing required field: {}.{}".format(section, key))
    if not isinstance(val, str):
        raise ConfigError(
            "{}.{} must be a string, got {!r}".format(section, key, val)
        )
    return val


def _require_int(value, field_name, min_val=None, max_val=None):
    # type: (object, str, int, int) -> int
    """Validate that *value* is an integer within [min_val, max_val].

    Provides a clear error for float literals (e.g. ``30.0`` written in YAML).

    Raises:
        ConfigError: On type or range failure.
    """
    if isinstance(value, float) and value == int(value):
        raise ConfigError(
            "{} must be a whole number integer, not a float. "
            "Remove the decimal point (got {!r})".format(field_name, value)
        )
    if not isinstance(value, int) or isinstance(value, bool):
        raise ConfigError(
            "{} must be an integer, got {!r}".format(field_name, value)
        )
    if min_val is not None and value < min_val:
        raise ConfigError(
            "{} must be >= {}, got {!r}".format(field_name, min_val, value)
        )
    if max_val is not None and value > max_val:
        raise ConfigError(
            "{} must be <= {}, got {!r}".format(field_name, max_val, value)
        )
    return value


def _validate_run(config):
    # type: (dict) -> None
    _require_str(config, "run", "test_label")


def _validate_fts(config):
    # type: (dict) -> None
    if "fts" not in config or not config["fts"]:
        raise ConfigError("Missing required config section: 'fts'")

    endpoint = _require_str(config, "fts", "endpoint")
    if not endpoint.startswith("https://"):
        raise ConfigError(
            "fts.endpoint must be an https:// URL, got: {!r}".format(endpoint)
        )

    # ssl_verify may be True/False (YAML booleans) or a CA bundle path string.
    # YAML's safe_load parses unquoted true/false as Python booleans.
    # A quoted "true" becomes the string "true" and will fail the path check.
    ssl_verify = config["fts"].get("ssl_verify")
    if ssl_verify is None:
        raise ConfigError("Missing required field: fts.ssl_verify")

    if ssl_verify is not True and ssl_verify is not False:
        if not isinstance(ssl_verify, str):
            raise ConfigError(
                "fts.ssl_verify must be true, false, or a CA bundle path string, "
                "got: {!r}".format(ssl_verify)
            )
        if not os.path.exists(ssl_verify):
            raise ConfigError(
                "fts.ssl_verify CA bundle path does not exist: {!r}".format(ssl_verify)
            )


def _validate_tokens(config):
    # type: (dict) -> None
    if "tokens" not in config or not config["tokens"]:
        raise ConfigError("Missing required config section: 'tokens'")
    for key in ("fts_submit", "source_read", "dest_write"):
        val = config["tokens"].get(key)
        if not val or not isinstance(val, str):
            # Do not include the token value in the error message
            raise ConfigError(
                "Missing or empty required field: tokens.{}".format(key)
            )


def _validate_transfer(config):
    # type: (dict) -> None
    _require_str(config, "transfer", "source_pfns_file")

    dst_prefix = _require_str(config, "transfer", "dst_prefix")
    if not dst_prefix.startswith("https://"):
        raise ConfigError(
            "transfer.dst_prefix must be an https:// URL, got: {!r}".format(dst_prefix)
        )

    _require_int(
        config["transfer"]["chunk_size"], "transfer.chunk_size", min_val=1, max_val=200
    )
    _require_int(
        config["transfer"]["priority"], "transfer.priority", min_val=1, max_val=5
    )

    algo = config["transfer"]["checksum_algorithm"]
    if algo not in _ALLOWED_CHECKSUM_ALGORITHMS:
        raise ConfigError(
            "transfer.checksum_algorithm must be one of {}, got: {!r}".format(
                _ALLOWED_CHECKSUM_ALGORITHMS, algo
            )
        )

    verify = config["transfer"]["verify_checksum"]
    if verify not in _ALLOWED_VERIFY_CHECKSUM:
        raise ConfigError(
            "transfer.verify_checksum must be one of {}, got: {!r}".format(
                _ALLOWED_VERIFY_CHECKSUM, verify
            )
        )


def _validate_concurrency(config):
    # type: (dict) -> None
    _require_int(
        config["concurrency"]["want_digest_workers"],
        "concurrency.want_digest_workers",
        min_val=1,
    )


def _validate_submission(config):
    # type: (dict) -> None
    _require_int(
        config["submission"]["scan_window_s"],
        "submission.scan_window_s",
        min_val=60,
    )


def _validate_polling(config):
    # type: (dict) -> None
    polling = config["polling"]

    initial = _require_int(
        polling["initial_interval_s"], "polling.initial_interval_s", min_val=1
    )
    _require_int(
        polling["max_interval_s"], "polling.max_interval_s", min_val=initial
    )
    _require_int(
        polling["campaign_timeout_s"], "polling.campaign_timeout_s", min_val=1
    )

    backoff = polling["backoff_multiplier"]
    if not isinstance(backoff, (int, float)) or isinstance(backoff, bool):
        raise ConfigError(
            "polling.backoff_multiplier must be a number, got {!r}".format(backoff)
        )
    if float(backoff) < 1.0:
        raise ConfigError(
            "polling.backoff_multiplier must be >= 1.0, got {!r}".format(backoff)
        )


def _validate_retry(config):
    # type: (dict) -> None
    _require_int(
        config["retry"]["fts_retry_max"], "retry.fts_retry_max", min_val=0
    )
    _require_int(
        config["retry"]["framework_retry_max"], "retry.framework_retry_max", min_val=0
    )

    threshold = config["retry"]["min_success_threshold"]
    if not isinstance(threshold, (int, float)) or isinstance(threshold, bool):
        raise ConfigError(
            "retry.min_success_threshold must be a float, got {!r}".format(threshold)
        )
    if not (0.0 <= float(threshold) <= 1.0):
        raise ConfigError(
            "retry.min_success_threshold must be between 0.0 and 1.0, "
            "got {!r}".format(threshold)
        )


def _validate_output(config):
    # type: (dict) -> None
    _require_str(config, "output", "base_dir")

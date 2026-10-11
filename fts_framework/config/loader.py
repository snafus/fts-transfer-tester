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

Token resolution order (highest wins)
--------------------------------------
1. Explicit per-role keyword arguments passed to ``load()`` — used by the
   CLI for ``--fts-submit-token``, ``--source-read-token``, ``--dest-write-token``.
2. Shared ``--token`` argument (also via ``load()`` keyword) — applies the
   same value to all three roles.
3. Per-role environment variables: ``FTS_SUBMIT_TOKEN``, ``SOURCE_READ_TOKEN``,
   ``DEST_WRITE_TOKEN``.
4. Shared environment variable ``FTS_TOKEN`` — applies to all three roles.
5. Values in the YAML ``tokens`` section.
6. OIDC client-credentials generation (``oidc.enabled: true``) — fires for
   any role still unset after steps 1–5.

The YAML ``tokens`` section may be omitted entirely if all three roles are
supplied through environment variables or keyword arguments.

X.509 proxy mode (``auth.mode: x509``)
--------------------------------------
All roles authenticate with one VOMS proxy presented as the TLS client
certificate; no bearer tokens are used anywhere (no mixed mode).  Setting
``tokens``, a token CLI argument, or ``oidc.enabled`` in this mode is a
``ConfigError``; token environment variables are ignored with a warning.
Proxy path priority: ``x509_proxy`` keyword (CLI ``--x509-proxy``, which
also forces x509 mode) > ``$X509_USER_PROXY`` > ``auth.x509_proxy`` >
``/tmp/x509up_u<uid>``.  The resolved path is stored back into
``config["auth"]["x509_proxy"]``.  Delegation to FTS3 is the user's
responsibility and is checked at campaign start (see ``auth.x509``).
"""

import logging
import os
from urllib.parse import urlparse as _urlparse

import yaml

from fts_framework.exceptions import ConfigError
from fts_framework.auth import env_loader as _env_loader
from fts_framework.auth import oidc as _oidc
from fts_framework.auth import x509 as _x509
from fts_framework.fts.copy_mode import ALLOWED_COPY_MODES as _ALLOWED_COPY_MODES

logger = logging.getLogger(__name__)

# Environment variable names for token resolution.
_ENV_TOKEN_SHARED = "FTS_TOKEN"
_ENV_TOKEN_ROLES = {
    "fts_submit": "FTS_SUBMIT_TOKEN",
    "source_read": "SOURCE_READ_TOKEN",
    "dest_write": "DEST_WRITE_TOKEN",
}

# ---------------------------------------------------------------------------
# Allowed constant values for enum-like fields
# ---------------------------------------------------------------------------

_ALLOWED_CHECKSUM_ALGORITHMS = ("adler32",)
_ALLOWED_VERIFY_CHECKSUM = ("both", "source", "target", "none")
_ALLOWED_DESTINATION_NAMING = ("sequential", "uuid", "date")
_ALLOWED_AUTH_MODES = ("token", "x509")

# ---------------------------------------------------------------------------
# Defaults for optional fields only.
# Required sections (fts, tokens) have no defaults — absence is an error.
# ---------------------------------------------------------------------------

_DEFAULTS = {
    "auth": {
        "mode": "token",
        "x509_proxy": None,
        "x509_min_lifetime_s": 3600,
    },
    "oidc": {
        "enabled": False,
        "env_file": ".env",
        "roles": {},
    },
    "run": {
        "run_id": None,
    },
    "transfer": {
        "preserve_extension": False,
        "destination_naming": "sequential",
        "checksum_algorithm": "adler32",
        "verify_checksum": "both",
        "overwrite": False,
        "max_files": None,
        "shuffle_source_pfns": False,
        "chunk_size": 200,
        "priority": 3,
        "activity": "default",
        "job_metadata": {},
        "unmanaged_tokens": False,
        "source_prefix": None,
        "destinations": None,
        "nostreams": None,
        "copy_mode": None,
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
        "stuck_active_check_rounds": 10,
        "poll_error_max_consecutive": 3,
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
        "timeseries_bucket_s": 60,
        "reports": {
            "console": True,
            "json": True,
            "markdown": True,
            "html": False,
            "csv": True,
            "timeseries_csv": True,
        },
    },
}

# Sections that must be dicts — validated before merging
_DICT_SECTIONS = set(_DEFAULTS.keys()) | {"fts", "tokens"}


# ---------------------------------------------------------------------------
# Token source identification (for --check-tokens reporting)
# ---------------------------------------------------------------------------

def identify_token_sources(path, token=None, fts_submit_token=None,
                           source_read_token=None, dest_write_token=None,
                           x509_proxy=None):
    # type: (str, object, object, object, object, object) -> dict
    """Return a dict describing where each token role will be sourced from.

    Does NOT fetch OIDC tokens — reports 'oidc' as the source if that is
    where the role would be resolved.  Raises ConfigError if the file cannot
    be read.

    Returns:
        dict mapping role name → source description string, e.g.:
            {
                "fts_submit":  "cli (--fts-submit-token)",
                "source_read": "env (SOURCE_READ_TOKEN)",
                "dest_write":  "oidc (https://iam.example.org/token)",
            }
    """
    raw = _read_yaml(path)
    config = _apply_defaults(raw)
    oidc_cfg = config.get("oidc", {})

    if x509_proxy or config["auth"]["mode"] == "x509":
        proxy = _x509.resolve_proxy_path(
            cli_path=x509_proxy, yaml_path=config["auth"].get("x509_proxy"),
        )
        return {role: "x509 proxy ({})".format(proxy)
                for role in ("fts_submit", "source_read", "dest_write")}

    sources = {}
    for role in ("fts_submit", "source_read", "dest_write"):
        # Check in priority order, highest first.
        per_role_cli = {
            "fts_submit":  fts_submit_token,
            "source_read": source_read_token,
            "dest_write":  dest_write_token,
        }.get(role)
        if per_role_cli:
            sources[role] = "cli (--{}-token)".format(role.replace("_", "-"))
            continue
        if token:
            sources[role] = "cli (--token)"
            continue
        per_role_env = os.environ.get(_ENV_TOKEN_ROLES[role])
        if per_role_env:
            sources[role] = "env ({})".format(_ENV_TOKEN_ROLES[role])
            continue
        shared_env = os.environ.get(_ENV_TOKEN_SHARED)
        if shared_env:
            sources[role] = "env ({})".format(_ENV_TOKEN_SHARED)
            continue
        yaml_val = (raw.get("tokens") or {}).get(role)
        if yaml_val:
            sources[role] = "yaml (tokens.{})".format(role)
            continue
        if oidc_cfg.get("enabled") and (oidc_cfg.get("roles") or {}).get(role):
            endpoint = oidc_cfg["roles"][role].get("token_endpoint", "?")
            sources[role] = "oidc ({})".format(endpoint)
            continue
        sources[role] = "MISSING"

    return sources


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load(path, token=None, fts_submit_token=None,
         source_read_token=None, dest_write_token=None, x509_proxy=None):
    # type: (str, object, object, object, object, object) -> dict
    """Load, validate, and return the framework config from a YAML file.

    Applies defaults for all optional fields before validation, so the
    returned dict always contains every key defined in the schema.

    Token values are resolved in this order (highest wins):

    1. *fts_submit_token* / *source_read_token* / *dest_write_token* — explicit
       per-role values (typically from CLI ``--fts-submit-token`` etc.).
    2. *token* — a single shared value applied to all three roles.
    3. ``FTS_SUBMIT_TOKEN`` / ``SOURCE_READ_TOKEN`` / ``DEST_WRITE_TOKEN``
       environment variables (per-role).
    4. ``FTS_TOKEN`` environment variable (shared).
    5. Values in the YAML ``tokens`` section.

    The ``tokens`` section may be absent from the YAML if all three roles
    are satisfied through the above sources.

    Args:
        path (str): Absolute or relative path to the YAML config file.
        token (str or None): Shared bearer token for all three roles.
        fts_submit_token (str or None): Token for FTS3 job submission.
        source_read_token (str or None): Token for source storage reads.
        dest_write_token (str or None): Token for destination storage writes.
        x509_proxy (str or None): X.509 proxy path; forces ``auth.mode: x509``.

    Returns:
        dict: Fully populated and validated config dict.

    Raises:
        ConfigError: If the file cannot be read or parsed, or if any
            required field is missing or fails a value constraint.
    """
    logger.info("Loading config from: %s", path)

    raw = _read_yaml(path)
    _check_section_types(raw)

    if x509_proxy:
        raw["auth"] = dict(raw.get("auth") or {}, mode="x509")
    if (raw.get("auth") or {}).get("mode") == "x509":
        cli_tokens = (token, fts_submit_token, source_read_token, dest_write_token)
        config = _load_x509(raw, x509_proxy, any(cli_tokens))
        logger.info(
            "Config loaded — endpoint=%s ssl_verify=%s test_label=%s chunk_size=%d "
            "auth=x509 proxy=%s",
            config["fts"]["endpoint"],
            config["fts"]["ssl_verify"],
            config["run"]["test_label"],
            config["transfer"]["chunk_size"],
            config["auth"]["x509_proxy"],
        )
        return config

    # Build resolved token map: YAML < shared-env < per-role-env < shared-CLI < per-role-CLI
    resolved = {}

    # 4. Shared env var
    shared_env = os.environ.get(_ENV_TOKEN_SHARED)
    if shared_env:
        resolved["fts_submit"] = shared_env
        resolved["source_read"] = shared_env
        resolved["dest_write"] = shared_env

    # 3. Per-role env vars
    for role, env_var in _ENV_TOKEN_ROLES.items():
        val = os.environ.get(env_var)
        if val:
            resolved[role] = val

    # 2. Shared CLI token
    if token:
        resolved["fts_submit"] = token
        resolved["source_read"] = token
        resolved["dest_write"] = token

    # 1. Per-role CLI tokens
    if fts_submit_token:
        resolved["fts_submit"] = fts_submit_token
    if source_read_token:
        resolved["source_read"] = source_read_token
    if dest_write_token:
        resolved["dest_write"] = dest_write_token

    if resolved:
        tokens_section = dict(raw.get("tokens") or {})
        tokens_section.update(resolved)
        raw["tokens"] = tokens_section

    config = _apply_defaults(raw)
    _validate_oidc(config)
    _resolve_oidc_tokens(config)
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

def _load_x509(raw, cli_proxy, cli_tokens_given):
    # type: (dict, object, bool) -> dict
    """Build and validate the config for ``auth.mode: x509``.

    Raises:
        ConfigError: If any token source other than environment variables
            is configured, if OIDC is enabled, or if the proxy file is
            missing or malformed.
    """
    if cli_tokens_given:
        raise ConfigError(
            "auth.mode is x509: bearer token CLI arguments cannot be combined "
            "with X.509 proxy authentication"
        )
    if any((raw.get("tokens") or {}).values()):
        raise ConfigError(
            "auth.mode is x509: remove the 'tokens' section (mixed token/proxy "
            "authentication is not supported)"
        )
    if (raw.get("oidc") or {}).get("enabled"):
        raise ConfigError(
            "auth.mode is x509: oidc.enabled must be false"
        )
    env_tokens = [v for v in [_ENV_TOKEN_SHARED] + sorted(_ENV_TOKEN_ROLES.values())
                  if os.environ.get(v)]
    if env_tokens:
        logger.warning(
            "auth.mode is x509: ignoring token environment variable(s) %s",
            ", ".join(env_tokens),
        )

    config = _apply_defaults(raw)
    config["tokens"] = {}
    auth_cfg = config["auth"]
    auth_cfg["x509_proxy"] = _x509.resolve_proxy_path(
        cli_path=cli_proxy, yaml_path=auth_cfg.get("x509_proxy"),
    )
    _validate(config)
    return config


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


def _resolve_scope_template(scope, config):
    # type: (str, dict) -> str
    """Substitute {dst_prefix_path} and {src_prefix_path} in a scope string.

    Raises:
        ConfigError: If {src_prefix_path} is used but transfer.source_prefix
            is not set.
    """
    if "{dst_prefix_path}" in scope:
        dst = (config.get("transfer") or {}).get("dst_prefix") or ""
        scope = scope.replace("{dst_prefix_path}", _urlparse(dst).path if dst else "")
    if "{src_prefix_path}" in scope:
        src = (config.get("transfer") or {}).get("source_prefix") or ""
        if not src:
            raise ConfigError(
                "OIDC scope uses {src_prefix_path} but transfer.source_prefix is not set"
            )
        scope = scope.replace("{src_prefix_path}", _urlparse(src).path)
    return scope


def refresh_oidc_tokens_for_roles(config, roles):
    # type: (dict, list) -> None
    """Re-fetch OIDC tokens for *roles*, re-evaluating scope templates.

    Intended for the sequence runner when a per-trial override (e.g.
    ``transfer.dst_prefix``) changes the effective scope for a role.
    Clears the stored token for each named role then re-runs OIDC
    resolution so the template is substituted against the updated config.
    """
    tokens = config.setdefault("tokens", {})
    for role in roles:
        tokens[role] = None
    _resolve_oidc_tokens(config)


def _resolve_oidc_tokens(config):
    # type: (dict) -> None
    """Fetch OIDC tokens for any role not yet satisfied by higher-priority sources.

    Only runs when ``oidc.enabled`` is True.  Tokens are injected directly
    into ``config["tokens"]`` before validation.
    """
    oidc_cfg = config.get("oidc", {})
    if not oidc_cfg.get("enabled"):
        return

    tokens = config.setdefault("tokens", {})
    roles_cfg = oidc_cfg.get("roles") or {}
    if not roles_cfg:
        return

    ssl_verify = config.get("fts", {}).get("ssl_verify", True)

    # Load .env file once; ignore missing file if no role needs it
    env_file_vars = {}
    env_file = oidc_cfg.get("env_file") or ".env"
    if os.path.isfile(env_file):
        try:
            env_file_vars = _env_loader.load_env_file(env_file)
        except IOError as exc:
            raise ConfigError("Cannot read oidc.env_file {!r}: {}".format(env_file, exc))

    for role in ("fts_submit", "source_read", "dest_write"):
        if tokens.get(role):
            continue
        role_cfg = roles_cfg.get(role)
        if not role_cfg:
            continue

        endpoint    = role_cfg.get("token_endpoint") or ""
        id_var      = role_cfg.get("client_id_var") or ""
        secret_var  = role_cfg.get("client_secret_var") or ""
        scope       = _resolve_scope_template(role_cfg.get("scope") or "", config)
        audience    = role_cfg.get("audience") or None

        client_id     = _env_loader.resolve_var(id_var, os.environ, env_file_vars) if id_var else None
        client_secret = _env_loader.resolve_var(secret_var, os.environ, env_file_vars) if secret_var else None

        if not client_id:
            raise ConfigError(
                "OIDC role {!r}: client_id_var {!r} not set in environment or env_file".format(
                    role, id_var
                )
            )
        if not client_secret:
            raise ConfigError(
                "OIDC role {!r}: client_secret_var {!r} not set in environment or env_file".format(
                    role, secret_var
                )
            )

        logger.info("Fetching OIDC token for role %r from %s", role, endpoint)
        tokens[role] = _oidc.fetch_token(
            endpoint, client_id, client_secret, scope, ssl_verify, audience=audience
        )


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
    _validate_auth(config)
    if config["auth"]["mode"] == "x509":
        _x509.validate_proxy_file(config["auth"]["x509_proxy"])
    else:
        _validate_oidc(config)
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


def _validate_auth(config):
    # type: (dict) -> None
    mode = config["auth"]["mode"]
    if mode not in _ALLOWED_AUTH_MODES:
        raise ConfigError(
            "auth.mode must be one of {}, got: {!r}".format(_ALLOWED_AUTH_MODES, mode)
        )
    _require_int(
        config["auth"]["x509_min_lifetime_s"], "auth.x509_min_lifetime_s", min_val=0,
    )


def _validate_oidc(config):
    # type: (dict) -> None
    """Validate the oidc section structure when enabled."""
    oidc_cfg = config.get("oidc", {})
    if not oidc_cfg.get("enabled"):
        return

    roles_cfg = oidc_cfg.get("roles") or {}
    for role, role_cfg in roles_cfg.items():
        if not isinstance(role_cfg, dict):
            raise ConfigError(
                "oidc.roles.{} must be a mapping, got {!r}".format(role, type(role_cfg).__name__)
            )
        endpoint = role_cfg.get("token_endpoint") or ""
        if not endpoint.startswith("https://"):
            raise ConfigError(
                "oidc.roles.{}.token_endpoint must be an https:// URL, got: {!r}".format(
                    role, endpoint
                )
            )
        for field in ("client_id_var", "client_secret_var", "scope"):
            if not role_cfg.get(field):
                raise ConfigError(
                    "oidc.roles.{}.{} is required".format(role, field)
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


def _validate_destinations(destinations):
    # type: (object) -> None
    """Validate the transfer.destinations list."""
    if not isinstance(destinations, list) or len(destinations) < 1:
        raise ConfigError(
            "transfer.destinations must be a non-empty list"
        )
    for i, entry in enumerate(destinations):
        if not isinstance(entry, dict):
            raise ConfigError(
                "transfer.destinations[{}] must be a mapping with 'prefix' and 'weight'".format(i)
            )
        prefix = entry.get("prefix") or ""
        if not (prefix.startswith("https://") or prefix.startswith("davs://")):
            raise ConfigError(
                "transfer.destinations[{}].prefix must be an https:// or davs:// URL, "
                "got: {!r}".format(i, prefix)
            )
        weight = entry.get("weight")
        if weight is None:
            raise ConfigError(
                "transfer.destinations[{}].weight is required".format(i)
            )
        if not isinstance(weight, int) or isinstance(weight, bool) or weight < 1:
            raise ConfigError(
                "transfer.destinations[{}].weight must be a positive integer, "
                "got: {!r}".format(i, weight)
            )


def _validate_transfer(config):
    # type: (dict) -> None
    _require_str(config, "transfer", "source_pfns_file")

    destinations = config["transfer"].get("destinations")
    if destinations is not None:
        _validate_destinations(destinations)
    else:
        dst_prefix = config["transfer"].get("dst_prefix")
        if not dst_prefix:
            raise ConfigError(
                "transfer.dst_prefix is required when transfer.destinations is not set"
            )
        if not (dst_prefix.startswith("https://") or dst_prefix.startswith("davs://")):
            raise ConfigError(
                "transfer.dst_prefix must be an https:// or davs:// URL, got: {!r}".format(
                    dst_prefix
                )
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

    destination_naming = config["transfer"]["destination_naming"]
    if destination_naming not in _ALLOWED_DESTINATION_NAMING:
        raise ConfigError(
            "transfer.destination_naming must be one of {}, got: {!r}".format(
                _ALLOWED_DESTINATION_NAMING, destination_naming
            )
        )

    max_files = config["transfer"]["max_files"]
    if max_files is not None:
        _require_int(max_files, "transfer.max_files", min_val=1)

    shuffle = config["transfer"]["shuffle_source_pfns"]
    if not isinstance(shuffle, bool):
        raise ConfigError(
            "transfer.shuffle_source_pfns must be a boolean (true/false), got {!r}".format(
                shuffle
            )
        )

    unmanaged_tokens = config["transfer"]["unmanaged_tokens"]
    if not isinstance(unmanaged_tokens, bool):
        raise ConfigError(
            "transfer.unmanaged_tokens must be a boolean (true/false), got {!r}".format(
                unmanaged_tokens
            )
        )

    source_prefix = config["transfer"]["source_prefix"]
    if source_prefix is not None:
        if not isinstance(source_prefix, str) or not (
            source_prefix.startswith("https://") or source_prefix.startswith("davs://")
        ):
            raise ConfigError(
                "transfer.source_prefix must be an https:// or davs:// URL, got: {!r}".format(
                    source_prefix
                )
            )

    nostreams = config["transfer"]["nostreams"]
    if nostreams is not None:
        _require_int(nostreams, "transfer.nostreams", min_val=1)

    copy_mode = config["transfer"]["copy_mode"]
    if copy_mode is not None and copy_mode not in _ALLOWED_COPY_MODES:
        raise ConfigError(
            "transfer.copy_mode must be null or one of {}, got: {!r}".format(
                _ALLOWED_COPY_MODES, copy_mode
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

    _require_int(
        polling["stuck_active_check_rounds"],
        "polling.stuck_active_check_rounds",
        min_val=0,
    )
    _require_int(
        polling["poll_error_max_consecutive"],
        "polling.poll_error_max_consecutive",
        min_val=1,
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

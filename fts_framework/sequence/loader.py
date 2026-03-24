"""
fts_framework.sequence.loader
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Parse and validate a sequence parameter file; generate the list of
(case_params) dicts that the sequence runner will execute.

Sequence parameter file format (YAML)::

    baseline_config: "config/my_campaign.yaml"

    sequence:
      trials: 3
      label: "scale_test"          # optional; used in directory name

      sweep:
        mode: cartesian            # cartesian (default) | zip
        parameters:
          transfer.max_files: [100, 200, 500]
          transfer.source_pfns_file: ["small.txt", "large.txt"]
          # Integer range shorthand:
          # transfer.chunk_size: {range: [50, 200, 50]}  -> [50, 100, 150, 200]

      output:
        base_dir: "sequences"      # where sequence state/reports are written

Parameter keys must use dot-notation matching the baseline config structure
(e.g. ``transfer.max_files``).  A scalar value is treated as a single-element
list.

Cartesian mode (default):  generates the full cross-product of all parameter
lists.  Zip mode: pairs elements positionally; all lists must have equal
length.
"""

import itertools
import os

import yaml

from fts_framework.exceptions import ConfigError


# ---------------------------------------------------------------------------
# Range expansion
# ---------------------------------------------------------------------------

def expand_range(spec):
    # type: (dict) -> list
    """Expand ``{range: [start, stop, step]}`` to a list of integers.

    The range is inclusive of *stop* (unlike Python's ``range``).
    *step* must be a non-zero integer.  All three values must be integers.

    Args:
        spec (dict): Must have the form ``{"range": [start, stop, step]}``.

    Returns:
        list[int]: Expanded list.

    Raises:
        ConfigError: on any validation failure or empty result.
    """
    if not isinstance(spec, dict) or "range" not in spec:
        raise ConfigError(
            "range spec must be a mapping with a 'range' key, "
            "e.g. {{range: [10, 100, 10]}}"
        )
    r = spec["range"]
    if not isinstance(r, list) or len(r) != 3:
        raise ConfigError(
            "range value must be a list of exactly 3 integers "
            "[start, stop, step]; got: {}".format(r)
        )
    start, stop, step = r
    for name, val in (("start", start), ("stop", stop), ("step", step)):
        if not isinstance(val, int):
            raise ConfigError(
                "range {}: expected integer, got {} ({})".format(
                    name, type(val).__name__, val
                )
            )
    if step == 0:
        raise ConfigError("range step must not be zero")

    # Build inclusive range
    result = []
    v = start
    if step > 0:
        while v <= stop:
            result.append(v)
            v += step
    else:
        while v >= stop:
            result.append(v)
            v += step

    if not result:
        raise ConfigError(
            "range [{}, {}, {}] produces an empty list".format(start, stop, step)
        )
    return result


# ---------------------------------------------------------------------------
# Case generation
# ---------------------------------------------------------------------------

def _normalise_param_list(key, value):
    # type: (str, object) -> list
    """Normalise a parameter value to a non-empty list."""
    if isinstance(value, dict):
        return expand_range(value)
    if isinstance(value, list):
        if len(value) == 0:
            raise ConfigError(
                "sweep parameter '{}' has an empty list".format(key)
            )
        return value
    # Scalar → single-element list
    return [value]


def generate_cases(sweep_config):
    # type: (dict) -> list
    """Generate the list of parameter override dicts from a sweep config.

    Args:
        sweep_config (dict): The ``sweep`` sub-section of the sequence params.

    Returns:
        list[dict]: One dict per case; each maps dotted key -> value.

    Raises:
        ConfigError: on invalid mode, empty parameters, or zip length mismatch.
    """
    mode = sweep_config.get("mode", "cartesian")
    if mode not in ("cartesian", "zip"):
        raise ConfigError(
            "sweep.mode must be 'cartesian' or 'zip'; got: '{}'".format(mode)
        )

    params = sweep_config.get("parameters", {})
    if not params:
        raise ConfigError("sweep.parameters must not be empty")

    keys = list(params.keys())
    lists = [_normalise_param_list(k, params[k]) for k in keys]

    if mode == "cartesian":
        combos = list(itertools.product(*lists))
    else:  # zip
        lengths = [len(lst) for lst in lists]
        if len(set(lengths)) > 1:
            raise ConfigError(
                "sweep.mode=zip requires all parameter lists to have the "
                "same length; got: {}".format(
                    dict(zip(keys, lengths))
                )
            )
        combos = list(zip(*lists))

    return [dict(zip(keys, combo)) for combo in combos]


# ---------------------------------------------------------------------------
# Config override application
# ---------------------------------------------------------------------------

def apply_override(config, dotkey, value):
    # type: (dict, str, object) -> None
    """Apply a dotted-key override to a config dict **in-place**.

    Args:
        config (dict): Validated framework config dict (will be mutated).
        dotkey (str): Dot-separated key path, e.g. ``"transfer.max_files"``.
        value: Value to set.

    Raises:
        ConfigError: if the key has fewer than two components, or if an
            intermediate section is missing from the config.
    """
    parts = dotkey.split(".")
    if len(parts) < 2:
        raise ConfigError(
            "parameter key '{}' must be dot-separated "
            "(e.g. 'transfer.max_files')".format(dotkey)
        )
    d = config
    for part in parts[:-1]:
        if part not in d:
            raise ConfigError(
                "parameter key '{}': section '{}' not found in config".format(
                    dotkey, part
                )
            )
        d = d[part]
    d[parts[-1]] = value


# ---------------------------------------------------------------------------
# Top-level loader
# ---------------------------------------------------------------------------

def load(path):
    # type: (str) -> dict
    """Load and validate a sequence parameter file.

    Args:
        path (str): Path to the sequence YAML file.

    Returns:
        dict with keys:

        - ``baseline_config_path`` (str)
        - ``trials`` (int >= 1)
        - ``label`` (str | None)
        - ``sweep_mode`` (str)
        - ``cases`` (list[dict])  -- one dict per case, dotted key -> value
        - ``output_base_dir`` (str)

    Raises:
        ConfigError: on any validation failure.
    """
    if not os.path.isfile(path):
        raise ConfigError(
            "sequence params file not found: {}".format(path)
        )

    with open(path, "r") as fh:
        raw = yaml.safe_load(fh)

    if not isinstance(raw, dict):
        raise ConfigError("sequence params file must be a YAML mapping")

    # baseline_config
    baseline_config_path = raw.get("baseline_config")
    if not baseline_config_path:
        raise ConfigError("baseline_config is required in the sequence params file")
    if not isinstance(baseline_config_path, str):
        raise ConfigError("baseline_config must be a string path")
    if not os.path.isfile(baseline_config_path):
        raise ConfigError(
            "baseline_config not found: {}".format(baseline_config_path)
        )

    # sequence section
    seq = raw.get("sequence", {})
    if not isinstance(seq, dict):
        raise ConfigError("sequence must be a YAML mapping")

    # trials
    trials = seq.get("trials", 1)
    if not isinstance(trials, int) or trials < 1:
        raise ConfigError(
            "sequence.trials must be an integer >= 1; got: {}".format(trials)
        )

    # label
    label = seq.get("label", None)
    if label is not None and not isinstance(label, str):
        raise ConfigError("sequence.label must be a string or null")

    # sweep
    sweep = seq.get("sweep", {})
    if not isinstance(sweep, dict):
        raise ConfigError("sequence.sweep must be a YAML mapping")

    sweep_mode = sweep.get("mode", "cartesian")
    cases = generate_cases(sweep)

    # output
    output = seq.get("output", {})
    if not isinstance(output, dict):
        raise ConfigError("sequence.output must be a YAML mapping")
    output_base_dir = output.get("base_dir", "sequences")
    if not isinstance(output_base_dir, str):
        raise ConfigError("sequence.output.base_dir must be a string")

    return {
        "baseline_config_path": baseline_config_path,
        "trials": trials,
        "label": label,
        "sweep_mode": sweep_mode,
        "cases": cases,
        "output_base_dir": output_base_dir,
    }

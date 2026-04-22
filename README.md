# lacelang-executor (python)

Canonical Python executor for [Lace](https://github.com/tracedown/lacelang) —
the reference implementation with **100% spec conformance**. Runs `.lace`
scripts against real HTTP endpoints and emits ProbeResult JSON.

This is the executor that the Lace specification is developed and tested
against. Conformance vectors, error codes, and wire-format schemas are
verified against this implementation before each spec release.

Parsing and semantic validation are delegated to
[`lacelang-validator`](https://github.com/tracedown/lacelang-python-validator) — this package contains
only the runtime (HTTP client, assertion evaluation, cookie jars, extension
dispatch). See `lace-spec.md` §15 for the validator / executor package
separation rule.

## Install

Not published to PyPI yet. Install from source:

```bash
# 1. Install the validator (required dependency)
pip install git+https://github.com/tracedown/lacelang-python-validator.git

# 2. Install the executor
pip install git+https://github.com/tracedown/lacelang-python-executor.git
```

Or from local clones:

```bash
git clone https://github.com/tracedown/lacelang-python-validator.git
git clone https://github.com/tracedown/lacelang-python-executor.git

pip install ./lacelang-python-validator
pip install ./lacelang-python-executor
```

## Usage

```bash
# Parse (delegates to validator)
lacelang-executor parse script.lace

# Validate (delegates to validator)
lacelang-executor validate script.lace --context context.json --vars-list vars.json

# Run — full HTTP execution
lacelang-executor run script.lace \
    --vars vars.json \
    --prev prev.json \
    --bodies-dir ./bodies

# Enable extensions (laceNotifications is built in)
lacelang-executor run script.lace --enable-extension laceNotifications
```

All subcommands support `--pretty` for indented JSON.

## Library usage

### Project layout

The executor expects Lace files under a dedicated `lace/` directory:

```
my-project/
  lace/
    lace.config                      # executor config (auto-discovered)
    config.staging.json              # env-specific overrides (optional)
    extensions/                      # third-party extensions
      myext/
        myext.laceext
        myext.config
    scripts/
      health/
        health.lace                  # script (name = directory name)
        vars.json                    # default variables
        vars.staging.json            # env-specific variables
      auth-flow/
        auth-flow.lace
        vars.json
```

All paths are overridable at runtime — the layout is a convention,
not a requirement.

### `LaceExecutor` + `LaceProbe`

```python
from lacelang_executor import LaceExecutor

# Point to the lace/ directory — config loaded once
executor = LaceExecutor("lace")

# Or override the config path directly
executor = LaceExecutor(config="path/to/lace.config", env="staging")

# Register third-party extensions
executor.extension("lace/extensions/myext")           # directory (finds myext.laceext + myext.config)
executor.extension("path/to/custom.laceext", "path/to/custom.config")  # explicit paths

# Prepare a probe by name — resolves to lace/scripts/health/health.lace
# AST is parsed and validated once, reused across runs
probe = executor.probe("health")

# Run — returns a ProbeResult dict
result = probe.run(vars={"base_url": "https://api.example.com"})

# Run again — prev result from last run injected automatically
result = probe.run()

# All inputs accept file paths or dicts
result = probe.run(
    vars="lace/scripts/health/vars.staging.json",
    prev="results/last_run.json",    # explicit prev overrides auto-tracking
)
```

### One-shot execution

```python
# No probe caching, no prev tracking
result = executor.run("lace/scripts/health/health.lace", vars={"key": "val"})

# Inline source
result = executor.run('''
get("https://api.example.com/health")
    .expect(status: 200)
''')
```

### Development mode

```python
# Re-read and re-parse the script file on every run()
probe = executor.probe("health", always_reparse=True)
```

### API reference

**`LaceExecutor(root, *, config, env, extensions, track_prev)`**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `root` | `str \| None` | `None` | Path to the `lace/` directory. Discovers `lace.config` and resolves script names relative to `{root}/scripts/`. |
| `config` | `str \| None` | `None` | Explicit path to `lace.config` (overrides root-based discovery). |
| `env` | `str \| None` | `None` | Selects `[lace.config.{env}]` section (overrides `LACE_ENV`). |
| `extensions` | `list[str] \| None` | `None` | Built-in extensions to activate (e.g. `["laceNotifications"]`). |
| `track_prev` | `bool` | `True` | Auto-store last result as `prev` for next run on each probe. |

**`executor.extension(path, config_path=None)`** — register a third-party extension.

**`executor.probe(script, *, vars, always_reparse)`** — prepare a reusable probe.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `script` | `str` | — | Script name (`"health"`), file path (`"path/to.lace"`), or inline source. |
| `always_reparse` | `bool` | `False` | Re-read script file on every `run()`. |

**`probe.run(vars, prev, *, reparse)`** — execute and return ProbeResult.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `vars` | `str \| dict \| None` | `None` | Script variables — dict or path to JSON. |
| `prev` | `str \| dict \| None` | `None` | Previous result — dict or path to JSON. Overrides auto-tracking. |
| `reparse` | `bool` | `False` | Re-read script from disk for this run only. |

### Config and environment overrides

There is exactly **one config file** per executor. The `env` parameter
selects a **section within that file**, not a different file.

```toml
# lace/lace.config

[executor]
maxRedirects = 10
maxTimeoutMs = 300000

# Staging overlay — deep-merged on top of base.
# Only the keys you specify are overridden; the rest is inherited.
[lace.config.staging]
[lace.config.staging.executor]
maxTimeoutMs = 60000           # overridden
# maxRedirects is inherited (10)

[lace.config.production]
[lace.config.production.executor]
user_agent = "lace-probe/0.9.1 (acme-platform)"
```

Resolution by constructor arguments:

| Constructor | Config file | Env overlay |
|-------------|-------------|-------------|
| `LaceExecutor("lace")` | `lace/lace.config` | none (base only) |
| `LaceExecutor("lace", env="staging")` | `lace/lace.config` | `[lace.config.staging]` merged on base |
| `LaceExecutor(config="/path/lace.config", env="prod")` | `/path/lace.config` | `[lace.config.prod]` merged on base |
| `LaceExecutor("lace", config="/other/lace.config")` | `/other/lace.config` | none (root still used for script names) |
| `LACE_ENV=staging` + `LaceExecutor("lace")` | `lace/lace.config` | `[lace.config.staging]` (from env var) |
| `LACE_ENV=staging` + `LaceExecutor("lace", env="prod")` | `lace/lace.config` | `[lace.config.prod]` (kwarg wins) |

The `config=` kwarg overrides where the file is loaded from.
The `env=` kwarg (or `LACE_ENV`) selects which section inside
that file is overlaid. They are independent — setting one does
not affect the other.

### Return value

Both `probe.run()` and `executor.run()` return a `dict` matching the
ProbeResult wire format (spec §9):

```python
{
    "outcome": "success",        # "success" | "failure" | "timeout"
    "startedAt": "2026-04-20T10:00:00.000Z",
    "endedAt":   "2026-04-20T10:00:01.234Z",
    "elapsedMs": 1234,
    "runVars":   {},             # run-scoped variables from .store()
    "calls":     [...],          # per-call result records
    "actions":   {},             # write-back variables, notifications, etc.
}
```

### Low-level API

The stateless `run_script()` function is available for callers that need
full control over parsing, validation, and config:

```python
from lacelang_validator.parser import parse
from lacelang_executor.executor import run_script
from lacelang_executor.config import load_config

ast = parse(open("script.lace").read())
config = load_config(explicit_path="lace.config")

result = run_script(ast, script_vars={"key": "val"}, config=config)
```

## User-Agent

Per `lace-spec.md` §3.6, this executor sets a default `User-Agent` on
outgoing requests:

```
User-Agent: lace-probe/<version> (lacelang-python)
```

Precedence (highest first): per-request `headers: { "User-Agent": ... }` →
`lace.config [executor].user_agent` → the default above.

## Responsible use

This software is designed for monitoring endpoints you **own or have
explicit authorization to probe**. You are solely responsible for
ensuring your use complies with all applicable laws, terms of service,
and acceptable use policies. See `NOTICE` for the full statement.

## License

Apache License 2.0

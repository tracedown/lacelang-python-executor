"""Environment overrides — how config, env, and runtime interact.

Config resolution works in layers:

    1. A single lace.config file is loaded (from root or explicit path).
    2. The base sections ([executor], [result], [extensions]) are read.
    3. If env is set, the [lace.config.{env}] section is deep-merged
       on top of the base — only keys present in the env section are
       overridden; everything else is inherited from the base.
    4. After merging, env:VARNAME references in string values are
       resolved against os.environ.

The env parameter selects a SECTION WITHIN the config file — it does
NOT select a different file.  There is exactly one config file per
executor; the env overlay lives inside that file.

Examples of what each combination does:

    LaceExecutor("lace")
        Config: lace/lace.config (auto-discovered)
        Env: none (base sections only)

    LaceExecutor("lace", env="staging")
        Config: lace/lace.config (same file)
        Env: [lace.config.staging] merged on top of base

    LaceExecutor(config="/abs/path/lace.config", env="prod")
        Config: /abs/path/lace.config (explicit, root is None)
        Env: [lace.config.prod] merged on top of base

    LaceExecutor("lace", config="/other/lace.config")
        Config: /other/lace.config (config= overrides root discovery)
        Root: lace/ (still used for script name resolution)
        Env: none

    LACE_ENV=staging  LaceExecutor("lace")
        Config: lace/lace.config
        Env: [lace.config.staging] (from LACE_ENV env var)

    LACE_ENV=staging  LaceExecutor("lace", env="prod")
        Config: lace/lace.config
        Env: [lace.config.prod] (env= kwarg wins over LACE_ENV)
"""

import json
from lacelang_executor import LaceExecutor

# ── Base config (no env) ────────────────────────────────────────

executor_base = LaceExecutor("lace")
print("Base config:")
print(f"  maxTimeoutMs = {executor_base.config['executor']['maxTimeoutMs']}")
print(f"  user_agent   = {executor_base.config['executor'].get('user_agent')}")
print()

# ── Staging overlay ─────────────────────────────────────────────

executor_staging = LaceExecutor("lace", env="staging")
print("Staging config (env='staging'):")
print(f"  maxTimeoutMs = {executor_staging.config['executor']['maxTimeoutMs']}")
print(f"  maxRedirects = {executor_staging.config['executor']['maxRedirects']}")
print(f"  user_agent   = {executor_staging.config['executor'].get('user_agent')}")
print("  ^ maxTimeoutMs overridden (60000 vs 300000)")
print("  ^ maxRedirects inherited from base (10)")
print("  ^ user_agent inherited from base (None)")
print()

# ── Production overlay ──────────────────────────────────────────

executor_prod = LaceExecutor("lace", env="production")
print("Production config (env='production'):")
print(f"  maxTimeoutMs = {executor_prod.config['executor']['maxTimeoutMs']}")
print(f"  user_agent   = {executor_prod.config['executor'].get('user_agent')}")
print("  ^ maxTimeoutMs inherited from base (300000)")
print("  ^ user_agent overridden")
print()

# ── Explicit config path + root for scripts ─────────────────────

executor_split = LaceExecutor("lace", config="lace/lace.config", env="staging")
print("Explicit config + root:")
print(f"  root   = {executor_split.root}")
print(f"  config = {executor_split.config['_meta']['source_path']}")
print("  ^ root is used for script name resolution")
print("  ^ config= overrides auto-discovery from root")

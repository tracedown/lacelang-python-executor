"""Extension registration — built-in and third-party."""

from lacelang_executor import LaceExecutor

executor = LaceExecutor("lace")

# ── Built-in extensions ─────────────────────────────────────────
# Declared in lace.config under [executor].extensions, or passed
# at construction time.  These ship with the executor package.

executor_with_builtins = LaceExecutor(
    "lace",
    extensions=["laceNotifications"],
)

# ── Third-party extensions ──────────────────────────────────────
# Registered explicitly via executor.extension().  Each call takes
# a path to a .laceext manifest (or a directory containing one)
# and an optional config path.

# Directory-based: finds myext.laceext and myext.config inside
# executor.extension("lace/extensions/myext")

# Explicit paths:
# executor.extension("path/to/custom.laceext", "path/to/custom.config")

# Extension config is merged into the executor's [extensions] block
# so the extension's rules can access it via the `config` base in
# the DSL.  Equivalent to adding [extensions.myext] in lace.config.

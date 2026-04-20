"""Basic library usage — single probe, auto-prev tracking."""

from lacelang_executor import LaceExecutor

# Point to the lace/ directory.  Config is loaded once from
# lace/lace.config; scripts resolve by name from lace/scripts/.
executor = LaceExecutor("lace")

# Prepare the "health" probe.
# Resolves to lace/scripts/health/health.lace, parses and validates
# the AST once.  The probe is bound to this executor and reuses the
# same config and extension set.
probe = executor.probe("health")

# First run — no prev result yet.
result = probe.run(vars={"base_url": "https://httpbin.org"})
print(f"Run 1: {result['outcome']}  (calls: {len(result['calls'])})")

# Second run — prev is automatically injected from the first run.
# The script can access prev values via $$prev.* syntax.
result = probe.run(vars={"base_url": "https://httpbin.org"})
print(f"Run 2: {result['outcome']}  (prev was injected: {probe.prev is not None})")

# Override prev explicitly when needed.
result = probe.run(
    vars={"base_url": "https://httpbin.org"},
    prev={"outcome": "success", "calls": []},
)
print(f"Run 3: {result['outcome']}  (explicit prev)")

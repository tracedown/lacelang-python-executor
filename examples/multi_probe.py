"""Multiple probes on one executor — each tracks its own prev."""

import json
from lacelang_executor import LaceExecutor

executor = LaceExecutor("lace")

# Each probe has its own AST (parsed once) and its own prev chain.
health = executor.probe("health")
auth = executor.probe("auth-flow")

# Run health probe twice — prev chains automatically.
r1 = health.run(vars="lace/scripts/health/vars.json")
r2 = health.run(vars="lace/scripts/health/vars.json")
print(f"health run 1: {r1['outcome']}")
print(f"health run 2: {r2['outcome']}  (prev injected)")

# Auth probe has its own independent prev.
r3 = auth.run(vars="lace/scripts/auth-flow/vars.json")
print(f"auth run 1:   {r3['outcome']}  (no prev — independent)")

# Verify prev isolation: health's prev is from r2, not r3.
print(f"health.prev == r2: {health.prev is r2}")
print(f"auth.prev == r3:   {auth.prev is r3}")

# Reset prev manually.
health.prev = None
r4 = health.run(vars="lace/scripts/health/vars.json")
print(f"health run 3: {r4['outcome']}  (prev reset to None)")

# Disable auto-tracking entirely.
executor_no_prev = LaceExecutor("lace", track_prev=False)
probe = executor_no_prev.probe("health")
probe.run(vars="lace/scripts/health/vars.json")
print(f"no-track prev: {probe.prev}")  # None

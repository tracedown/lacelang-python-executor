"""Generic `.laceext` processor.

Implements lace-extensions.md:
    - TOML file loading + schema/result/functions/rules extraction
    - Rule-body DSL lexer, parser, and tree-walking interpreter
    - Hook dispatch at `on [before] call | expect | check | assert | store`
    - Tag-constructor function registration from `[types]` sections
    - Primitives: compare, map_get, map_match, is_null, type_of

No native per-extension logic — all extension behaviour comes from the
.laceext file itself.
"""

from lacelang_executor.laceext.loader import Extension, load_extension  # noqa: F401
from lacelang_executor.laceext.registry import ExtensionRegistry  # noqa: F401

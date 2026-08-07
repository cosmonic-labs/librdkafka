#!/usr/bin/env python3
"""Generate stubs for the cosmonic:kafka exports this plugin does not implement.

Which symbols the guest must provide is decided by the linker, not by guessing
at name suffixes: compile the generated bindings and take the symbols they leave
undefined. An earlier version filtered on `_free`/`_return` and duplicated the
stream/future `drop_readable` helpers that the bindings do define.

    python3 tools/gen-stubs.py <needed-symbols-file>

where the symbol file is `llvm-nm host_plugin.o | grep ' U exports_'`.
"""
import re
import sys
from pathlib import Path

HDR = Path("src/host_plugin.h")
OUT = Path("src/stubs.c")

# Implemented for real in plugin.c.
HAND = {
    "exports_wasi_cli_run_run",
    "exports_wasi_cli_run_run_callback",
    "exports_wasmcloud_host_workload_lifecycle_on_workload_bind",
    "exports_wasmcloud_host_workload_lifecycle_on_workload_bind_callback",
    "exports_wasmcloud_host_workload_lifecycle_on_workload_unbind",
    "exports_wasmcloud_host_workload_lifecycle_on_workload_unbind_callback",
}

HEADER = '''/*
 * Generated stubs for the parts of cosmonic:kafka this plugin does not
 * implement yet.
 *
 * Every stub reports failure rather than returning a plausible empty result.
 * A caller that receives an empty topic list cannot tell "no topics" from "not
 * built yet"; a failure can only mean one thing.
 *
 * Regenerate with tools/gen-stubs.py. Do not edit by hand.
 */
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include "host_plugin.h"
'''


def main() -> int:
    needed = {l.strip() for l in open(sys.argv[1]) if l.strip()} - HAND
    # Names containing a double underscore are canonical-ABI stream/future
    # intrinsics. They show up undefined but are declared __import_module__ —
    # the *host* provides them at component link. Defining them here is what
    # caused "duplicate symbol" the first time round.
    needed = {n for n in needed if "__" not in n}
    hdr = HDR.read_text()

    # Any return type: several exports return generated enum/stream types.
    decls = re.findall(
        r"^([A-Za-z_][A-Za-z_0-9]*\s+\*?\s*(exports_[a-z_0-9]+)\s*\(([^;]*)\));\s*$",
        hdr,
        re.M,
    )

    parts = [HEADER]
    emitted = set()

    for full, name, args in decls:
        if name not in needed or name in emitted:
            continue
        emitted.add(name)

        ret = full.split()[0]
        body = [f"{full} {{"]
        for arg in (a.strip() for a in args.split(",")):
            if not arg or arg == "void":
                continue
            pname = arg.split()[-1].lstrip("*")
            if pname.isidentifier():
                body.append(f"    (void){pname};")
        if ret == "host_plugin_callback_code_t":
            body.append("    return HOST_PLUGIN_CALLBACK_CODE_EXIT;")
        elif ret == "bool":
            body.append("    return false;")
        elif ret != "void":
            body.append("    return 0;")
        body.append("}")
        parts.append("\n".join(body))

    OUT.write_text("\n\n".join(parts) + "\n")

    missing = needed - emitted
    print(f"generated {len(emitted)} stubs")
    if missing:
        print(f"WARNING: {len(missing)} required symbols had no declaration:")
        for m in sorted(missing)[:10]:
            print(f"  {m}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

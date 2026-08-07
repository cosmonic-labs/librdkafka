#!/usr/bin/env python3
"""Check that `error-code` in wit/types.wit still matches rd_kafka_resp_err_t.

The enum is generated from the C header. Nothing enforces that afterwards, so
without this check a librdkafka bump that adds error codes leaves the interface
quietly unable to represent them — callers get `unknown-error-code` for a
condition the client knew perfectly well how to describe.

Run from the repo root:

    python3 wit/check-error-codes.py
"""

import re
import sys
from pathlib import Path

HEADER = Path("src/rdkafka.h")
WIT = Path("wit/types.wit")

# Range sentinels, not real error codes.
SENTINELS = {
    "RD_KAFKA_RESP_ERR__BEGIN",
    "RD_KAFKA_RESP_ERR__END",
    "RD_KAFKA_RESP_ERR_END_ALL",
}

# Present in the WIT with no C counterpart, so a newer broker's code has
# somewhere to land instead of the mapping failing.
WIT_ONLY = {"unknown-error-code"}


def kebab(name: str) -> str:
    return (
        name.replace("RD_KAFKA_RESP_ERR__", "")
        .replace("RD_KAFKA_RESP_ERR_", "")
        .lower()
        .replace("_", "-")
    )


def header_codes() -> set[str]:
    src = HEADER.read_text()
    m = re.search(r"typedef enum \{(.*?)\} rd_kafka_resp_err_t;", src, re.S)
    if not m:
        sys.exit(f"error: could not find rd_kafka_resp_err_t in {HEADER}")
    names = re.findall(r"^\s*(RD_KAFKA_RESP_ERR_[A-Z_0-9]+)\s*=", m.group(1), re.M)
    return {kebab(n) for n in names if n not in SENTINELS}


def wit_codes() -> set[str]:
    text = WIT.read_text()
    m = re.search(r"enum error-code \{(.*?)\n    \}", text, re.S)
    if not m:
        sys.exit(f"error: could not find `enum error-code` in {WIT}")
    return set(re.findall(r"^\s{8}([a-z][a-z0-9-]*),$", m.group(1), re.M))


def main() -> int:
    hdr = header_codes()
    wit = wit_codes()

    missing = sorted(hdr - wit)
    extra = sorted(wit - hdr - WIT_ONLY)

    print(f"librdkafka: {len(hdr)} codes")
    print(f"WIT:        {len(wit)} cases ({len(WIT_ONLY)} intentionally WIT-only)")

    if missing:
        print(f"\n::error::{len(missing)} error code(s) in librdkafka are missing "
              f"from {WIT}:")
        for name in missing:
            print(f"  - {name}")
        print("\nRegenerate the enum from the header rather than adding them by "
              "hand; transcribing ~190 cases is how they get dropped.")

    if extra:
        print(f"\n::error::{len(extra)} case(s) in {WIT} do not exist in "
              f"librdkafka:")
        for name in extra:
            print(f"  - {name}")

    if missing or extra:
        return 1

    print("\nOK: error-code matches rd_kafka_resp_err_t")
    return 0


if __name__ == "__main__":
    sys.exit(main())

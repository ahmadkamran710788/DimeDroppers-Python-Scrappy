#!/usr/bin/env python
"""Convenience launcher so you don't have to remember the ``scrapy crawl`` flags.

    python run.py                      # all 50 states + DC (large!)
    python run.py wy                   # one state
    python run.py wy,vt,ri             # a few states
    python run.py wy --no-schedules    # schools + sports only (fast)
    python run.py all --levels all     # include JV / Freshman schedules too

Anything after the state list is forwarded to ``scrapy crawl`` as-is, e.g.
    python run.py wy -s JOBDIR=.jobs/wy        # make it resumable
"""
import sys

from scrapy.cmdline import execute


def main():
    argv = sys.argv[1:]
    states = "all"
    passthrough = []
    for arg in argv:
        if arg == "--no-schedules":
            passthrough += ["-a", "schedules=0"]
        elif arg == "--no-discover":
            passthrough += ["-a", "discover=0"]
        elif arg.startswith("--levels"):
            # support "--levels all" handled below via next arg, or "--levels=all"
            if "=" in arg:
                passthrough += ["-a", f"levels={arg.split('=', 1)[1]}"]
            else:
                passthrough.append("__LEVELS__")
        elif passthrough and passthrough[-1] == "__LEVELS__":
            passthrough[-1] = "-a"
            passthrough += [f"levels={arg}"]
        elif arg.startswith("-"):
            passthrough.append(arg)
        elif states == "all" and not passthrough:
            states = arg
        else:
            passthrough.append(arg)

    execute(["scrapy", "crawl", "maxpreps", "-a", f"states={states}", *passthrough])


if __name__ == "__main__":
    main()

"""Sample only the validation command and its descendants; never attach to a service."""
import json
from pathlib import Path
import subprocess
import sys
import time

import psutil

directory = Path(sys.argv[1])
peaks = {"sum_rss_bytes": 0, "jvm_rss_bytes": 0, "fds": 0, "processes": 0}
started = time.monotonic()
with (directory / "jvm.log").open("w") as log:
    proc = subprocess.Popen(sys.argv[2:], stdout=log, stderr=subprocess.STDOUT)
    root = psutil.Process(proc.pid)
    owned = []
    try:
        while proc.poll() is None:
            if time.monotonic() - started > 900:
                raise TimeoutError("Validation exceeded 15 minutes")
            owned = root.children(recursive=True)
            rss, fds, alive = 0, 0, 0
            for child in [root] + owned:
                try:
                    size = child.memory_info().rss
                    rss += size
                    fds += child.num_fds()
                    alive += 1
                    if child.pid == root.pid:
                        peaks["jvm_rss_bytes"] = max(peaks["jvm_rss_bytes"], size)
                except psutil.Error:
                    pass
            for key, value in (("sum_rss_bytes", rss), ("fds", fds), ("processes", alive)):
                peaks[key] = max(peaks[key], value)
            time.sleep(0.2)
    finally:
        if proc.poll() is None:
            # Terminate only processes observed in this command's own tree.
            for child in reversed(owned):
                try:
                    child.terminate()
                except psutil.Error:
                    pass
            proc.terminate()
            try:
                proc.wait(10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
peaks.update(exit_code=proc.returncode, elapsed_seconds=time.monotonic() - started,
             note="Sampled every 200ms. Sum of RSS double-counts shared pages; not PSS.")
(directory / "resource-peaks.json").write_text(json.dumps(peaks, indent=2))
sys.exit(proc.returncode)

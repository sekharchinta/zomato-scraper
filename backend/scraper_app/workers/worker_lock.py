"""
Worker Singleton Lock & Instance Detector:
Ensures only a single instance of the RQ Worker Supervisor can run at any time.
If another terminal tries to launch workers while an instance is already running,
it detects the active process, identifies its TTY/terminal and PID, and cleanly
refuses to launch duplicate workers.
"""

import atexit
import fcntl
import json
import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from django.conf import settings
from config import SINGLE_QUEUE_NAME

logger = logging.getLogger("ZomatoWorkerLock")

LOCK_FILE_NAME = ".worker_manager.lock"


def get_current_tty() -> str:
    """Returns the current terminal/TTY name (e.g. 'pts/2' or '/dev/pts/2')."""
    for fd in (sys.stdin, sys.stdout, sys.stderr):
        try:
            if hasattr(fd, "fileno") and os.isatty(fd.fileno()):
                raw_tty = os.ttyname(fd.fileno())
                return raw_tty.replace("/dev/", "")
        except Exception:
            pass
    return "unknown"


def get_process_info(pid: int) -> Optional[Dict[str, Any]]:
    """
    Queries OS for detailed process metadata given a PID:
    Returns dict with pid, ppid, tty, tty_display, user, etime, and command.
    """
    if not pid or pid <= 0:
        return None
    try:
        out = subprocess.check_output(
            ["ps", "-p", str(pid), "-o", "pid=,ppid=,tty=,user=,etime=,cmd=", "--no-headers"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        parts = out.split(None, 5)
        if len(parts) >= 6:
            raw_tty = parts[2].strip()
            tty_display = f"{raw_tty} (/dev/{raw_tty})" if raw_tty and raw_tty != "?" else "unknown"
            return {
                "pid": int(parts[0]),
                "ppid": int(parts[1]),
                "tty": raw_tty,
                "tty_display": tty_display,
                "user": parts[3].strip(),
                "etime": parts[4].strip(),
                "cmd": parts[5].strip(),
            }
    except Exception:
        pass
    return None


def find_running_worker_supervisor(exclude_pid: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """
    Scans OS processes to detect if an active worker supervisor is running.
    Excludes the calling process (exclude_pid) and identifies the top supervisor
    process (the one whose parent is a user shell/init and not another worker).
    """
    my_pid = exclude_pid or os.getpid()

    try:
        out = subprocess.check_output(
            ["pgrep", "-f", "manage.py run_workers"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        pids = [int(p) for p in out.splitlines() if p.strip()]
    except Exception:
        pids = []

    # Exclude our own PID and any children we might have spawned
    filtered_pids = [p for p in pids if p != my_pid]
    if not filtered_pids:
        return None

    # Identify the supervisor PID: the PID whose PPID is NOT in the list of run_workers pids
    for p in filtered_pids:
        info = get_process_info(p)
        if info and info["ppid"] not in filtered_pids:
            info["child_workers_count"] = len([cp for cp in filtered_pids if cp != p])
            return info

    # Fallback to the lowest PID in the list
    if filtered_pids:
        info = get_process_info(filtered_pids[0])
        if info:
            info["child_workers_count"] = len(filtered_pids) - 1
            return info

    return None


class WorkerSingletonLock:
    """
    Advisory process lock preventing duplicate worker supervisors from running.
    Uses kernel-level fcntl.flock on .worker_manager.lock and cross-verifies with OS process state.
    """

    def __init__(self, queue_name: Optional[str] = None, num_workers: int = 3) -> None:
        self.queue_name = queue_name or getattr(settings, "SINGLE_QUEUE_NAME", SINGLE_QUEUE_NAME)
        self.num_workers = num_workers
        base_dir = getattr(settings, "BASE_DIR", Path.cwd())
        self.lock_path = Path(base_dir) / LOCK_FILE_NAME
        self._fd: Optional[int] = None
        self._file_obj: Optional[Any] = None
        self.is_locked = False

    def acquire(self) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Attempts to acquire the singleton worker supervisor lock.
        Returns (True, None) if lock acquired.
        Returns (False, running_process_info) if another worker supervisor is already active.
        """
        my_pid = os.getpid()

        # 1. First safety check: Scan OS processes for any existing supervisor
        existing_info = find_running_worker_supervisor(exclude_pid=my_pid)
        if existing_info:
            return False, existing_info

        # 2. Kernel-level advisory lock using fcntl.flock
        try:
            self._file_obj = open(self.lock_path, "a+", encoding="utf-8")
            self._fd = self._file_obj.fileno()

            # Request non-blocking exclusive lock
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

            # Lock acquired! Record our metadata into the lockfile
            self._file_obj.seek(0)
            self._file_obj.truncate()

            current_tty = get_current_tty()
            metadata = {
                "pid": my_pid,
                "tty": current_tty,
                "tty_display": f"{current_tty} (/dev/{current_tty})" if current_tty != "unknown" else "unknown",
                "user": os.environ.get("USER", os.environ.get("USERNAME", "unknown")),
                "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "cmd": " ".join(sys.argv),
                "queue": self.queue_name,
                "num_workers": self.num_workers,
            }
            json.dump(metadata, self._file_obj, indent=2)
            self._file_obj.flush()

            self.is_locked = True
            atexit.register(self.release)
            return True, None

        except (BlockingIOError, IOError):
            # Another process currently holds the lockfile
            holder_info = self._read_lock_metadata()
            if not holder_info:
                holder_info = existing_info or {"pid": "Unknown", "tty": "Unknown", "user": "Unknown"}
            return False, holder_info
        except Exception as exc:
            logger.warning("Could not set file lock: %s", exc)
            return True, None

    def _read_lock_metadata(self) -> Optional[Dict[str, Any]]:
        """Reads metadata from the lockfile if available, augmented with live ps stats."""
        try:
            if self.lock_path.exists():
                with open(self.lock_path, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
                    pid = data.get("pid")
                    if pid:
                        live_info = get_process_info(int(pid))
                        if live_info:
                            data.update(live_info)
                    return data
        except Exception:
            pass
        return None

    def release(self) -> None:
        """Releases the file lock and cleans up the lockfile."""
        if not self.is_locked:
            return
        try:
            if self._fd is not None:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
            if self._file_obj is not None and not self._file_obj.closed:
                self._file_obj.close()
            if self.lock_path.exists():
                # Verify we own the lockfile before unlinking
                try:
                    with open(self.lock_path, "r", encoding="utf-8") as fh:
                        data = json.load(fh)
                        if data.get("pid") == os.getpid():
                            self.lock_path.unlink(missing_ok=True)
                except Exception:
                    self.lock_path.unlink(missing_ok=True)
        except Exception:
            pass
        finally:
            self.is_locked = False
            self._fd = None
            self._file_obj = None


def format_already_running_banner(info: Dict[str, Any], queue_name: str = "zomato") -> str:
    """Formats a crystal-clear, high-visibility terminal banner showing where workers are running."""
    pid = info.get("pid", "Unknown")
    tty = info.get("tty_display") or info.get("tty") or "Unknown"
    user = info.get("user", "Unknown")
    etime = info.get("etime") or info.get("started_at", "Unknown")
    cmd = info.get("cmd", "python manage.py run_workers")
    workers_cnt = info.get("child_workers_count", info.get("num_workers", 3))

    banner = f"""
\033[1;31m========================================================================================\033[0m
\033[1;31m [!] CANNOT START WORKERS: AN ACTIVE INSTANCE IS ALREADY RUNNING IN ANOTHER TERMINAL\033[0m
\033[1;31m========================================================================================\033[0m

  \033[1;33m• Active Supervisor PID :\033[0m  \033[1;37m{pid}\033[0m
  \033[1;33m• Running on Terminal   :\033[0m  \033[1;32m{tty}\033[0m
  \033[1;33m• Running User          :\033[0m  \033[1;37m{user}\033[0m
  \033[1;33m• Active Runtime        :\033[0m  \033[1;37m{etime}\033[0m
  \033[1;33m• Worker Processes      :\033[0m  \033[1;37m{workers_cnt} concurrent worker(s) active\033[0m
  \033[1;33m• Monitored Queue       :\033[0m  \033[1;36m{queue_name}\033[0m (Single Redis Queue on DB 1)
  \033[1;33m• Command Line          :\033[0m  \033[0;37m{cmd}\033[0m

\033[1;34m----------------------------------------------------------------------------------------\033[0m
\033[1;37mWHY THIS IS BLOCKED:\033[0m
  To prevent duplicate jobs and race conditions, the architecture strictly enforces
  that only \033[1;37mONE\033[0m worker manager instance may run at any time.

\033[1;37mHOW TO PROCEED:\033[0m
  1. \033[1;32mSwitch to the terminal running on '{info.get('tty', 'pts')}'\033[0m where the workers are already active.
  2. If you wish to stop the existing workers and run them here instead, terminate them:
     \033[1;33mkill -TERM {pid}\033[0m
     Then re-run \033[1;32m./commands/run_workers.sh\033[0m in this terminal.
\033[1;31m========================================================================================\033[0m
"""
    return banner.strip()

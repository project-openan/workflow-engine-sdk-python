# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Cross-process ownership of one on-disk execution spool."""

import errno
import os
from pathlib import Path


class SpoolInUseError(OSError):
    """Another reporter already owns the spool namespace."""


class SpoolLease:
    def __init__(self, namespace: Path):
        # Keep the lock file in place: unlinking it would allow a second process to lock
        # a different file while the first owner still has its descriptor open.
        lock_file = (namespace / "owner.lock").open("a+b")
        try:
            lock_file.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            lock_file.close()
            if error.errno in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}:
                raise SpoolInUseError("Execution spool namespace is already owned") from error
            raise
        self._file = lock_file

    def close(self) -> None:
        self._file.close()

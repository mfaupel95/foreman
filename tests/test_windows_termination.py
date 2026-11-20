import asyncio
import os
import subprocess
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from foreman.workers.hermes import HermesWorker


@pytest.mark.skipif(os.name != "nt", reason="Windows taskkill path")
@pytest.mark.asyncio
@pytest.mark.parametrize("returncode", [1, 0])
async def test_unconfirmed_windows_termination_is_bounded(returncode):
    worker = HermesWorker(probe_capabilities=False, graceful_termination_seconds=0.05)
    never = asyncio.Event()
    worker.process = SimpleNamespace(pid=123, returncode=None, wait=AsyncMock(side_effect=never.wait))
    with patch("subprocess.run", return_value=subprocess.CompletedProcess(["taskkill"], returncode)):
        with pytest.raises(RuntimeError, match="Hermes process termination"):
            await asyncio.wait_for(worker.terminate("worker_timeout"), timeout=0.5)


@pytest.mark.skipif(os.name != "nt", reason="Windows taskkill path")
@pytest.mark.asyncio
async def test_run_does_not_wait_for_open_pipes_after_failed_taskkill(tmp_path):
    from test_hermes import make_emit, make_record

    worker = HermesWorker(probe_capabilities=False, graceful_termination_seconds=0.05)
    never = asyncio.Event()
    process = SimpleNamespace(
        pid=123, returncode=None, wait=AsyncMock(side_effect=never.wait),
        stdout=asyncio.StreamReader(), stderr=asyncio.StreamReader(),
    )
    with (
        patch("asyncio.create_subprocess_exec", return_value=process),
        patch("subprocess.run", return_value=subprocess.CompletedProcess(["taskkill"], 1)),
        pytest.raises(RuntimeError, match="Hermes process termination"),
    ):
        await asyncio.wait_for(worker.run(make_record(), tmp_path, make_emit([]), 0.05), 0.5)


@pytest.mark.skipif(os.name != "nt", reason="Windows taskkill path")
@pytest.mark.asyncio
async def test_cancelled_run_keeps_cancellation_when_taskkill_fails(tmp_path):
    from test_hermes import make_emit, make_record

    worker = HermesWorker(probe_capabilities=False)
    waiting = asyncio.Event()

    async def wait():
        waiting.set()
        await asyncio.Event().wait()

    process = SimpleNamespace(
        pid=123, returncode=None, wait=wait,
        stdout=asyncio.StreamReader(), stderr=asyncio.StreamReader(),
    )
    record = make_record()
    with (
        patch("asyncio.create_subprocess_exec", return_value=process),
        patch("subprocess.run", return_value=subprocess.CompletedProcess(["taskkill"], 1)),
    ):
        task = asyncio.create_task(worker.run(record, tmp_path, make_emit([]), 30))
        await asyncio.wait_for(waiting.wait(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
    assert "taskkill exited 1" in record.stderr
    assert record.finished_at is not None

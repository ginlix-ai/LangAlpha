"""Contract tests for full end-to-end sandbox lifecycle."""

from __future__ import annotations

import shlex

import pytest

from ptc_agent.core.sandbox.runtime import RuntimeState

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class TestFullLifecycle:
    """End-to-end lifecycle: create -> use -> stop -> reconnect -> use -> delete.

    Uses sandbox_provider so it runs against whichever provider is configured.
    """

    async def test_complete_lifecycle(self, sandbox_provider, timed):
        # 1. Create
        async with timed("full_lifecycle", "create"):
            runtime = await sandbox_provider.create(
                env_vars={"APP_ENV": "test", "API_KEY": "secret123"}
            )
        try:
            assert await runtime.get_state() == RuntimeState.RUNNING
            wd = await runtime.fetch_working_dir()

            # 2. Setup workspace structure
            async with timed("full_lifecycle", "setup_workspace"):
                await runtime.exec(f"mkdir -p {wd}/sandbox_tools {wd}/data {wd}/results")

            # 3. Upload tool files
            tool_code = b"def get_price(symbol): return 42.0\n"
            async with timed("full_lifecycle", "upload_tools"):
                await runtime.upload_file(tool_code, f"{wd}/sandbox_tools/market.py")
                await runtime.upload_file(b"", f"{wd}/sandbox_tools/__init__.py")

            # 4. Execute code using the tool
            async with timed("full_lifecycle", "python_with_tool"):
                code = (
                    "from sandbox_tools.market import get_price; "
                    "print(f'AAPL: ${get_price(\"AAPL\")}')"
                )
                result = await runtime.exec(
                    f"python3 -c {shlex.quote(code)}", env={"PYTHONPATH": wd}
                )
            assert result.exit_code == 0
            assert "AAPL: $42.0" in result.stdout

            # 5. Bash command with env var
            async with timed("full_lifecycle", "exec_env_var"):
                bash_result = await runtime.exec("echo API_KEY=$API_KEY")
            assert "secret123" in bash_result.stdout

            # 6. Upload and process data
            async with timed("full_lifecycle", "upload_and_process"):
                await runtime.upload_file(
                    b"col1,col2\n1,2\n3,4\n", f"{wd}/data/input.csv"
                )
                code = (
                    f"import csv\n"
                    f"with open('{wd}/data/input.csv') as f:\n"
                    f"    rows = list(csv.DictReader(f))\n"
                    f"print(f'Rows: {{len(rows)}}')\n"
                )
                result = await runtime.exec(f"python3 -c {shlex.quote(code)}")
            assert result.exit_code == 0
            assert "Rows: 2" in result.stdout

            # 7. Generate output
            async with timed("full_lifecycle", "generate_output"):
                code = (
                    f"with open('{wd}/results/output.txt', 'w') as f: "
                    f"f.write('Analysis complete\\n')"
                )
                await runtime.exec(f"python3 -c {shlex.quote(code)}")
            output = await runtime.download_file(f"{wd}/results/output.txt")
            assert b"Analysis complete" in output

            # 8. Stop (with retry — Daytona stop can timeout under load)
            async with timed("full_lifecycle", "stop"):
                try:
                    await runtime.stop(timeout=180)
                except Exception:
                    import asyncio
                    await asyncio.sleep(5)
                    await runtime.stop(timeout=180)
            assert await runtime.get_state() == RuntimeState.STOPPED

            # 9. Reconnect via provider.get
            async with timed("full_lifecycle", "reconnect"):
                reconnected = await sandbox_provider.get(runtime.id)
                await reconnected.start()
            assert await reconnected.get_state() == RuntimeState.RUNNING

            # 10. Verify data survived
            async with timed("full_lifecycle", "verify_persistence"):
                output = await reconnected.download_file(f"{wd}/results/output.txt")
            assert b"Analysis complete" in output

            # 11. More work after reconnect
            async with timed("full_lifecycle", "post_reconnect_exec"):
                result = await reconnected.exec(f"cat {wd}/data/input.csv | wc -l")
            assert "3" in result.stdout

        finally:
            # 12. ALWAYS delete -- prevents resource leaks on real providers
            try:
                await runtime.delete()
            except Exception:
                pass

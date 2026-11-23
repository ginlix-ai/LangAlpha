"""The settled contracts of the setup step: what it may hold, who wins, and
that a removal takes away only what it set."""

import asyncio
import json
import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from ptc_agent.config import core
from ptc_agent.config.core import KindsInReach, kind_availability
from ptc_agent.config.utils import create_sandbox_config
from src.server.app import deployment_setup, web_dist
from src.server.services.setup_settings import apply, environment, step
from src.server.services.setup_settings.catalog import key_names, key_specs

MANAGED = (
    *key_names(),
    "SANDBOX_PROVIDER",
    "LOCAL_INSTALL_GROUPS",
    "DAYTONA_SECRET_NAMESPACE",
    "E2B_SECRET_NAMESPACE",
)


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch):
    for name in (*MANAGED, "LANGALPHA_DOCKER_COMPUTERS", "LANGALPHA_LOCAL_COMPUTER", "LANGALPHA_OWNER_SECRET"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(environment, "_at_start", None)
    yield
    # Not delenv: it would record what the test exported for undo to put back.
    for name in MANAGED:
        os.environ.pop(name, None)


def _daemon(up: bool):
    return patch.object(core, "docker_reachable", AsyncMock(return_value=up))


class TestAllowlist:
    def test_holds_every_manifest_key_and_the_named_vendors(self):
        manifest = Path("src/tools/manifest/web_providers.json")
        web = {
            spec["env_key"]
            for spec in json.loads(manifest.read_text())["providers"].values()
            if spec.get("env_key")
        }
        assert key_names() == web | {"FMP_API_KEY", "DAYTONA_API_KEY", "E2B_API_KEY"}

    def test_a_fetch_only_provider_is_not_listed_as_search(self):
        groups = {spec.provider: spec.group for spec in key_specs()}
        assert groups["tavily"] == "search"
        assert groups["firecrawl"] == "fetch"
        assert groups["fmp"] == "data"
        assert groups["e2b"] == "sandbox"


class TestEnvironmentPrecedence:
    def test_a_value_set_outside_the_app_wins(self, monkeypatch):
        monkeypatch.setenv("TAVILY_API_KEY", "from-env")
        environment.export({"TAVILY_API_KEY": "saved"})
        assert environment.set_outside_app("TAVILY_API_KEY")
        assert environment.os.environ["TAVILY_API_KEY"] == "from-env"

    def test_an_empty_value_is_unset(self, monkeypatch):
        # Every name has this shape in a .env copied from the example.
        monkeypatch.setenv("TAVILY_API_KEY", "")
        assert not environment.set_outside_app("TAVILY_API_KEY")
        environment.export({"TAVILY_API_KEY": "saved"})
        assert environment.os.environ["TAVILY_API_KEY"] == "saved"
        assert not environment.set_outside_app("TAVILY_API_KEY")

    def test_what_the_operator_set_is_read_once(self, monkeypatch):
        environment.take_snapshot()
        monkeypatch.setenv("TAVILY_API_KEY", "written-later")
        assert not environment.set_outside_app("TAVILY_API_KEY")

    @pytest.mark.asyncio
    async def test_writing_a_name_set_outside_the_app_is_refused(self, monkeypatch):
        monkeypatch.setenv("TAVILY_API_KEY", "from-env")
        for value in ("new", None):
            with pytest.raises(step.SetupRefused, match="read-only"):
                await step.save_keys({"TAVILY_API_KEY": value})

    @pytest.mark.asyncio
    async def test_a_name_outside_the_allowlist_is_refused(self):
        for name in ("OPENAI_API_KEY", "SANDBOX_PROVIDER", "SETUP_COMPLETED"):
            with pytest.raises(step.SetupRefused, match="not a key"):
                await step.save_keys({name: "x"})


class TestHotApply:
    def test_removal_takes_away_only_what_this_module_set(self, monkeypatch):
        monkeypatch.setenv("SERPER_API_KEY", "from-env")
        rows = {"TAVILY_API_KEY": "saved", "SERPER_API_KEY": "shadowed"}
        environment.export(rows)
        assert environment.os.environ["TAVILY_API_KEY"] == "saved"

        changed = environment.export({})
        assert changed == {"TAVILY_API_KEY"}
        assert "TAVILY_API_KEY" not in environment.os.environ
        assert environment.os.environ["SERPER_API_KEY"] == "from-env"

    def test_applying_the_same_rows_changes_nothing(self):
        rows = {"TAVILY_API_KEY": "saved"}
        assert environment.export(rows) == {"TAVILY_API_KEY"}
        assert environment.export(rows) == set()

    def test_a_changed_value_replaces_the_old_one(self):
        environment.export({"FMP_API_KEY": "old"})
        assert environment.export({"FMP_API_KEY": "new"}) == {"FMP_API_KEY"}
        assert environment.os.environ["FMP_API_KEY"] == "new"

    def test_a_saved_empty_install_group_list_is_core_alone(self):
        environment.export({"LOCAL_INSTALL_GROUPS": ""})
        assert environment.os.environ["LOCAL_INSTALL_GROUPS"] == ""

    def test_a_cloud_choice_waits_for_its_key(self):
        """Config requires the key of the provider it runs on, so a choice
        exported without one is a boot that serves no computers."""
        environment.export({"SANDBOX_PROVIDER": "e2b"})
        assert "SANDBOX_PROVIDER" not in environment.os.environ

        environment.export({"SANDBOX_PROVIDER": "e2b", "E2B_API_KEY": "k"})
        assert environment.os.environ["SANDBOX_PROVIDER"] == "e2b"

        environment.export({"SANDBOX_PROVIDER": "e2b"})
        assert "SANDBOX_PROVIDER" not in environment.os.environ

    def test_a_local_choice_needs_the_gate(self):
        environment.export({"SANDBOX_PROVIDER": "local"})
        assert "SANDBOX_PROVIDER" not in environment.os.environ


@pytest.mark.asyncio
class TestNeedsSetup:
    @pytest.fixture(autouse=True)
    def no_daemon(self):
        with _daemon(False):
            yield

    async def _status(self, rows, *, computers=False):
        with (
            patch.object(step, "any_computer_exists", AsyncMock(return_value=computers)),
            patch.object(step, "_current_provider", return_value="docker"),
            patch.object(step, "_provider_source", return_value="detected"),
        ):
            return await step._status_from(rows)

    async def test_a_new_deployment_needs_it(self):
        assert (await self._status({}))["needs_setup"] is True

    async def test_finishing_it_ends_it(self):
        status = await self._status({"SETUP_COMPLETED": "1"})
        assert status["completed"] is True
        assert status["needs_setup"] is False

    async def test_a_deployment_with_a_computer_predates_it(self):
        assert (await self._status({}, computers=True))["needs_setup"] is False

    @pytest.mark.parametrize("name", ["SANDBOX_PROVIDER", "DAYTONA_API_KEY", "E2B_API_KEY"])
    async def test_a_sandbox_chosen_in_the_environment_predates_it(self, monkeypatch, name):
        monkeypatch.setenv(name, "docker")
        assert (await self._status({}))["needs_setup"] is False

    async def test_a_saved_key_is_masked_and_an_environment_one_is_not_shown(self, monkeypatch):
        monkeypatch.setenv("EXA_API_KEY", "exa-from-the-environment")
        status = await self._status({"TAVILY_API_KEY": "tvly-abcdefghijkl-mnop-a1b2"})
        keys = {k["name"]: k for k in status["keys"]}
        assert keys["TAVILY_API_KEY"]["state"] == "saved"
        assert keys["TAVILY_API_KEY"]["masked"] == "tvly...a1b2"
        assert keys["EXA_API_KEY"] == {
            "name": "EXA_API_KEY", "group": "search", "provider": "exa", "state": "environment",
        }
        assert keys["FMP_API_KEY"]["state"] == "unset"


@pytest.mark.asyncio
class TestStatusIsRead:
    """The status reads the table and writes nothing: not a row, not this
    worker's environment, not its config."""

    @pytest.fixture(autouse=True)
    def no_daemon(self):
        with _daemon(False):
            yield

    async def test_a_save_another_worker_took_shows_without_applying_it(self, monkeypatch):
        rows = {"SANDBOX_PROVIDER": "e2b", "E2B_API_KEY": "e2b-key-0123456789"}
        add = AsyncMock()
        with (
            patch.object(step, "read_deployment_settings", AsyncMock(return_value=rows)),
            patch.object(step, "any_computer_exists", AsyncMock(return_value=False)),
            patch.object(apply, "add_deployment_setting", add),
            patch.object(apply, "refresh", AsyncMock(side_effect=AssertionError("refreshed"))),
        ):
            status = await step.read_status()
        assert status["sandbox"]["provider"] == "e2b"
        assert status["sandbox"]["provider_source"] == "saved"
        e2b = next(o for o in status["sandbox"]["options"] if o["kind"] == "e2b")
        assert e2b["key_set"] is True
        add.assert_not_awaited()
        assert "SANDBOX_PROVIDER" not in environment.os.environ
        assert "E2B_API_KEY" not in environment.os.environ


class TestMasking:
    @pytest.mark.parametrize(
        ("value", "masked"),
        [
            ("x" * 20 + "abcd", "xxxx...abcd"),
            ("0123456789abcdef", "...ef"),
            ("x" * 21 + "ab", "...ab"),
            ("01234567", "...67"),
            ("0123456", "..."),
        ],
    )
    def test_the_shorter_the_key_the_less_of_it_shows(self, value, masked):
        assert step._masked(value) == masked


class TestDockerNotOffered:
    @pytest.fixture(autouse=True)
    def off(self, monkeypatch):
        monkeypatch.setenv("LANGALPHA_DOCKER_COMPUTERS", "0")

    @pytest.mark.asyncio
    async def test_the_card_says_why_without_asking_the_daemon(self):
        reachable = AsyncMock(return_value=True)
        with (
            patch.object(core, "docker_reachable", reachable),
            patch.object(step, "any_computer_exists", AsyncMock(return_value=False)),
        ):
            status = await step._status_from({})
        docker = next(o for o in status["sandbox"]["options"] if o["kind"] == "docker")
        assert docker["available"] is False
        assert docker["detail"] == "not_offered"
        reachable.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_saving_it_is_refused(self):
        with _daemon(True), pytest.raises(step.SetupRefused, match="not offered"):
            await step.save_sandbox("docker", None)

    def test_a_saved_docker_choice_does_not_reach_the_config(self):
        environment.export({"SANDBOX_PROVIDER": "docker"})
        assert "SANDBOX_PROVIDER" not in environment.os.environ

    def test_it_is_never_created_on_even_as_the_fallback(self):
        """With no key the provider falls back to docker, which the step then
        asks the owner to replace; no computer is made there meanwhile."""
        cfg = create_sandbox_config({"sandbox": {}})
        assert cfg.provider == "docker"
        assert cfg.creatable_providers() == ()

    @pytest.mark.asyncio
    async def test_a_new_computer_is_not_offered_it(self, monkeypatch):
        monkeypatch.setenv("E2B_API_KEY", "k")
        with _daemon(True):
            in_reach = KindsInReach("e2b", await kind_availability())
        cfg = create_sandbox_config(
            {"sandbox": {"offered_providers": ["docker", "e2b"]}}, in_reach=in_reach
        )
        assert cfg.creatable_providers() == ("e2b",)


class TestOfferedProviders:
    @pytest.fixture(autouse=True)
    def daemon_up(self):
        with _daemon(True):
            yield

    def _offered(self, chosen, *, declared=()):
        availability = asyncio.run(kind_availability())
        in_reach = None if chosen is None else KindsInReach(chosen, availability)
        cfg = create_sandbox_config(
            {"sandbox": {"offered_providers": list(declared)}}, in_reach=in_reach
        )
        return cfg.offered_providers

    def test_a_deployment_that_never_chose_keeps_its_list(self, monkeypatch):
        monkeypatch.setenv("E2B_API_KEY", "k")
        assert self._offered(None, declared=("daytona",)) == ("daytona",)
        assert self._offered(None) == ()

    def test_a_saved_choice_stands_beside_what_would_work(self, monkeypatch):
        monkeypatch.setenv("E2B_API_KEY", "k")
        monkeypatch.setenv("LANGALPHA_LOCAL_COMPUTER", "1")
        monkeypatch.setenv("LANGALPHA_OWNER_SECRET", "s3cret")
        assert self._offered("local") == ("local", "e2b", "docker")

    def test_local_is_offered_only_as_the_choice(self, monkeypatch):
        monkeypatch.setenv("LANGALPHA_LOCAL_COMPUTER", "1")
        monkeypatch.setenv("LANGALPHA_OWNER_SECRET", "s3cret")
        assert self._offered("docker") == ("docker",)


@pytest.mark.asyncio
class TestRefresh:
    """A refresh builds the config anew and swaps it in whole."""

    @pytest.fixture(autouse=True)
    def no_daemon(self):
        with _daemon(False):
            yield

    def _config(self):
        from ptc_agent.config.core import MCPConfig, SandboxConfig

        return MagicMock(
            sandbox=SandboxConfig(provider="docker"),
            mcp=MCPConfig(servers=[], tool_discovery_enabled=False),
            search_api="tavily",
        )

    async def test_a_saved_key_and_choice_reach_a_new_sandbox_config(self, monkeypatch):
        config = self._config()
        old_sandbox = config.sandbox
        rows = {"SANDBOX_PROVIDER": "e2b", "E2B_API_KEY": "e2b-key"}
        monkeypatch.setattr(apply, "_platform_secrets_declared", lambda: False)
        with (
            patch.object(apply, "read_deployment_settings", AsyncMock(return_value=rows)),
            patch.object(apply, "_current_config", return_value=config),
            patch(
                "ptc_agent.config.file_utils.load_agent_config",
                return_value={"sandbox": {}, "search_api": "tavily"},
            ),
        ):
            await apply.refresh()
        assert config.sandbox is not old_sandbox
        assert config.sandbox.provider == "e2b"
        assert config.sandbox.e2b.api_key == "e2b-key"
        assert config.sandbox.offered_providers == ("e2b",)

    async def test_a_refresh_that_failed_partway_is_finished_by_the_next(self):
        rebuild = AsyncMock(side_effect=[RuntimeError("bad config"), None])
        with (
            patch.object(apply, "read_deployment_settings", AsyncMock(return_value={"TAVILY_API_KEY": "t"})),
            patch.object(apply, "_current_config", return_value=self._config()),
            patch.object(apply, "_rebuild_config", rebuild),
        ):
            with pytest.raises(RuntimeError):
                await apply.refresh()
            await apply.refresh()
        assert "TAVILY_API_KEY" in rebuild.await_args_list[1].args[2]


    async def test_boot_drops_the_config_read_before_the_saved_keys(self):
        """Reading the rows loads the config to look for platform secrets, so
        its ``${VAR}`` expansions predate the export."""
        from ptc_agent.config import file_utils

        file_utils._config_cache["stale-test-entry"] = {}
        rows = {"TAVILY_API_KEY": "t"}
        with (
            patch.object(apply, "read_deployment_settings", AsyncMock(return_value=rows)),
            patch.object(apply, "_platform_secrets_declared", return_value=False),
        ):
            await apply.load_at_boot()
        assert file_utils._config_cache == {}
        assert environment.os.environ["TAVILY_API_KEY"] == "t"


class TestSelfHostedOnly:
    def test_the_server_mounts_it_only_when_self_hosted(self):
        from src.server.app.setup import app
        from src.server.services import setup_settings

        paths = {getattr(route, "path", None) for route in app.routes}
        assert setup_settings.self_hosted()
        assert "/api/v1/setup/status" in paths
        with patch("src.config.settings.HOST_MODE", "platform"):
            assert not setup_settings.self_hosted()

    @pytest.mark.asyncio
    async def test_a_hosted_server_reads_no_saved_settings(self):
        from src.server.services import setup_settings

        load = AsyncMock()
        with (
            patch("src.config.settings.HOST_MODE", "platform"),
            patch.object(apply, "load_at_boot", load),
        ):
            reading = await setup_settings.boot()
        assert reading.autodetect is False
        load.assert_not_awaited()

    def test_saving_with_nothing_to_encrypt_with_says_so(self, monkeypatch):
        monkeypatch.delenv("BYOK_ENCRYPTION_KEY", raising=False)
        app = FastAPI()
        app.include_router(deployment_setup.router)
        response = TestClient(app).put("/api/v1/setup/keys", json={"keys": {}})
        assert response.status_code == 503
        assert "BYOK_ENCRYPTION_KEY" in response.json()["detail"]

    def test_only_the_app_reaches_it(self, monkeypatch):
        from src.server.auth import owner_requests

        monkeypatch.setenv(owner_requests.OWNER_SECRET_ENV, "s3cret")
        app = FastAPI()
        app.include_router(deployment_setup.router)
        save = AsyncMock()
        with patch.object(step, "save_keys", save):
            response = TestClient(app).put("/api/v1/setup/keys", json={"keys": {"FMP_API_KEY": "x"}})
        assert response.status_code == 401
        save.assert_not_awaited()
        with patch.object(step, "read_status", AsyncMock(return_value={"completed": False})):
            answered = TestClient(app).get(
                "/api/v1/setup/status", headers={"X-LangAlpha-Owner": "s3cret"}
            )
        assert answered.json() == {"completed": False}

    def test_the_status_route_answers(self):
        app = FastAPI()
        app.include_router(deployment_setup.router)
        with patch.object(step, "read_status", AsyncMock(return_value={"completed": False})):
            response = TestClient(app).get("/api/v1/setup/status")
        assert response.json() == {"completed": False}


class TestWebDist:
    @pytest.fixture
    def client(self, tmp_path, monkeypatch):
        (tmp_path / "assets").mkdir()
        (tmp_path / "index.html").write_text("shell")
        (tmp_path / "assets" / "app-abc.js").write_text("code")
        (tmp_path / "frame-host.html").write_text("frame")
        monkeypatch.setenv("LANGALPHA_WEB_DIST", str(tmp_path))
        app = FastAPI()

        @app.get("/api/v1/real/")
        async def real():
            return {"real": True}

        web_dist.serve_web_dist(app)
        return TestClient(app, follow_redirects=False)

    def test_a_client_route_gets_the_app_uncached(self, client):
        response = client.get("/chat/abc")
        assert response.text == "shell"
        assert response.headers["cache-control"] == "no-cache"

    def test_a_file_is_served_as_itself(self, client):
        assert client.get("/frame-host.html").text == "frame"
        assert client.get("/assets/app-abc.js").text == "code"

    @pytest.mark.parametrize("path", ["/version.json", "/assets/gone.js", "/api/v1/nope", "/ws/x"])
    def test_a_miss_here_is_a_404_and_not_the_app(self, client, path):
        assert client.get(path).status_code == 404

    def test_a_route_is_never_shadowed(self, client):
        assert client.get("/api/v1/real/").json() == {"real": True}
        # The router's own trailing-slash redirect still runs first.
        assert client.get("/api/v1/real").status_code == 307

    def test_a_path_cannot_leave_the_directory(self, tmp_path):
        (tmp_path / "web").mkdir()
        (tmp_path / "secret").write_text("secret")
        assert web_dist._file_under((tmp_path / "web").resolve(), "../secret") is None

    def test_nothing_changes_when_unset(self, monkeypatch):
        monkeypatch.delenv("LANGALPHA_WEB_DIST", raising=False)
        app = FastAPI()
        before = app.router.default
        web_dist.serve_web_dist(app)
        assert app.router.default is before

    def test_every_route_of_the_server_is_kept_from_the_web_app(self):
        """A route under a root the fallback does not know would answer its own
        misses with the web app's page instead of a 404."""
        from src.server.app.setup import app

        roots = {getattr(r, "path", "").strip("/").split("/", 1)[0] for r in app.routes}
        assert roots - {""} <= web_dist._BACKEND_ROOTS

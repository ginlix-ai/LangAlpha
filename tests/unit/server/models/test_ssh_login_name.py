"""The account names a computer may log in as, local or from a directory."""

import pytest

from src.server.models.computer import is_login_name


@pytest.mark.parametrize("name", ["dev", "a.b-c_d", "_svc", "alice@corp.example"])
def test_a_local_or_directory_account_is_a_login_name(name):
    assert is_login_name(name)


@pytest.mark.parametrize(
    "name", ["-oProxyCommand=x", "a b", "root;id", "@corp", "a@", "a@b@c", "x" * 129]
)
def test_what_ssh_would_read_as_an_option_or_more_is_not(name):
    assert not is_login_name(name)

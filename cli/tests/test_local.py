"""--local: the checks of the Docker wrapper (PR #30), without running Docker."""

from __future__ import annotations

import io
import shlex
from types import SimpleNamespace

import pytest

from conftest import FakeApi
from hf_s3ream_cli import IMAGE, local
from hf_s3ream_cli.events import Exit, Fail
from hf_s3ream_cli.main import copy_parser

ENV = {
    "HF_TOKEN": "hf_secret_value",
    "AWS_ACCESS_KEY_ID": "AKIAEXAMPLE",
    "AWS_SECRET_ACCESS_KEY": "aws_secret_value",
    "AWS_SESSION_TOKEN": "aws_session_value",
    "AWS_PROFILE": "dev",
    "AWS_DEFAULT_REGION": "us-east-1",
    "AWS_CONFIG_FILE": "/home/me/.aws/config",
    "AWS_CREDENTIAL_EXPIRATION": "2026-11-04T18:00:00Z",
    "AWS_REGION": "eu-west-3",
    "HF_XET_HIGH_PERFORMANCE": "1",
    "HF_S3REAM_PLANNER_JOB_ID": "abc",
    "HF_S3REAM_NO_VERIFY": "1",
    "RUST_LOG": "debug",
    "HOME": "/home/me",
    "PATH": "/usr/bin",
}


def args(*argv):
    return copy_parser().parse_args(["s3://src/p/", "org/dst", "--local", *argv])


class Docker:
    """`docker info` and exec, recorded."""

    def __init__(self, info_ok=True):
        self.info_ok, self.calls, self.exec = info_ok, [], None

    def run(self, cmd, **kw):
        self.calls.append(cmd)
        return SimpleNamespace(returncode=0 if self.info_ok else 1, stdout="", stderr="")

    def execve(self, path, cmd, env):
        self.exec = (path, cmd, env)


def run_local(a, passed=(), env=None, docker=None, which="/usr/bin/docker", machine="x86_64", tty=False, api=None):
    docker = docker or Docker()
    out, err = io.StringIO(), io.StringIO()
    code = local.run(
        a, list(passed), api=api or FakeApi(), env=dict(ENV if env is None else env), which=lambda _: which,
        run_cmd=docker.run, execve=docker.execve, machine=lambda: machine, out=out, err=err, tty=tty,
    )  # fmt: skip
    return code, docker, out.getvalue(), err.getvalue()


def test_the_container_gets_the_secrets_by_name_only():
    _, d, _, err = run_local(args(), passed=["--exclude", "*.tmp"])
    path, cmd, env = d.exec
    assert path == "/usr/bin/docker" and d.calls == [["/usr/bin/docker", "info"]]
    assert cmd[:8] == ["/usr/bin/docker", "run", "--rm", "--init", "--pull", "missing", "--platform", "linux/amd64"]
    names = [cmd[i + 1] for i, a in enumerate(cmd) if a == "-e"]
    assert names == [
        "AWS_ACCESS_KEY_ID", "AWS_REGION", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "HF_S3REAM_NO_VERIFY",
        "HF_TOKEN", "HF_XET_HIGH_PERFORMANCE", "RUST_LOG",
    ]  # fmt: skip
    joined = " ".join(cmd)
    for secret in ("hf_secret_value", "aws_secret_value", "aws_session_value"):
        assert secret not in joined
    assert env["HF_TOKEN"] == "hf_secret_value"  # the value goes through the environment
    assert cmd[-6:] == [IMAGE, "--exclude", "*.tmp", "--", "s3://src/p/", "org/dst"]
    assert "expire at 2026-11-04T18:00:00Z" in err


def test_analyze_is_a_dry_run_and_a_public_source_sends_no_aws_key():
    env = {"HF_TOKEN": "hf_x", "PATH": "/usr/bin"}
    _, d, _, _ = run_local(args("--analyze", "--no-sign-request", "--skip-existing"), env=env)
    cmd = d.exec[1]
    assert cmd[-6:] == ["--dry-run", "--no-sign-request", "--skip-existing", "--", "s3://src/p/", "org/dst"]
    assert not [a for a in cmd if a.startswith("AWS_")]


def test_print_command_runs_nothing_and_needs_no_docker():
    code, d, out, _ = run_local(args("--print-command", "--docker-arg=--network=host"), which=None)
    assert code == Exit.OK and d.exec is None and d.calls == []
    cmd = shlex.split(out)
    assert cmd[0] == "docker" and "--network=host" in cmd
    assert "hf_secret_value" not in out and "aws_secret_value" not in out


def test_a_terminal_goes_to_the_container():
    _, d, _, _ = run_local(args(), tty=True)
    assert "-it" in d.exec[1]


@pytest.mark.parametrize(
    ("which", "info_ok", "msg"), [(None, True, "not on PATH"), ("/usr/bin/docker", False, "daemon")]
)
def test_no_docker_is_exit_8(which, info_ok, msg):
    with pytest.raises(Fail) as e:
        run_local(args(), docker=Docker(info_ok), which=which)
    assert e.value.exit_code == Exit.LOCAL == 8 and msg in e.value.message


def test_an_arm_machine_is_warned():
    _, _, _, err = run_local(args(), machine="arm64")
    assert "linux/amd64 only" in err and "arm64" in err


@pytest.mark.parametrize(
    "flag", [["--detach"], ["--max-cost", "5"], ["--label", "x"], ["--namespace", "org"], ["--range-gib", "8"],
             ["--json"]],
)  # fmt: skip
def test_options_of_a_copy_on_jobs_are_refused(flag):
    with pytest.raises(Fail) as e:
        run_local(args(*flag))
    assert e.value.exit_code == Exit.USAGE and flag[0] in e.value.message


def test_bad_src_or_dest_fail_before_docker():
    a = copy_parser().parse_args(["foo", "org/dst", "--local"])
    with pytest.raises(Fail) as e:
        run_local(a)
    assert e.value.exit_code == Exit.USAGE


def test_an_expired_sso_session_says_how_to_log_in():
    env = {"HF_TOKEN": "hf_x", "AWS_PROFILE": "sso-dev", "PATH": "/usr/bin"}

    def aws(cmd, **kw):
        assert kw["env"]["AWS_PROFILE"] == "sso-dev"
        return SimpleNamespace(
            returncode=255, stdout="", stderr="aws: [ERROR]: Error when retrieving token from sso: Token has expired"
        )

    from hf_s3ream_cli import hub

    with pytest.raises(Fail) as e:
        hub.aws_secrets(False, run=aws, env=env)
    assert "aws sso login" in e.value.message and e.value.exit_code == Exit.AUTH


def test_a_new_bucket_is_created_private_as_on_jobs():
    api = FakeApi()
    _, _, _, err = run_local(args(), api=api)
    assert api.bucket_exists and "bucket org/dst created, private" in err
    _, _, _, err = run_local(args(), api=api)  # it exists now: nothing to say
    assert "created" not in err


def test_a_bare_bucket_name_is_a_bucket_of_yours():
    a = copy_parser().parse_args(["s3://src/p/", "dst", "--local"])
    _, d, _, err = run_local(a)
    assert d.exec[1][-1] == "alice/dst" and "bucket alice/dst created" in err


def test_print_command_creates_nothing():
    api = FakeApi()
    run_local(args("--print-command"), api=api)
    assert not api.bucket_exists


def test_a_public_read_says_the_credentials_are_not_used():
    _, d, _, err = run_local(args("--no-sign-request"))
    assert "AWS_PROFILE and AWS_ACCESS_KEY_ID not used" in err
    assert not [a for a in d.exec[1] if a.startswith("AWS_ACCESS")]

"""What the E2B runtime and its streams share: who commands run as, and what
an envd result's exit status means."""

# Every command and file operation runs as root, as on Daytona: the image is
# built as root, HOME is /root, and packages install into the system Python.
_USER = "root"

# envd reports a process a signal ended as exit -1, naming the signal the way
# Go does ("signal: killed"). A shell reports 128 + its number, which is what
# the other providers return, so an OOM kill reads 137 on every one of them.
# Linux numbers and Go's names for them, the sandbox's, not this host's
# (SIGBUS is 10 on macOS). Go names a signal past these "signal 34".
_SIGNAL_NUMBERS = {
    name: number
    for number, name in enumerate(
        [
            "hangup",
            "interrupt",
            "quit",
            "illegal instruction",
            "trace/breakpoint trap",
            "aborted",
            "bus error",
            "floating point exception",
            "killed",
            "user defined signal 1",
            "segmentation fault",
            "user defined signal 2",
            "broken pipe",
            "alarm clock",
            "terminated",
            "stack fault",
            "child exited",
            "continued",
            "stopped (signal)",
            "stopped",
            "stopped (tty input)",
            "stopped (tty output)",
            "urgent I/O condition",
            "CPU time limit exceeded",
            "file size limit exceeded",
            "virtual timer expired",
            "profiling timer expired",
            "window changed",
            "I/O possible",
            "power failure",
            "bad system call",
        ],
        start=1,
    )
}


def _exit_status(exit_code: int, error: str | None) -> int:
    """The shell's exit status for an envd result."""
    if exit_code != -1 or not error or not error.startswith("signal: "):
        return exit_code
    name = error.removeprefix("signal: ").removesuffix(" (core dumped)")
    number = _SIGNAL_NUMBERS.get(name)
    if number is None and name.removeprefix("signal ").isdigit():
        number = int(name.removeprefix("signal "))
    return exit_code if number is None else 128 + number

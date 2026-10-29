"""Shell that tells whether a recorded process is still the one recorded, and
signals its process group only then.

A command's wrapper leads a process group of its own and records its pid and
start time, so a later script never signals a pid that has since gone to
someone else. It also exports them as ``EXEC_MARK=<pid>-<start>``, which
everything it starts inherits: with the leader gone, the group's id alone
proves nothing (once the group empties the id may go to another group), and
the mark is what tells the command's own processes apart. Exec sessions, the
SSH computer's stop and detached jobs all prove and signal through these
functions, so the three agree on what is theirs.

Every function runs in bash, dash, and a zsh under ``emulate sh``, which is
what the providers' exec shells are.
"""

from __future__ import annotations

#: The variable a wrapper exports to mark what it starts.
EXEC_MARK = "LANGALPHA_EXEC"


def ps_start_sh(pid: str) -> str:
    """Shell printing ``pid``'s start time as one word, where there is no
    /proc (macOS): ps's, its words run together so a record stays two fields.
    To the second, which tells a reused pid apart."""
    return f'set -- $(LC_ALL=C ps -o lstart= -p {pid} 2>/dev/null); echo "$1$2$3$4$5"'


# ``pstart PID [z]`` prints PID's start time as one word, as a wrapper records
# its own: field 22 of its stat, counted after the command name, which may hold
# spaces. It fails for a pid that is gone, and for a zombie unless given ``z``:
# a sandbox whose init reaps nothing keeps a killed wrapper as one, which would
# otherwise read as running forever. A zombie still holds its pid and its
# group's id, though, so its start can still prove the group it leads
# (``psig`` with ``z``).
#
# ``plive FILE`` succeeds while the process FILE records (``PID START``) is
# still the one recorded.
#
# ``ours PGID PID-START`` succeeds when a process in group PGID carries that
# mark. A process shows the environment it was started with, so a subshell
# that ran no program, or a program that cleared its environment, shows none,
# and with no /proc (macOS) ps hides that of Apple's own binaries. A group
# left with only those stays unproven, and running.
#
# ``psig SIG PGID START [z]`` signals group PGID when it is the record's: its
# leader is the process recorded (and is signalled by its pid too), a zombie
# one only given ``z``, or a member carries the mark. It fails, signalling
# nothing, otherwise. Exec sessions and the SSH stop pass ``z``; jobs do not,
# since a job whose wrapper was killed reads as having vanished, and its stop
# must not take a lone zombie for a job still running (nor wait out a grace
# the zombie, holding its group, never ends).
# ``kill -SIG -PGID`` with no ``--``: dash refuses that form.
#
# ``pgrace TENTHS PGID...`` waits up to TENTHS tenths of a second while any of
# the groups lives, then KILLs what is left, so many groups end in one grace.
# Only a group whose leader is the process it found, or gone with its members
# left: a group that ended in the grace may have given its id to another.
PROC_SH = (
    "pstart() { local s z=$2; if [ ! -d /proc/1 ]; then s=$("
    + ps_start_sh('"$1"')
    + '); [ -n "$s" ] && echo "$s"; return; fi; '
    '{ read -r s < "/proc/$1/stat"; } 2>/dev/null || return 1; '
    's=${s##*) }; set -- $s; [ "$1" != Z ] || [ -n "$z" ] || return 1; echo "${20}"; }\n'
    'plive() { local p t; { read -r p t < "$1"; } 2>/dev/null; '
    'case $p in ""|*[!0-9]*) return 1;; esac; '
    '[ -n "$t" ] && [ "$(pstart "$p")" = "$t" ]; }\n'
    f'ours() {{ local g="$1" m="{EXEC_MARK}=$2" f l; '
    "if [ -d /proc/1 ]; then for f in /proc/[0-9]*/stat; do "
    '{ read -r l < "$f"; } 2>/dev/null || continue; l=${l##*) }; set -- $l; '
    '[ "$3" = "$g" ] || continue; '
    r"""tr '\000' '\n' 2>/dev/null < "${f%/stat}/environ" | grep -qxF -- "$m" """
    "&& return 0; done; return 1; fi; "
    "LC_ALL=C ps -axwwE -o pgid=,command= 2>/dev/null | "
    """G="$g" M="$m" awk '$1 == ENVIRON["G"] { for (i = 2; i <= NF; i++) """
    """if ($i == ENVIRON["M"]) { f = 1; exit } } END { exit !f }'; }\n"""
    'psig() { if [ -n "$3" ] && [ "$(pstart "$2" "$4")" = "$3" ]; then '
    'kill -"$1" "-$2" 2>/dev/null; kill -"$1" "$2" 2>/dev/null; return 0; fi; '
    'ours "$2" "$2-$3" || return 1; kill -"$1" "-$2" 2>/dev/null; return 0; }\n'
    'pgrace() { local n=$1 i=0 G L p s S=; shift; G=$*; '
    'for p in $G; do S="$S $p=$(pstart "$p" z)"; done; '
    'while [ -n "$G" ] && [ $i -lt "$n" ]; do L=; '
    'for p in $G; do ! kill -0 "-$p" 2>/dev/null || L="$L $p"; done; G=$L; '
    '[ -z "$G" ] || sleep 0.1; i=$((i+1)); done; '
    'for p in $G; do s=$(pstart "$p" z); '
    'case " $S " in *" $p=$s "*) ;; *) [ -z "$s" ] || continue;; esac; '
    'kill -KILL "-$p" 2>/dev/null; done; }\n'
)

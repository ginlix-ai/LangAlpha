# Keeps the CLI running apart from the channel that starts it, so a lost
# connection leaves it running, and relays it to that channel and to each
# one that attaches after it (SSHTransport).
#
# start DIR GRACE ARGV...: lay out DIR/attach, start the supervisor there,
# then attach from the start, as below. The supervisor runs ARGV in the
# current directory with its stdout and stderr appended to files in
# DIR/attach and its stdin fed from the channel attached, complete lines
# only. It holds one channel at a time, a newer one replacing the last, and
# ends the CLI once none has been attached for GRACE seconds. A line
# "langalpha-eof" ends the CLI's input.
#
# attach DIR OUT ERR: relay the CLI's stdout from byte OUT and its stderr
# from line ERR on, and this channel's input to the CLI. The first line out
# is "langalpha-attach N", N being the bytes of input the supervisor has
# taken, or "langalpha-attach done" once the CLI has ended, or
# "langalpha-attach gone" where there is nothing to attach to: no
# supervisor, or one that ended the CLI for having no channel. The first
# line on stderr is "__langalpha__ attach", and "__langalpha__ in N" lines
# go between the CLI's own as the supervisor takes more input. Once the CLI
# has ended and both files are relayed, the last line on stderr is
# "__langalpha__ exit x N" (its exit status), "__langalpha__ exit s N" (its
# signal) or "__langalpha__ exit g" (ended for having no channel). A channel
# that ends without that line lost its connection, whatever its own status.
use strict;
use warnings;
use Cwd qw(getcwd);
use Fcntl qw(O_RDONLY O_WRONLY O_CREAT O_EXCL O_APPEND F_GETFL F_SETFL O_NONBLOCK);
use POSIX qw(setsid WNOHANG);
use Socket qw(PF_UNIX SOCK_STREAM sockaddr_un);

my ($mode, $dir) = splice(@ARGV, 0, 2);
my $at = "$dir/attach";

sub put {
    my ($fh, $data) = @_;
    while (length $data) {
        my $n = syswrite($fh, $data);
        return 0 unless defined $n;
        substr($data, 0, $n, '');
    }
    return 1;
}

sub gone {
    put(\*STDOUT, "langalpha-attach gone\n");
    exit 3;
}

sub ended {
    open(my $rc, '<', 'rc') or return '';
    my $status = <$rc> // '';
    chomp $status;
    return $status;
}

sub supervise {
    my ($listen, $out, $err, $cwd, $grace, @argv) = @_;
    $SIG{PIPE} = 'IGNORE';
    $SIG{HUP} = 'IGNORE';
    pipe(my $r, my $w) or POSIX::_exit(1);
    defined(my $cli = fork) or POSIX::_exit(1);
    if (!$cli) {
        setpgrp(0, 0);
        $SIG{PIPE} = 'DEFAULT';
        $SIG{HUP} = 'DEFAULT';
        chdir $cwd or POSIX::_exit(1);
        open(STDIN, '<&', $r) && open(STDOUT, '>&', $out) && open(STDERR, '>&', $err) or POSIX::_exit(1);
        close $_ for $r, $w, $out, $err, $listen;
        { no warnings 'exec'; exec { $argv[0] } @argv; }
        put(\*STDERR, "langalpha: could not start $argv[0]: $!\n");
        POSIX::_exit(127);
    }
    close $_ for $r, $out, $err;
    fcntl($w, F_SETFL, fcntl($w, F_GETFL, 0) | O_NONBLOCK);
    my ($client, $part, $queue, $given, $eof, $alone, $killed, $ended) = (undef, '', '', 0, 0, time, 0, undef);
    # The acks owed the channel, written without blocking: a channel slow to
    # read them must not stall the CLI's input. Only the newest one matters,
    # so one not yet started is replaced by a newer.
    my ($ack, $fresh, $next) = ('', 0, undef);
    while (1) {
        if (waitpid($cli, WNOHANG) == $cli) {
            $ended = $?;
            last;
        }
        if (!$client && !$killed && time - $alone >= $grace) {
            kill 'TERM', -$cli;
            $killed = time;
        }
        kill 'KILL', -$cli if $killed && time - $killed >= 5;
        my ($rin, $win) = ('', '');
        vec($rin, fileno $listen, 1) = 1;
        vec($rin, fileno $client, 1) = 1 if $client;
        vec($win, fileno $w, 1) = 1 if $w && length $queue;
        vec($win, fileno $client, 1) = 1 if $client && length $ack;
        next if select(my $rout = $rin, my $wout = $win, undef, 0.5) <= 0;
        if (vec($rout, fileno $listen, 1) && accept(my $new, $listen)) {
            close $client if $client;
            ($client, $part, $ack, $fresh, $next) = ($new, '', '', 0, undef);
            put($client, "in $given\n");
            fcntl($client, F_SETFL, fcntl($client, F_GETFL, 0) | O_NONBLOCK);
        }
        if ($client && vec($rout, fileno $client, 1)) {
            my $n = sysread($client, my $data, 65536);
            if (!$n) {
                if (!defined $n && $!{EAGAIN}) {
                    next;
                }
                close $client;
                ($client, $part, $alone) = (undef, '', time);
            } else {
                $part .= $data;
                my $before = $given;
                while ((my $i = index($part, "\n")) >= 0) {
                    my $line = substr($part, 0, $i + 1, '');
                    $given += length $line;
                    if ($line eq "langalpha-eof\n") {
                        $eof = 1;
                    } elsif ($w && !$eof) {
                        $queue .= $line;
                    }
                }
                if ($given != $before) {
                    if (!length $ack || $fresh) {
                        ($ack, $fresh) = ("a $given\n", 1);
                    } else {
                        $next = $given;
                    }
                }
            }
        }
        if ($client && length $ack && vec($wout, fileno $client, 1)) {
            my $n = syswrite($client, $ack);
            if (defined $n) {
                substr($ack, 0, $n, '');
                $fresh = 0 if $n;
                ($ack, $fresh, $next) = ("a $next\n", 1, undef) if !length $ack && defined $next;
            } elsif (!$!{EAGAIN}) {
                close $client;
                ($client, $part, $alone, $ack, $fresh, $next) = (undef, '', time, '', 0, undef);
            }
        }
        if ($w && length $queue && vec($wout, fileno $w, 1)) {
            my $n = syswrite($w, $queue);
            if (defined $n) {
                substr($queue, 0, $n, '');
            } elsif (!$!{EAGAIN}) {
                close $w;
                ($w, $queue) = (undef, '');
            }
        }
        if ($w && $eof && !length $queue) {
            close $w;
            $w = undef;
        }
    }
    my $status = $killed ? 'g' : ($ended & 127) ? 's ' . ($ended & 127) : 'x ' . ($ended >> 8);
    if (open(my $rc, '>', 'rc.new')) {
        print $rc "$status\n";
        close $rc;
        rename 'rc.new', 'rc';
    }
    unlink 's';
    close $client if $client;
}

if ($mode eq 'start') {
    my $grace = shift @ARGV;
    my $cwd = getcwd();
    mkdir $at, 0700 or die "langalpha: could not make $at: $!\n";
    chdir $at or die "langalpha: $at: $!\n";
    my $listen;
    socket($listen, PF_UNIX, SOCK_STREAM, 0) && bind($listen, sockaddr_un('s')) && listen($listen, 4)
        or die "langalpha: could not listen in $at: $!\n";
    sysopen(my $out, 'out', O_WRONLY | O_CREAT | O_EXCL | O_APPEND, 0600)
        && sysopen(my $err, 'err', O_WRONLY | O_CREAT | O_EXCL | O_APPEND, 0600)
        or die "langalpha: could not open the CLI's output in $at: $!\n";
    defined(my $pid = fork) or die "langalpha: fork: $!\n";
    if (!$pid) {
        setsid();
        open(STDIN, '<', '/dev/null');
        open(STDOUT, '>', '/dev/null');
        open(STDERR, '>', '/dev/null');
        supervise($listen, $out, $err, $cwd, $grace, @ARGV);
        POSIX::_exit(0);
    }
    close $_ for $listen, $out, $err;
    @ARGV = (0, 0);
} else {
    chdir $at or gone();
}

my ($out_at, $err_skip) = @ARGV;
my ($sock, $given);
if (socket($sock, PF_UNIX, SOCK_STREAM, 0) && connect($sock, sockaddr_un('s'))) {
    my ($c, $l) = ('', '');
    while (sysread($sock, $c, 1)) {
        last if $c eq "\n";
        $l .= $c;
    }
    $given = $1 if $l =~ /^in (\d+)$/;
}
if (!defined $given) {
    close $sock if $sock;
    $sock = undef;
    gone() unless -e 'rc' && ended() ne 'g';
}
sysopen(my $o, 'out', O_RDONLY) && sysopen(my $e, 'err', O_RDONLY) or gone();
sysseek($o, $out_at, 0);
my $epart = '';
while ($err_skip > 0 && sysread($e, my $data, 65536)) {
    $epart .= $data;
    while ($err_skip > 0 && (my $i = index($epart, "\n")) >= 0) {
        substr($epart, 0, $i + 1, '');
        $err_skip--;
    }
}
$epart = '' if $err_skip > 0;
put(\*STDOUT, 'langalpha-attach ' . (defined $given ? $given : 'done') . "\n") or exit 0;
put(\*STDERR, "__langalpha__ attach\n") or exit 0;

my $spart = '';
while (1) {
    my $done = -e 'rc';
    while ((my $n = sysread($o, my $data, 65536)) > 0) {
        put(\*STDOUT, $data) or exit 0;
    }
    while ((my $n = sysread($e, my $data, 65536)) > 0) {
        $epart .= $data;
    }
    my $i = rindex($epart, "\n");
    put(\*STDERR, substr($epart, 0, $i + 1, '')) or exit 0 if $i >= 0;
    if ($done) {
        put(\*STDERR, "$epart\n") if length $epart;
        put(\*STDERR, '__langalpha__ exit ' . ended() . "\n");
        exit 0;
    }
    exit 0 unless $sock;
    my $rin = '';
    vec($rin, fileno STDIN, 1) = 1;
    vec($rin, fileno $sock, 1) = 1;
    next if select(my $rout = $rin, undef, undef, 0.05) <= 0;
    if (vec($rout, fileno STDIN, 1)) {
        my $n = sysread(STDIN, my $data, 65536);
        exit 0 unless $n;
        put($sock, $data);
    }
    if (vec($rout, fileno $sock, 1)) {
        my $n = sysread($sock, my $data, 65536);
        if (!$n) {
            close $sock;
            $sock = undef;
            sleep 1 unless -e 'rc';
            next;
        }
        $spart .= $data;
        while ($spart =~ s/^a (\d+)\n//) {
            put(\*STDERR, "__langalpha__ in $1\n") or exit 0;
        }
    }
}

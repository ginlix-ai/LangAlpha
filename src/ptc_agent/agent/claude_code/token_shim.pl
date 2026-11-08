# Starts the CLI with the turn's token; runs after token_file.pl.
#
# Arguments: whether relay is allowed (1 or 0), the token file (empty for
# none), then the CLI's argv. The first line of its stdout announces where the
# token comes from (file, relay or none), and only after announcing relay
# does it read one line of stdin, byte by byte, leaving the rest to the CLI:
# empty for none, else the setup token. A token file holds one too. The token
# goes into a socket pair the CLI reads by the descriptor $fd names (macOS has
# no /proc/self/fd), never into an argv, an environment or a file. $fd is set
# ahead of this file (machine_token shim). $$ stays the CLI's pid across the
# exec, for a close that has to signal it. The socket is held at file scope, since perl closes one whose
# variable goes out of scope.
use Socket;

my ($relay, $f) = splice(@ARGV, 0, 2);
my ($how, $t) = length $f ? token_file($f) : ('none', '');
$how = $how eq 'file' ? 'file' : $relay ? 'relay' : 'none';
-x $ARGV[0] or die "langalpha: Claude Code is missing at $ARGV[0]\n";
syswrite STDERR, "__langalpha__ pid $$\n";
syswrite STDOUT, "langalpha-token $how\n";
if ($how eq 'relay') {
    my ($c, $l) = ('', '');
    while (sysread STDIN, $c, 1) {
        last if $c eq "\n";
        $l .= $c;
    }
    $t = $l;
}
my ($r, $w);
if (length $t) {
    socketpair($r, $w, AF_UNIX, SOCK_STREAM, PF_UNSPEC) or die "langalpha: socketpair: $!\n";
    syswrite $w, "$t\n";
    close $w;
    $t = '';
    fcntl($r, F_SETFD, 0) or die "langalpha: fcntl: $!\n";
    $ENV{$fd} = fileno $r;
}
exec { $ARGV[0] } @ARGV or die "langalpha: could not start $ARGV[0]: $!\n";

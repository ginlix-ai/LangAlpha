# Starts the CLI with the turn's credential; runs after token_file.pl.
#
# Arguments: whether relay is allowed (1 or 0), the token file (empty for
# none), then the CLI's argv. The first line of its stdout announces where the
# credential comes from (file, relay or none), and only after announcing relay
# does it read one line of stdin, byte by byte, leaving the rest to the CLI:
# empty for none, else the credential's kind (setup_token or api_key), a
# space and the credential. A token file holds a setup token. The credential
# goes into a socket pair the CLI reads by the descriptor its kind's variable
# names (macOS has no /proc/self/fd), never into an argv, an environment or a
# file. %fd, each kind's variable, is set ahead of this file (machine_token
# shim). $$ stays the CLI's pid across the exec, for a close that has to
# signal it. The socket is held at file scope, since perl closes one whose
# variable goes out of scope.
use Socket;

my ($relay, $f) = splice(@ARGV, 0, 2);
my ($how, $t) = length $f ? token_file($f) : ('none', '');
$how = $how eq 'file' ? 'file' : $relay ? 'relay' : 'none';
my $kind = 'setup_token';
-x $ARGV[0] or die "langalpha: Claude Code is missing at $ARGV[0]\n";
syswrite STDERR, "__langalpha__ pid $$\n";
syswrite STDOUT, "langalpha-token $how\n";
if ($how eq 'relay') {
    my ($c, $l) = ('', '');
    while (sysread STDIN, $c, 1) {
        last if $c eq "\n";
        $l .= $c;
    }
    if (length $l) {
        ($kind, $t) = split / /, $l, 2;
        exists $fd{$kind} && length $t or die "langalpha: the relayed credential is not one this shim knows\n";
    }
}
my ($r, $w);
if (length $t) {
    socketpair($r, $w, AF_UNIX, SOCK_STREAM, PF_UNSPEC) or die "langalpha: socketpair: $!\n";
    syswrite $w, "$t\n";
    close $w;
    $t = '';
    fcntl($r, F_SETFD, 0) or die "langalpha: fcntl: $!\n";
    $ENV{$fd{$kind}} = fileno $r;
}
exec { $ARGV[0] } @ARGV or die "langalpha: could not start $ARGV[0]: $!\n";

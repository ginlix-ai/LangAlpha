# A computer's LangAlpha token file on its machine, or one staged for a check,
# by the one set of rules every reader of it keeps: the shim a turn starts the
# CLI through, and the status check.
#
# token_file($path) is ('file', $token) for a regular file of the login's own
# that no one else can open and that holds text, ('none', '') when no file is
# there, and ('invalid', '') for anything else, with a warning saying why.
# The token is the file's text with surrounding whitespace stripped.
use strict;
use Fcntl;

sub token_file {
    my ($f) = @_;
    if (-l $f) {
        warn "langalpha: $f is a symbolic link, so it is ignored\n";
        return ('invalid', '');
    }
    my $h;
    if (!sysopen($h, $f, O_RDONLY | O_NOFOLLOW | O_NONBLOCK)) {
        return ('none', '') if $!{ENOENT};
        warn "langalpha: $f cannot be read ($!), so it is ignored\n";
        return ('invalid', '');
    }
    my @s = stat $h;
    my $why = !-f _ ? "$f is not a regular file, so it is ignored"
        : $s[4] != $< ? "$f is not owned by you, so it is ignored"
        : $s[2] & 077 ? "other users can open $f, so it is ignored; chmod 600 it"
        : '';
    my $t = '';
    if (!$why) {
        local $/;
        $t = <$h> // '';
        $t =~ s/^\s+|\s+$//g;
        $why = "$f holds no token, so it is ignored" if !length $t;
    }
    close $h;
    if ($why) {
        warn "langalpha: $why\n";
        return ('invalid', '');
    }
    return ('file', $t);
}

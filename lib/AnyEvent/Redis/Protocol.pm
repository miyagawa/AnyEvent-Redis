package AnyEvent::Redis::Protocol;

use strict;
use warnings;
use Protocol::Redis;

=head1 NAME

AnyEvent::Redis::Protocol - Redis response adapter (read handler) for AnyEvent

=head1 DESCRIPTION

This package should not be directly used.  It provides an AnyEvent read handler
capable of parsing Redis responses.

=head1 SEE ALSO

L<AnyEvent::Handle>, L<Protocol::Redis>,
Redis Protocol Specification L<http://redis.io/topics/protocol>

=cut

sub anyevent_read_type {
    my ($handle, $cb, $p_r) = @_;

    my $rbuf = \$handle->{rbuf};

    return sub {
        my $input = substr $$rbuf, 0, length($$rbuf), "";
        $p_r->parse($input) if length $input;

        if(my $msg = $p_r->get_message) {
            $cb->($msg->{data}, $msg->{type} eq '-');
            return 1;
        }

        ()
    }
}

=head1 AUTHOR

Michael S. Fischer <michael+cpan@dynamine.net>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2010 Michael S. Fischer.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut

1;

__END__

# vim:syn=perl:ts=4:sw=4:et:ai

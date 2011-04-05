package AnyEvent::Redis::Protocol;
use strict;
use warnings;

=head1 NAME

AnyEvent::Redis::Protocol - Redis response adapter (read handler) for AnyEvent

=head1 DESCRIPTION

This package should not be directly used.  It provides an AnyEvent read handler
capable of parsing Redis responses using L<Protocol::Redis> underneath.

=head1 SEE ALSO

L<AnyEvent::Handle>, L<Protocol::Redis>,
L<Redis Protocol Specification|http://redis.io/topics/protocol>

=cut

sub anyevent_read_type {
    my ($handle, $cb, $p_r) = @_;
    my $rbuf = \$handle->{rbuf};

    return sub {
        return unless defined $$rbuf;

        my $input = substr $$rbuf, 0, length($$rbuf), "";
        $p_r->parse($input) if length $input;

        if(my $message = $p_r->get_message) {
            my $is_error = $message->{type} eq '-';
            $cb->(_remove_type($handle, $message->{data}), $is_error);
            return 1;
        }

        return;
    }
}

# Adapt from the Protocol::Redis format of nested hashrefs to AE::Redis's
# nested list format with a special class for errors.
sub _remove_type {
    my ($handle) = @_;

    if (ref $_[1] eq 'ARRAY') {
        [map +($_->{type} eq '-'
                      ? bless \$_->{data}, "AnyEvent::Redis::Error"
                      : _remove_type($handle, $_->{data})
                ), @{$_[1]}];
    }
    elsif (ref $_[1] eq 'HASH') {
        _remove_type($handle, $_[1]->{data});
    }
    else {
        ($handle->{encoding} && length $_[1])
          ? $handle->{encoding}->decode($_[1])
          : $_[1];
    }
}

=head1 AUTHOR

Michael S. Fischer <michael+cpan@dynamine.net>

David Leadbeater <dglE<xFE6B>dgl.cx>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2010 Michael S. Fischer.

Copyright (C) 2011 David Leadbeater.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut

1;

__END__

# ex:sw=4:et:

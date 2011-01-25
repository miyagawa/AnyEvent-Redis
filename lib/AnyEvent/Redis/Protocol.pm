package AnyEvent::Redis::Protocol;

use strict;
use warnings;

=head1 NAME

AnyEvent::Redis::Protocol - Redis response parser (read handler) for AnyEvent

=head1 DESCRIPTION

This package should not be directly used.  It provides an AnyEvent read handler
capable of parsing Redis responses.

=head1 SEE ALSO

L<AnyEvent::Handle>,
Redis Protocol Specification L<http://redis.io/topics/protocol>

=cut

sub anyevent_read_type {
    my ($handle, $cb) = @_;

    return sub {
        $handle->push_read(line => sub {
            my $line = $_[1];
            my $type = substr($line, 0, 1);
            my $value = substr($line, 1);
            if ($type eq '*') {
                # Multi-bulk reply 
                my $remaining = $value;
                if ($remaining == 0) {
                    $cb->([]);
                } elsif ($remaining == -1) {
                    $cb->(undef);
                } else {
                    my $results = [];
                    $handle->unshift_read(sub {
                        my $need_more_data = 0;
                        do {
                            if ($handle->{rbuf} =~ /^(\$(-?\d+)\015\012)/) {
                                my ($match, $vallen) = ($1, $2);
                                if ($vallen == -1) {
                                    # Delete the bulk header.
                                    substr($handle->{rbuf}, 0, length($match), '');
                                    push @$results, undef;
                                    unless (--$remaining) {
                                        $cb->($results);
                                        return 1;
                                    }
                                } elsif (length $handle->{rbuf} >= (length($match) + $vallen + 2)) {
                                    # OK, we have enough in our buffer.
                                    # Delete the bulk header.
                                    substr($handle->{rbuf}, 0, length($match), '');
                                    my $value = substr($handle->{rbuf}, 0, $vallen, '');
                                    $value = $handle->{encoding}->decode($value) 
                                        if $handle->{encoding} && $vallen;
                                    push @$results, $value;
                                    # Delete trailing data characters.
                                    substr($handle->{rbuf}, 0, 2, '');
                                    unless (--$remaining) {
                                        $cb->($results);
                                        return 1;
                                    }
                                } else {
                                    $need_more_data = 1;
                                }
                            } elsif ($handle->{rbuf} =~ s/^([\+\-:])([^\015\012]*)\015\012//) {
                                my ($type, $value) = ($1, $2);
                                if ($type eq '+' || $type eq ':') {
                                    push @$results, $value;
                                } elsif ($type eq '-') {
                                    # Embedded error; this seems possible only in EXEC answer,
                                    #  so include error in results; don't abort parsing
                                    push @$results, bless \$value, 'AnyEvent::Redis::Error';
                                }
                                unless (--$remaining) {
                                    $cb->($results);
                                    return 1;
                                }
                            } elsif (substr($handle->{rbuf}, 0, 1) eq '*') {
                                # Oh, how fun!  A nested bulk reply.
                                my $reader; $reader = sub {
                                    $handle->unshift_read("AnyEvent::Redis::Protocol" => sub {
                                            push @$results, $_[0];
                                            if (--$remaining) {
                                                $reader->();
                                            } else {
                                                undef $reader;
                                                $cb->($results);
                                            }
                                    });
                                };
                                $reader->();
                                return 1;
                            } else {
                                # Nothing matched - read more...
                                $need_more_data = 1;
                            }
                        } until $need_more_data;
                        return; # get more data
                    });
                }
            } elsif ($type eq '+' || $type eq ':') {
                # Single line/integer reply
                $cb->($value);
            } elsif ($type eq '-') {
                # Single-line error reply
                $cb->($value, 1);
            } elsif ($type eq '$') {
                # Bulk reply
                my $length = $value;
                if ($length == -1) {
                    $cb->(undef);
                } else {
                    # We need to read 2 bytes more than the length (stupid 
                    # CRLF framing).  Then we need to discard them.
                    $handle->unshift_read(chunk => $length + 2, sub {
                        my $data = $_[1];
                        my $value = substr($data, 0, $length);
                        $value = $handle->{encoding}->decode($value)
                            if $handle->{encoding} && $length;
                        $cb->($value);
                    });
                }
            }
            return 1;
        });
        return 1;
    };
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

package AnyEvent::Redis;

use strict;
use 5.008_001;
our $VERSION = '0.12';

use constant DEBUG => $ENV{ANYEVENT_REDIS_DEBUG};
use AnyEvent;
use AnyEvent::Handle;
use AnyEvent::Socket;
use Try::Tiny;
use Carp qw(croak);

our $AUTOLOAD;

sub new {
    my($class, %args) = @_;

    my $host = delete $args{host} || '127.0.0.1';
    my $port = delete $args{port} || 6379;

    bless {
        host => $host,
        port => $port,
        %args,
    }, $class;
}

sub run_cmd {
    my $self = shift;
    my $cmd  = shift;

    $self->{cmd_cb} or return $self->connect($cmd, @_);
    $self->{cmd_cb}->($cmd, @_);
}

sub DESTROY { }

sub AUTOLOAD {
    my $self = shift;
    (my $method = $AUTOLOAD) =~ s/.*:://;
    $self->run_cmd($method, @_);
}

sub all_cv {
    my $self = shift;
    $self->{all_cv} = shift if @_;
    unless ($self->{all_cv}) {
        $self->{all_cv} = AE::cv;
    }
    $self->{all_cv};
}

sub cleanup {
    my $self = shift;
    delete $self->{cmd_cb};
    delete $self->{sock};
    $self->{on_error}->(@_);
}

sub connect {
    my $self = shift;

    my $cv;
    if (@_) {
        $cv = AE::cv;
        push @{$self->{connect_queue}}, [ $cv, @_ ];
    }

    return $cv if $self->{sock};

    $self->{sock} = tcp_connect $self->{host}, $self->{port}, sub {
        my $fh = shift
            or do {
              $cv->croak("Can't connect Redis server: $!");
              return
            };

        my $hd = AnyEvent::Handle->new(
            fh => $fh,
            on_error => sub { $_[0]->destroy;
                              if ($_[1]) {
                                  $self->cleanup($_[2]);
                              }
                          },
            on_eof   => sub { $_[0]->destroy;
                              $self->cleanup('connection closed');
                          },
        );

        $self->{cmd_cb} = sub {
            $self->all_cv->begin;
            my $command = shift;

            my($cv, $cb);
            if (@_) {
                $cv = pop if UNIVERSAL::isa($_[-1], 'AnyEvent::CondVar');
                $cb = pop if ref $_[-1] eq 'CODE';
            }

            my $send = join("\r\n",
                  "*" . (1 + @_),
                  map(('$' . length $_ => $_), uc($command), @_))
                . "\r\n";

            warn $send if DEBUG;

            $cv ||= AE::cv;

            $hd->push_write($send);

            # Are we already subscribed to anything?
            if($self->{sub} && %{$self->{sub}}) {

              croak "Use of non-pubsub command during pubsub session may result in unexpected behaviour"
                unless $command =~ /^p?(?:un)?subscribe$/i;

              # Remember subscriptions
              $self->{sub}->{$_} ||= [$cv, $cb] for @_;

            } elsif ($command !~ /^p?subscribe$/i) {

                $cv->cb(sub {
                    my $cv = shift;
                    try {
                        my $res = $cv->recv;
                        $cb->($res);
                    } catch {
                        ($self->{on_error} || sub { die @_ })->($_);
                    }
                }) if $cb;

                $hd->push_read(redis => sub {
                        my($res, $err) = @_;

                        if($command eq 'info') {
                          $res = { map { split /:/, $_, 2 } split /\r\n/, $res };
                        }

                        $self->all_cv->end;
                        $err ? $cv->croak($res) : $cv->send($res);
                    });

            } else {
                croak "Must provide a CODE reference for subscriptions" unless $cb;

                # Remember subscriptions
                $self->{sub}->{$_} ||= [$cv, $cb] for @_;

                my $res_cb; $res_cb = sub {

                    $hd->push_read(ref $self => sub {
                            my($res, $err) = @_;

                            if(ref $res) {
                                my $action = lc $res->[0];
                                warn "$action $res->[1]" if DEBUG;

                                if($action eq 'message') {
                                    $self->{sub}->{$res->[1]}[1]->($res->[2], $res->[1]);

                                } elsif($action eq 'pmessage') {
                                    $self->{sub}->{$res->[1]}[1]->($res->[3], $res->[2], $res->[1]);

                                } elsif($action eq 'subscribe' || $action eq 'psubscribe') {
                                    $self->{sub_count} = $res->[2];

                                } elsif($action eq 'unsubscribe' || $action eq 'punsubscribe') {
                                    $self->{sub_count} = $res->[2];
                                    $self->{sub}->{$res->[1]}[0]->send;
                                    delete $self->{sub}->{$res->[1]};
                                    $self->all_cv->end;

                                } else {
                                    warn "Unknown pubsub action: $action";
                                }
                            }

                            if($self->{sub_count} || %{$self->{sub}}) {
                                # Carry on reading while we are subscribed
                                $res_cb->();
                            }
                        });
                };

                $res_cb->();
            }

            return $cv;
        };

        for my $queue (@{$self->{connect_queue} || []}) {
            my($cv, @args) = @$queue;
            $self->{cmd_cb}->(@args, $cv);
        }

    };

    return $cv;
}

# For some reason the package based AnyEvent::Handle read type is not supported
# for unshift_read, only push_read, so we register this as a read type rather
# than using the package based form. Maybe AnyEvent could support using the
# package form for this one day.

AnyEvent::Handle::register_read_type(redis => sub {
    my(undef, $cb) = @_;

    sub {
        my($hd) = @_;

        return unless defined $hd->{rbuf};

        if($hd->{rbuf} =~ /^[-+:]/) {
            $hd->{rbuf} =~ s/^([-+:])([^\015\012]*)\015?\012// or return;

            $cb->($2, $1 eq '-');

            return 1;

        } elsif($hd->{rbuf} =~ /^\$/) {
            $hd->{rbuf} =~ s/^\$([-0-9]+)\015?\012// or return;
            my $len = $1;

            if($len < 0) {
                $cb->(undef);
            } elsif($len + 2 <= length $hd->{rbuf}) {
                $cb->(substr $hd->{rbuf}, 0, $len);
                # Remove ending newline
                substr $hd->{rbuf}, 0, $len + 2, "";
            } else {
                $hd->unshift_read (chunk => $len + 2, sub {
                        $cb->(substr $_[1], 0, $len);
                    });
            }

            return 1;

        } elsif($hd->{rbuf} =~ /^\*/) {

            $hd->{rbuf} =~ s/^\*([-0-9]+)\015?\012// or return;
            my $size = $1;
            my @lines;

            my $reader; $reader = sub {
                my($hd) = @_;

                while(@lines < $size) {
                    if($hd->{rbuf} =~ /^([\$\-+:])([^\012\015]+)\015?\012/) {
                        my $type = $1;
                        my $line = $2;

                        if($type =~ /[-+:]/) {
                            $hd->{rbuf} =~ s/^[^\012\015]+\015?\012//;
                            push @lines, $line;
                        } elsif($line < 0) {
                            $hd->{rbuf} =~ s/^[^\012\015]+\015?\012//;
                            push @lines, undef;

                        } elsif(2 + $line <= length $hd->{rbuf}) {
                            $hd->{rbuf} =~ s/^[^\012\015]+\015?\012//;
                            push @lines, substr $hd->{rbuf}, 0, $line, "";
                            $hd->{rbuf} =~ s/^\015?\012//;

                        } else {
                            # Data not buffered, so we need to do this async
                            $hd->unshift_read($reader);
                            return 1;
                        }
                    } elsif($hd->{rbuf} =~ /^\*/) { # Nested
                        
                        $hd->unshift_read(redis => sub {
                                push @lines, $_[0];

                                if(@lines == $size) {
                                    $cb->(\@lines);
                                } else {
                                    $hd->unshift_read($reader);
                                }
                                return 1;
                            });
                        return 1;
                    } else {
                        $hd->unshift_read($reader);
                    }
                }

                if($size < 0 || @lines == $size) {
                    $cb->($size < 0 ? undef : \@lines);
                    return 1;
                }

                return;
            };

            return $reader->($hd);

        } elsif(length $hd->{rbuf}) {
            # remove extra lines
            $hd->{rbuf} =~ s/^\015?\012//g;

            if(length $hd->{rbuf}) {
                # Unknown
                $cb->("Unknown type", 1);
                return 1;
            }
        }

        return;
    }
});

1;
__END__

=encoding utf-8

=for stopwords

=head1 NAME

AnyEvent::Redis - Non-blocking Redis client

=head1 SYNOPSIS

  use AnyEvent::Redis;

  my $redis = AnyEvent::Redis->new(
      host => '127.0.0.1',
      port => 6379,
      on_error => sub { warn @_ },
  );

  # callback based
  $redis->set( 'foo'=> 'bar', sub { warn "SET!" } );
  $redis->get( 'foo', sub { my $value = shift } );

  my ($key, $value) = ('list_key', 123);
  $redis->lpush( $key, $value );
  $redis->lpop( $key, sub { my $value = shift });

  # condvar based
  my $cv = $redis->lpop( $key );
  $cv->cb(sub { my $value = $_[0]->recv });

=head1 DESCRIPTION

AnyEvent::Redis is a non-blocking Redis client using AnyEvent.

=head1 METHODS

All methods supported by your version of Redis should be supported.

=head2 Normal commands

There are two alternative approaches for handling results from commands.

=over 4

=item * L<AnyEvent::CondVar> based:

  my $cv = $redis->command(
    # arguments to command
  );

  $cv->cb(sub {
    my($cv) = @_;
    my($result, $err) = $cv->recv
  });

=item * Callback:

  $redis->command(
    # arguments,
    sub {
      my($result, $err) = @_;
    });

(Callback is a wrapper around the C<$cv> approach.)

=back

=head2 Subscriptions

The subscription methods (C<subscribe> and C<psubscribe>) must be used with a callback:

  my $cv = $redis->subscribe("test", sub {
    my($message, $channel[, $actual_channel]) = @_;
    # ($actual_channel is provided for pattern subscriptions.)
  });

The C<$cv> condition will be met on unsubscribing from the channel.

Due to limitations of the Redis protocol the only valid commands on a
connection with an active subscription are subscribe and unsubscribe commands.

=head2 Common methods

=over 4

=item * get

=item * set

=item * hset

=item * hget

=item * lpush

=item * lpop

=back

The Redis command reference
(L<http://code.google.com/p/redis/wiki/CommandReference>) lists all commands
Redis supports.

=head1 REQUIREMENTS

This requires Redis >= 1.2.

=head1 AUTHOR

Tatsuhiko Miyagawa E<lt>miyagawa@bulknews.netE<gt>

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 SEE ALSO

L<Redis>, L<AnyEvent>

=cut

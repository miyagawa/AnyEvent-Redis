package AnyEvent::Redis;

use strict;
use 5.008_001;
our $VERSION = '0.24';

use constant DEBUG => $ENV{ANYEVENT_REDIS_DEBUG};
use AnyEvent;
use AnyEvent::Handle;
use AnyEvent::Socket;
use AnyEvent::Redis::Protocol;
use Carp qw( croak confess );
use Encode ();
use Scalar::Util qw(weaken);

our $AUTOLOAD;

sub new {
    my ($class, %args) = @_;

    my $host = delete $args{host} || '127.0.0.1';
    my $port = delete $args{port} || 6379;

    if (my $encoding = $args{encoding}) {
        $args{encoding} = Encode::find_encoding($encoding);
        croak qq{Encoding "$encoding" not found} unless ref $args{encoding};
    }

    bless {
        host => $host,
        port => $port,
        pending_cvs => [],
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
    $self->{all_cv} ||= AE::cv;
}

sub cleanup {
    my $self = shift;
    delete $self->{cmd_cb};
    delete $self->{sock};
    $self->{on_error}->(@_) if $self->{on_error};
    $self->{on_cleanup}->(@_) if $self->{on_cleanup};
    for (splice(@{$self->{pending_cvs}}),
         splice(@{$self->{multi_cvs} || []}))
    {
        eval { $_->croak(@_) };
        warn "Exception in cleanup callback (ignored): $@" if $@;
    }
    return;
}

sub connect {
    my $self = shift;

    my $cv;
    if (@_) {
        $cv = pop if UNIVERSAL::isa($_[-1], 'AnyEvent::CondVar');
        $cv ||= AE::cv;
        push @{$self->{connect_queue}}, [ $cv, @_ ];
    }

    return $cv if $self->{sock};
    weaken $self;

    $self->{sock} = tcp_connect $self->{host}, $self->{port}, sub {
        my $fh = shift
            or do {
              my $err = "Can't connect Redis server: $!";
              $self->cleanup($err);
              eval { $cv->croak($err) };
              warn "Exception in connect failure callback (ignored): $@" if $@;
              return
            };

        binmode $fh; # ensure bytes until we decode

        my $hd = AnyEvent::Handle->new(
            fh => $fh,
            on_error => sub { $_[0]->destroy;
                              $self->cleanup($_[2]) if $_[1];
                          },
            on_eof   => sub { $_[0]->destroy;
                              $self->cleanup('connection closed');
                          },
            encoding => $self->{encoding},
        );

        $self->{cmd_cb} = sub {
            my $command = lc shift;
            my $is_pubsub    = $command =~ /^p?(?:un)?subscribe\z/;
            my $is_subscribe = $command =~ /^p?subscribe\z/;


            # Are we already subscribed to anything?
            if ($self->{sub} && %{$self->{sub}}) {
                croak "Use of non-pubsub command during pubsub session may result in unexpected behaviour"
                  unless $is_pubsub;
            }
            # Are we already in a transaction?
            if ($self->{multi_write}) {
                croak "Use of pubsub or multi command in transaction is not supported"
                  if $is_pubsub || $command eq 'multi';
            } else {
                croak "Can't 'exec' a transaction because none is pending"
                  if $command eq 'exec';
            }

            my ($cv, $cb);
            if (@_) {
                $cv = pop if ref $_[-1] && UNIVERSAL::isa($_[-1], 'AnyEvent::CondVar');
                $cb = pop if ref $_[-1] eq 'CODE';
            }
            $cv ||= AE::cv;
            croak "Must provide a CODE reference for subscriptions" if $is_subscribe && !$cb;

            my $send = join("\r\n",
                  "*" . (1 + @_),
                  map { ('$' . length $_ => $_) }
                        (uc($command), map { $self->{encoding} && length($_)
                                             ? $self->{encoding}->encode($_)
                                             : $_ } @_))
                . "\r\n";

            warn $send if DEBUG;

            # $self is weakened to avoid leaks, hold on to a strong copy
            # controlled via a CV.
            my $cmd_cv = AE::cv;
            $cmd_cv->cb(sub {
                my $strong_self = $self;
              });

            # pubsub is very different - get it out of the way first

            if ($is_pubsub) {

                $hd->push_write($send);

                my $already = $self->{sub} && %{$self->{sub}};

                if ($is_subscribe) {
                    $self->{sub}->{$_} ||= [$cv, $cb] for @_;
                }

                if (!$already && @_) {
                    my $res_cb; $res_cb = sub {
                        $hd->push_read("AnyEvent::Redis::Protocol" => sub {
                            my ($res, $err) = @_;

                            if (ref $res) {
                                my $action = lc $res->[0];
                                warn "$action $res->[1]" if DEBUG;

                                if ($action eq 'message') {
                                    $self->{sub}->{$res->[1]}[1]->($res->[2], $res->[1]);

                                } elsif ($action eq 'pmessage') {
                                    $self->{sub}->{$res->[1]}[1]->($res->[3], $res->[2], $res->[1]);

                                } elsif ($action eq 'subscribe' || $action eq 'psubscribe') {
                                    $self->{sub_count} = $res->[2];

                                } elsif ($action eq 'unsubscribe' || $action eq 'punsubscribe') {
                                    $self->{sub_count} = $res->[2];
                                    eval { $self->{sub}->{$res->[1]}[0]->send };
                                    warn "Exception in callback (ignored): $@" if $@;
                                    delete $self->{sub}->{$res->[1]};
                                    $self->all_cv->end;
                                    $cmd_cv->send;

                                } else {
                                    warn "Unknown pubsub action: $action";
                                }
                            }

                            if ($self->{sub_count} || %{$self->{sub}}) {
                                # Carry on reading while we are subscribed
                                $res_cb->();
                            }
                        });
                    };

                    $res_cb->();
                }

                return $cv;
            }

            # non-pubsub from here on out

            $cv->cb(sub {
                my $cv = shift;
                local $@;
                eval {
                    my $res = $cv->recv;
                    $cb->($res);
                };
                if ($@) {
                    ($self->{on_error} || sub { die @_ })->(my $err = $@);
                }
            }) if $cb;

            $self->all_cv->begin;
            push @{$self->{pending_cvs}}, $cv;

            $hd->push_write($send);

            if ($command eq 'exec') {

                # at end of transaction, expect bulk reply possibly including errors
                $hd->push_read("AnyEvent::Redis::Protocol" => sub {
                    my ($res, $err) = @_;

                    $self->_expect($cv);

                    my @mcvs = splice @{$self->{multi_cvs}};

                    if ($err || ref($res) ne 'ARRAY') {
                        for ($cv, @mcvs) {
                            eval { $_->croak($res, 1) };
                            warn "Exception in callback (ignored): $@" if $@;
                        }
                    } else {
                        for my $i (0 .. $#mcvs) {
                            my $r = $res->[$i];
                            eval {
                                ref($r) && UNIVERSAL::isa($r, 'AnyEvent::Redis::Error')
                                  ? $mcvs[$i]->croak($$r)
                                  : $mcvs[$i]->send($r);
                            };
                            warn "Exception in callback (ignored): $@" if $@;
                        }
                        eval { $cv->send($res) };
                        warn "Exception in callback (ignored): $@" if $@;
                    }

                    $self->all_cv->end;
                    $cmd_cv->send;
                });

                delete $self->{multi_write};

            } elsif ($self->{multi_write}) {

                # in transaction, expect only "QUEUED"
                $hd->push_read("AnyEvent::Redis::Protocol" => sub {
                    my ($res, $err) = @_;

                    $self->_expect($cv);

                    if (!$err && $res eq 'QUEUED') {
                        push @{$self->{multi_cvs}}, $cv;
                    }
                    else {
                        eval { $cv->croak($res) };
                        warn "Exception in callback (ignored): $@" if $@;
                    }

                    $self->all_cv->end;
                    $cmd_cv->send;
                });

            } else {

                $hd->push_read("AnyEvent::Redis::Protocol" => sub {
                    my ($res, $err) = @_;

                    $self->_expect($cv);

                    if ($command eq 'info') {
                        $res = { map { split /:/, $_, 2 } grep !/^#/, split /\r\n/, $res };
                    } elsif ($command eq 'keys' && !ref $res) {
                        # Older versions of Redis (1.2) need this
                        $res = [split / /, $res];
                    }

                    eval { $err ? $cv->croak($res) : $cv->send($res) };
                    warn "Exception in callback (ignored): $@" if $@;

                    $self->all_cv->end;
                    $cmd_cv->send;
                });

                $self->{multi_write} = 1 if $command eq 'multi';

            }

            return $cv;
        };

        my $queue = delete $self->{connect_queue} || [];
        for my $command (@$queue) {
            my($cv, @args) = @$command;
            $self->{cmd_cb}->(@args, $cv);
        }

    };

    return $cv;
}

sub _expect {
    my ($self, $cv) = @_;
    my $p = shift @{$self->{pending_cvs} || []};
    $p && $p == $cv or confess "BUG: mismatched CVs";
}

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
      encoding => 'utf8',
      on_error => sub { warn @_ },
      on_cleanup => sub { warn "Connection closed: @_" },
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

AnyEvent::Redis is a non-blocking (event-driven) Redis client.

This module is an AnyEvent user; you must install and use a supported event loop.

=head1 ESTABLISHING A CONNECTION

To create a new connection, use the new() method with the following attributes:

=over

=item host => <HOSTNAME>

B<Required.>  The hostname or literal address of the server.  

=item port => <PORT>

Optional.  The server port.

=item encoding => <ENCODING>

Optional.  Encode and decode data (when storing and retrieving, respectively)
according to I<ENCODING> (C<"utf8"> is recommended or see L<Encode::Supported>
for details on possible I<ENCODING> values).

Omit if you intend to handle raw binary data with this connection.

=item on_error => $cb->($errmsg)

Optional.  Callback that will be fired if a connection or database-level error
occurs.  The error message will be passed to the callback as the sole argument.

=item on_cleanup => $cb->($errmsg)

Optional.  Callback that will be fired if a connection error occurs.  The
error message will be passed to the callback as the sole argument.  After
this callback, errors will be reported for all outstanding requests.

=back

=head1 METHODS

All methods supported by your version of Redis should be supported.

=head2 Normal commands

There are two alternative approaches for handling results from commands:

=over 4

=item * L<AnyEvent::CondVar> based:

  my $cv = $redis->command(
    # arguments to command
  );

  # Then...
  my $res;
  eval { 
      # Could die()
      $res = $cv->recv;
  }; 
  warn $@ if $@;

  # or...
  $cv->cb(sub {
    my ($cv) = @_;
    my ($result, $err) = $cv->recv
  });


=item * Callback:

  $redis->command(
    # arguments,
    sub {
      my ($result, $err) = @_;
    });

(Callback is a wrapper around the C<$cv> approach.)

=back

=head2 Transactions (MULTI/EXEC)

Redis transactions begin with a "multi" command and end with an "exec"
command.  Commands in between are not executed immediately when they're
sent.  On receipt of the "exec", the server executes all the saved commands
atomically, and returns all their results as one bulk reply.

After a transaction is finished, results for each individual command are
reported in the usual way.  Thus, by the time any of these callbacks is
called, the entire transaction is finished for better or worse.

Results of the "exec" (containing all the other results) will be returned as
an array reference containing all of the individual results.  This may in
some cases make callbacks on the individual commands unnecessary, or vice
versa.  In this bulk reply, errors reported for each individual command are
represented by objects of class C<AnyEvent::Redis::Error>, which will
respond to a C<< ->message >> method call with that error message.

It is not permitted to nest transactions.  This module does not permit
subscription-related commands in a transaction.

=head2 Subscriptions

The subscription methods (C<subscribe> and C<psubscribe>) must be used with a callback:

  my $cv = $redis->subscribe("test", sub {
    my ($message, $channel[, $actual_channel]) = @_;
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

The Redis command reference (L<http://redis.io/commands>) lists all commands
Redis supports.

=head1 REQUIREMENTS

This requires Redis >= 1.2.

=head1 COPYRIGHT

Tatsuhiko Miyagawa E<lt>miyagawa@bulknews.netE<gt> 2009-

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHORS

Tatsuhiko Miyagawa

David Leadbeater

Chia-liang Kao

franck cuny

Lee Aylward

Joshua Barratt

Jeremy Zawodny

Leon Brocard

Michael S. Fischer

Chip Salzenberg

=head1 SEE ALSO

L<Redis>, L<AnyEvent>

=cut

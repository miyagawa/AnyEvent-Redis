package AnyEvent::Redis;

use strict;
use 5.008_001;
our $VERSION = '0.23_01';

use constant DEBUG => $ENV{ANYEVENT_REDIS_DEBUG};
use AnyEvent;
use AnyEvent::Handle;
use AnyEvent::Socket;
use AnyEvent::Redis::Protocol;
use Try::Tiny;
use Carp qw(croak);
use Encode ();

our $AUTOLOAD;

sub new {
    my($class, %args) = @_;

    my $host = delete $args{host} || '127.0.0.1';
    my $port = delete $args{port} || 6379;

    if (my $encoding = $args{encoding}) {
        $args{encoding} = Encode::find_encoding($encoding);
        croak qq{Encoding "$encoding" not found} unless ref $args{encoding};
    }

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
    $self->{on_error}->(@_) if $self->{on_error};
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
              my $err = "Can't connect Redis server: $!";
              $self->cleanup($err);
              $cv->croak($err);
              return
            };

        binmode $fh; # ensure bytes until we decode

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
            encoding => $self->{encoding},
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
                  map { ('$' . length $_ => $_) }
                        (uc($command), map { $self->{encoding} && $_
                                             ? $self->{encoding}->encode($_)
                                             : $_ } @_))
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

                $hd->push_read("AnyEvent::Redis::Protocol" => sub {
                        my($res, $err) = @_;

                        if($command eq 'info') {
                          $res = { map { split /:/, $_, 2 } split /\r\n/, $res };
                        } elsif($command eq 'keys' && !ref $res) {
                          # Older versions of Redis (1.2) need this
                          $res = [split / /, $res];
                        }

                        $self->all_cv->end;
                        $err ? $cv->croak($res) : $cv->send($res);
                });

            } else {
                croak "Must provide a CODE reference for subscriptions" unless $cb;

                # Remember subscriptions
                $self->{sub}->{$_} ||= [$cv, $cb] for @_;

                my $res_cb; $res_cb = sub {

                    $hd->push_read("AnyEvent::Redis::Protocol" => sub {
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

=head1 SEE ALSO

L<Redis>, L<AnyEvent>

=cut

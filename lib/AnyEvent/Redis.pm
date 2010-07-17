package AnyEvent::Redis;

use strict;
use 5.008_001;
our $VERSION = '0.10';

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
            on_error => sub { $_[0]->destroy },
            on_eof   => sub { $_[0]->destroy },
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

            if ($command !~ /^p?subscribe$/i) {

                $cv->cb(sub {
                    my $cv = shift;
                    try {
                        my $res = $cv->recv;
                        $cb->($res);
                    } catch {
                        ($self->{on_error} || sub { die @_ })->($_);
                    }
                }) if $cb;

                $hd->push_read(ref $self => sub {
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
                $self->{sub}->{$_} = [$cv, $cb] for @_;

                my $res_cb; $res_cb = sub {

                    $hd->push_read(ref $self => sub {
                            my($res, $err) = @_;

                            if(ref $res) {
                                my $action = $res->[0];

                                if($action eq 'message') {
                                    warn "Message on $res->[1]" if DEBUG;
                                    $self->{sub}->{$res->[1]}[1]->($res->[2], $res->[1]);

                                } elsif($action eq 'pmessage') {
                                    warn "Pmessage on $res->[1] ($res->[2])" if DEBUG;
                                    $self->{sub}->{$res->[1]}[1]->($res->[3], $res->[2], $res->[1]);

                                } elsif($action eq 'subscribe' || $action eq 'psubscribe') {
                                    warn "Subscribe to $res->[1]" if DEBUG;
                                    $self->{sub_count} = $res->[2];

                                } elsif($action eq 'unsubscribe' || $action eq 'punsubscribe') {
                                    warn "Unsubscribe from $res->[1]" if DEBUG;

                                    $self->{sub_count} = $res->[2];
                                    $self->{sub}->{$res->[1]}[0]->send;
                                    delete $self->{sub}->{$res->[1]};

                                } else {
                                    warn "Unknown pubsub action: $action";
                                }
                            }

                            if($self->{sub_count}) {
                                # Carry on reading while we are subscribed
                                $res_cb->();
                            } else {
                                $self->all_cv->end;
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

sub anyevent_read_type {
    my(undef, $cb) = @_;

    sub {
        my($hd) = @_;

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
                    if($hd->{rbuf} =~ /^([\$:])([-0-9]+)\015?\012/) {
                        my $type = $1;
                        my $len = $2;

                        if($type eq ':') {
                            $hd->{rbuf} =~ s/^[^\012\015]+\015?\012//;
                            push @lines, $len;
                        } elsif($len < 0) {
                            $hd->{rbuf} =~ s/^[^\012\015]+\015?\012//;
                            push @lines, undef;

                        } elsif(2 + $len <= length $hd->{rbuf}) {
                            $hd->{rbuf} =~ s/^[^\012\015]+\015?\012//;
                            push @lines, substr $hd->{rbuf}, 0, $len, "";
                            $hd->{rbuf} =~ s/^\015?\012//;

                        } else {
                            # Data not buffered, so we need to do this async
                            $hd->unshift_read($reader);
                            last;
                        }
                    } else {
                        $hd->unshift_read($reader);
                        last;
                    }
                }

                $cb->(\@lines) if @lines == $size;
            };

            $reader->($hd);

            return 1;

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
      on_error => sub { warn @_ },
  );

  # callback based
  $redis->set( 'foo'=> 'bar', sub { warn "SET!" } );
  $redis->get( 'foo', sub { my $value = shift } );

  $redis->lpush( $key, $value );
  $redis->lpop( $key, sub { my $value = shift });

  # condvar based
  my $cv = $redis->lpop( $key );
  $cv->cb(sub { my $value = $_[0]->recv });

=head1 DESCRIPTION

AnyEvent::Redis is a non-blocking Redis client using AnyEvent.

=head1 AUTHOR

Tatsuhiko Miyagawa E<lt>miyagawa@bulknews.netE<gt>

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 SEE ALSO

L<Redis> L<AnyEvent>

=cut

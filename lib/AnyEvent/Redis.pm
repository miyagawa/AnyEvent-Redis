package AnyEvent::Redis;

use strict;
use 5.008_001;
our $VERSION = '0.12';

use constant DEBUG => $ENV{ANYEVENT_REDIS_DEBUG};
use AnyEvent;
use AnyEvent::Handle;
use AnyEvent::Socket;
use Try::Tiny;

our $AUTOLOAD;

my %bulk_command = map { $_ => 1 }
    qw( set setnx rpush lpush lset lrem sadd srem sismember echo getset smove zadd zrem zscore zincrby append hexists hset hsetnx hget hmget hmset hdel);

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
            or die "Can't connect Redis server: $!";

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

            my $cv_send = sub {
                my $cv = shift;
                my($res, $err) = @_;
                $self->all_cv->end;
                $err ? $cv->croak($res) : $cv->send($res);
            };

            my $send;
            if ( defined $bulk_command{$command} ) {
                my $value = pop;
                $value = '' if ! defined $value;
                $send = uc($command)
                      . ' '
                      . join(' ', @_)
                      . ' '
                      . length( $value )
                      . "\r\n$value\r\n";
            } else {
                $send = uc($command)
                    . ' '
                    . join(' ', @_)
                    . "\r\n";
            }

            warn $send if DEBUG;

            $cv ||= AE::cv;
            $cv->cb(sub {
                my $cv = shift;
                try {
                    my $res = $cv->recv;
                    $cb->($res);
                } catch {
                    ($self->{on_error} || sub { die @_ })->($_);
                }
            }) if $cb;

            $hd->push_write($send);
            $hd->push_read(line => _decoder($send, $cv, $cv_send, $command));

            return $cv;
        };

        for my $queue (@{$self->{connect_queue} || []}) {
            my($cv, @args) = @$queue;
            $self->{cmd_cb}->(@args, $cv);
        }
    };

    return $cv;
}

sub _decoder {
    my ($send, $cv, $cv_send, $command) = @_;
    sub {
        my($hd, $result) = @_;
        warn "got line <$result> for command [$send]" if DEBUG;
        my $type = substr $result, 0, 1;
        $result =~ s/^.//;

        if ( $type eq '-' ) {
            $cv_send->($cv, $result, 1);
        } elsif ( $type eq '+' ) {
            $cv_send->($cv, $result);
        } elsif ( $type eq '$' ) {
            if ($result < 0) {
                return $cv_send->($cv, undef);
            }
            $hd->unshift_read(chunk => $result + 2, sub {
                        my($hd, $chunk) = @_;
                        $chunk =~ s/\r\n$//;
                        warn "chunk <$chunk>" if DEBUG;
                        if ($command eq 'info') {
                            my %info = map { split /:/, $_, 2 } split /\r\n/, $chunk;
                            $cv_send->($cv, \%info);
                        } elsif ($command eq 'keys') {
                            my @keys = split /\s+/, $chunk;
                            $cv_send->($cv, \@keys);
                        } else {
                            $cv_send->($cv, $chunk);
                        }
                    });
        } elsif ( $type eq '*' ) {
            my $size = $result;
            warn "size is $size" if DEBUG;
            if ($result < 0) {
                return $cv_send->($cv, undef);
            } elsif ($result == 0) {
                return $cv_send->($cv, []);
            }
            my @lines;
            for (1..$size) {
                $hd->unshift_read(line => _decoder
                                      ("recursion $_ of $send", undef,
                                       sub { my (undef, $result) = @_;
                                             push @lines, $result;
                                             if (@lines >= $size) {
                                                 $cv_send->($cv, \@lines);
                                             }
                                         },
                                       $command
                                   ));
            }
        } elsif ( $type eq ':' ) {
            $cv_send->($cv, $result);
        } else {
            $cv_send->($cv, "Unknown type $type", 1);
        }
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

  my ($key, $value) = ('list_key', 123);
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

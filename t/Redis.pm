package t::Redis;
use strict;
use Test::TCP;
use Test::More;
use AnyEvent::Redis;

use base qw(Exporter);
our @EXPORT = qw(test_redis);

sub test_redis(&) {
    my $cb = shift;

    chomp(my $redis_server = `which redis-server`);
    unless ($redis_server && -e $redis_server && -x _) {
        plan skip_all => 'redis-server not found in your PATH';
    }

    test_tcp
        server => sub {
            my $port = shift;
            rewrite_redis_conf($port);
            exec "redis-server", "t/redis.conf";
        },
        client => sub {
            my $port = shift;
            my $r = AnyEvent::Redis->new("127.0.0.1:$port");
            $cb->($r);
        };
}

sub rewrite_redis_conf {
    my $port = shift;

    open my $in, "<", "t/redis.conf.base" or die $!;
    open my $out, ">", "t/redis.conf" or die $!;

    while (<$in>) {
        s/__PORT__/$port/;
        print $out $_;
    }
}

END { unlink "t/redis.conf" }

1;


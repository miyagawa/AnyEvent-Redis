use strict;
use Test::More;
use t::Redis;

test_redis {
    my ($r, $port) = @_;

    # make a new redis object using wrong port
    my $redis = AnyEvent::Redis->new(host => "127.0.0.1", port => Test::TCP::empty_port());

    # should fail
    eval { $redis->info->recv; };
    ok $@, "got exception from error";

    # fix the port and try again
    $redis->{port} = $port;

    my $info = $redis->info->recv;

    ok $info->{redis_version}, "got response after reconnect";
};

done_testing;

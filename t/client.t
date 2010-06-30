use strict;
use Test::More;
use t::Redis;

test_redis {
    my $r = shift;

    $r->all_cv->begin(sub { $_[0]->send });

    my $info = $r->info->recv;
    is ref $info, 'HASH';
    ok $info->{redis_version};

    $r->set("foo", "bar", sub { pass "SET foo" });
    $r->get("foo", sub { is $_[0], "bar" });

    $r->lpush("list", "bar");
    $r->lpush("list", "baz");

    is $r->lpop("list")->recv, 'baz';
    is $r->lpop("list")->recv, 'bar';

    $r->set("prefix.bar", "test", sub { $r->get("prefix.bar", sub { is $_[0], "test" }) });
    $r->set("prefix.baz", "test");

    $r->mget(
        "prefix.bar",
        "prefix.baz",
        "foo",
        sub {
            my $res = shift;
            is $res->[0], "test";
            is $res->[2], "bar";
        }
    );
    $r->mget(
        1, 2, 3,
        sub {
            my $res = shift;
            map { ok !$_ } @$res;
        }
    );
    $r->mget(
        1,
        "prefix.baz",
        "barbaz",
        sub {
            my $res = shift;
            ok !$res->[0];
            is $res->[1], "test";
            ok !$res->[2];
        }
    );

    if ($info->{redis_version} =~ /^2/) {
        $r->hset("hash", "foo", "bar", sub { is $_[0], 1 });
        $r->hset("hash", "baz", "foo");
        $r->hget("hash", "foo", sub { is $_[0], "bar" });
        $r->hmget("hash", "foo", "baz",
            sub { my $res = shift; is $res->[0], "bar"; is $res->[1], "foo" });
        $r->hdel("hash", "foo", sub { is $_[0], 1 });
        $r->hmset("hash", "foo", 1, "bar", 2, "baz", 3,
            sub { my $res = shift; ok $res });
        $r->hincrby("hash", "foo", 2, sub { my $res = shift; is $res, 3 });
        $r->hkeys("hash", sub {my $res = shift; is scalar @$res, 3;});
        $r->hvals("hash", sub {my $res = shift; is scalar @$res, 3;});
        $r->hgetall("hash", sub {my $res = shift; is scalar @$res, 6;});
        for (qw/foo bar baz/) {
            $r->hdel("hash", $_);
        }
    }

    $r->keys('prefix.*', sub { my $keys = shift; is ref $keys, 'ARRAY'; is @$keys, 2 });

    my $cv = $r->get("nonx");
    is $cv->recv, undef;

    my $err;
    $r->{on_error} = sub { $err = shift };
    $r->bogus("foo", sub { });

    $r->all_cv->end;
    $r->all_cv->recv;

    like $err, qr/ERR unknown command/;
};

done_testing;



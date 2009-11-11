use strict;
use Test::More;
use t::Redis;

test_redis {
    my $r = shift;

    $r->all_cv->begin;

    my $info = $r->info->recv;
    is ref $info, 'HASH';
    ok $info->{redis_version};

    $r->set("foo", "bar", sub { pass "SET foo" });
    $r->get("foo", sub { is $_[0], "bar" });

    $r->lpush("list", "bar");
    $r->lpush("list", "baz");

    is $r->lpop("list")->recv, 'baz';
    is $r->lpop("list")->recv, 'bar';

    $r->set("prefix.bar", "test", sub { $r->get("prefix.bar", sub { warn @_; is $_[0], "test" }) });
    $r->set("prefix.baz", "test");

    $r->keys('prefix.*', sub { my $keys = shift; is ref $keys, 'ARRAY'; is @$keys, 2 });

    my $cv = $r->get("nonx");
    is $cv->recv, undef;

    $r->all_cv->end;
};

done_testing;



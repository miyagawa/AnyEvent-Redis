use strict;
use Test::More;
use t::Redis;
use utf8;

test_redis {
    my $r = shift;

    $r->all_cv->begin(sub { $_[0]->send });

    my $info = $r->info->recv;
    is ref $info, 'HASH';
    ok $info->{redis_version};

    $r->set("foo", "ba∫∫", sub { pass "SET foo" });
    $r->get("foo", sub { 
            is length $_[0], 8, "stored 8 bytes";
            utf8::decode($_[0]); 
            is $_[0], "ba∫∫" 
    });

    $r->all_cv->end;
    $r->all_cv->recv;
};

done_testing;



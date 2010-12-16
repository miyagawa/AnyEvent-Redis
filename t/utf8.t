use strict;
use Test::More;
use t::Redis;

test_redis {
    my $r = shift;
    $r->{encoding} = 'utf8';

    $r->all_cv->begin(sub { $_[0]->send });

    my $info = $r->info->recv;
    is ref $info, 'HASH';
    ok $info->{redis_version};

    { 
        use utf8;
        my $key = "ロジ プロセスド";
        my $val = "लचकनहि";
        $r->set($key => $val, sub { pass "SET literal UTF-8 key/value" });
        $r->get($key, sub { 
                is $_[0], $val, "GET literal UTF-8 key/value";
        });
    }

    my $key = "\x{a3}\x{acd}";
    my $val = "\x{90e}\x{60a}";
    $r->set($key => $val, sub { pass "SET escaped UTF-8 key/value" });
    $r->get($key, sub { 
            is $_[0], $val, "GET escaped UTF-8 key/value";
    });

    my $key = "V\x{f6}gel";
    my $val = "Gr\x{fc}n";
    $r->set($key => $val, sub { pass "SET escaped Latin-1 key/value" });
    $r->get($key, sub { 
            warn $val;
            is $_[0], $val, "GET escaped Latin-1 key/value";
    });

    $r->all_cv->end;
    $r->all_cv->recv;
};

done_testing;

# vim:fileencoding=utf8

use strict;
use Test::More;
use t::Redis;

test_redis {
    my $r = shift;

    $r->all_cv->begin(sub { $_[0]->send });

    my $info = $r->info->recv;
    is ref $info, 'HASH';
    ok $info->{redis_version};

    { 
        use utf8;
        my $key = "ロジ プロセスド";
        my $val = "लचकनहि";
        $r->set($key => $val, sub { pass "SET literal key" });
        $r->get($key, sub { 
                is $_[0], $val;
        });
    }

    my $key = "\x{a3}\x{acd}";
    my $val = "\x{90e}\x{60a}";
    $r->set($key => $val, sub { pass "SET escaped key" });
    $r->get($key, sub { 
            is $_[0], $val;
    });

    $r->all_cv->end;
    $r->all_cv->recv;

    eval { AnyEvent::Redis->new(encoding => "invalid") };
    like $@, qr/Encoding "invalid" not found/;

}
# Extra args for the constructor
  { encoding => "utf8" };

done_testing;

# vim:fileencoding=utf8

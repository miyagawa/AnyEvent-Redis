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
        $r->set($key => $val, sub { pass "SET literal UTF-8 key/value" });
        $r->get($key, sub { 
                is $_[0], $val, "GET literal UTF-8 key/value";
        });
    }

    my $utf8_key = "\x{a3}\x{acd}";
    my $utf8_val = "\x{90e}\x{60a}";
    $r->set($utf8_key => $utf8_val, sub { 
            pass "SET escaped UTF-8 key/value" 
    });
    $r->get($utf8_key, sub { 
            is $_[0], $utf8_val, "GET escaped UTF-8 key/value";
    });

    my $latin1_key = "V\x{f6}gel";
    my $latin1_val = "Gr\x{fc}n";
    $r->set($latin1_key => $latin1_val, sub { 
            pass "SET escaped Latin-1 key/value" 
    });
    $r->get($latin1_key, sub { 
            is $_[0], $latin1_val, "GET escaped Latin-1 key/value";
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

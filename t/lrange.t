use strict;
use Test::More;
use t::Redis;

test_redis {
    my $r = shift;
    $r->del("lrange_test_1");
    $r->del("lrange_test_2");

    for (1 .. 20) {
      $r->rpush("lrange_test_1", join "\n", map {"A" x 100} (0 .. 3));
      $r->rpush("lrange_test_2", join "\n", map {"B" x 100} (0 .. 3));
    }
    for (1 .. 20) {
      $r->rpush("lrange_test_1", join "\n", map {"C" x 100} (0 .. 3));
      $r->rpush("lrange_test_2", join "\n", map {"D" x 100} (0 .. 3));
    }

    $r->all_cv->begin;
    $r->lrange("lrange_test_1", 0, 19, sub {
        my $value = join "\n", map {"A" x 100} (0 .. 3);
        is scalar @{$_[0]}, 20, "correct length lrange_test_1 0 19";
        is $_[0][-1], $value, "correct end value lrange_test_1 0 19";
        is $_[0][0], $value, "correct start value lrange_test_1 0 19";
        $r->all_cv->end;
    });

    $r->all_cv->begin;
    $r->lrange("lrange_test_2", 0, 19, sub {
        my $value = join "\n", map {"B" x 100} (0 .. 3);
        is scalar @{$_[0]}, 20, "correct length lrange_test_2 0 19";
        is $_[0][-1], $value, "correct end value lrange_test_2 0 19";
        is $_[0][0], $value, "correct start value lrange_test_2 0 19";
        $r->all_cv->end;
    });

    $r->all_cv->begin;
    $r->lrange("lrange_test_1", 20, 39, sub {
        my $value = join "\n", map {"C" x 100} (0 .. 3);
        is scalar @{$_[0]}, 20, "correct length lrange_test_1 20 39";
        is $_[0][-1], $value, "correct end value lrange_test_1 20 39";
        is $_[0][0], $value, "correct start value lrange_test_1 20 39";
        $r->del("lrange_test_2", sub { $r->all_cv->end });
    });

    $r->all_cv->begin;
    $r->lrange("lrange_test_2", 20, 39, sub {
        my $value = join "\n", map {"D" x 100} (0 .. 3);
        is scalar @{$_[0]}, 20, "correct length lrange_test_2 20 39";
        is $_[0][-1], $value, "correct end value lrange_test_2 20 39";
        is $_[0][0], $value, "correct start value lrange_test_2 20 39";
        $r->del("lrange_test_2", sub { $r->all_cv->end });
    });

    $r->all_cv->recv;
};
done_testing;

use strict;
use Test::More;
use t::Redis;

test_redis {
    my $r = shift;

    for my $n(1 .. 10) {
      my $key1 = "lrange_test1_$n";
      my $key2 = "lrange_test2_$n";

      $r->del($key1);
      $r->del($key2);

      for (1 .. 20) {
        $r->rpush($key1, join "\n", map {"A" x 100} (0 .. 3));
        $r->rpush($key2, join "\n", map {"B" x 100} (0 .. 3));
      }
      for (1 .. 20) {
        $r->rpush($key1, join "\n", map {"C" x 100} (0 .. 3));
        $r->rpush($key2, join "\n", map {"D" x 100} (0 .. 3));
      }

      $r->all_cv->begin;
      $r->lrange($key1, 0, 19, sub {
          my $value = join "\n", map {"A" x 100} (0 .. 3);
          is scalar @{$_[0]}, 20, "correct length $key1 0 19";
          is $_[0][-1], $value, "correct end value $key1 0 19";
          is $_[0][0], $value, "correct start value $key1 0 19";
          $r->all_cv->end;
      });

      $r->all_cv->begin;
      $r->lrange($key2, 0, 19, sub {
          my $value = join "\n", map {"B" x 100} (0 .. 3);
          is scalar @{$_[0]}, 20, "correct length $key2 0 19";
          is $_[0][-1], $value, "correct end value $key2 0 19";
          is $_[0][0], $value, "correct start value $key2 0 19";
          $r->all_cv->end;
      });

      $r->all_cv->begin;
      $r->lrange($key1, 20, 39, sub {
          my $value = join "\n", map {"C" x 100} (0 .. 3);
          is scalar @{$_[0]}, 20, "correct length $key1 20 39";
          is $_[0][-1], $value, "correct end value $key1 20 39";
          is $_[0][0], $value, "correct start value $key1 20 39";
          $r->del($key1, sub { $r->all_cv->end });
      });

      $r->all_cv->begin;
      $r->lrange($key2, 20, 39, sub {
          my $value = join "\n", map {"D" x 100} (0 .. 3);
          is scalar @{$_[0]}, 20, "correct length $key2 20 39";
          is $_[0][-1], $value, "correct end value $key2 20 39";
          is $_[0][0], $value, "correct start value $key2 20 39";
          $r->del($key2, sub { $r->all_cv->end });
      });
    }

    $r->all_cv->recv;
};
done_testing;

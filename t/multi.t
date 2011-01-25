use strict;
use Test::More;
use t::Redis;

test_redis {
    my $r = shift;
    $r->all_cv->begin(sub { $_[0]->send });

    if($r->info->recv->{redis_version} ge "1.3.0") {

      # Multi/exec with no commands
      $r->multi->recv;
      $r->exec(sub {
          ok 0 == @{$_[0]};
        });

      # Simple multi/exec
      my $nsets = 0;
      $r->multi;
      $r->set("foo$_" => "bar$_", sub { ++$nsets }) for 1 .. 10;
      $r->exec(sub {
          ok 10 == grep /^OK$/, @{$_[0]};
	  ok 10 == $nsets;
        });

      # Complex multi/exec

      my $y;
      my $get5 = sub { ok 5 == grep { $y++; /^bar$y$/ } @{$_[0]} };
      $r->multi;
      $r->mget((map { "foo$_" } 1 ..  5), $get5);
      $r->mget((map { "foo$_" } 6 .. 10), $get5);
      $r->exec(sub {
          my $x = 0;
          ok 10 == grep { $x++; /^bar$x$/ } map { @$_ } @{$_[0]};
        });

      $r->all_cv->end;
      $r->all_cv->recv;
      done_testing;

    } else {
      plan skip_all => "No support for MULTI in this server version";
    }
};

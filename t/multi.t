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
      $r->multi;
      $r->set("foo$_" => "bar$_") for 1 .. 10;
      $r->exec(sub {
          ok 10 == grep /^OK$/, @{$_[0]};
        });

      # Complex multi/exec

      $r->multi;
      $r->mget(map { "foo$_" } 1 .. 5);
      $r->mget(map { "foo$_" } 6 .. 10);
      $r->exec(sub {
          my $x = 0;
          ok 10 == grep { $x++; /^bar$x$/ } map { @$_ } @{$_[0]};
        });

      $r->all_cv->end;
      $r->all_cv->recv;
      done_testing;

    } else {
      # Test::TCP needs to cleanup, plan skip_all calls exit(0), see:
      # https://rt.cpan.org/Ticket/Display.html?id=60657
      print "1..0 # SKIP No support for MULTI in this server version\n";
      # Hoop jumping to output our own TAP...
      $Test::Builder::Test->no_ending(1);
      $Test::Builder::Test->no_header(1);
    }
};

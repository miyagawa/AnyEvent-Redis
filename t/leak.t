use strict;
use Test::More;
use t::Redis;

BEGIN {
  eval q{use Test::LeakTrace};
  plan skip_all => "Test::LeakTrace required" if $@;
}

test_redis {
    my $r = shift;
    my $port = shift;
    $r->all_cv->begin(sub { $_[0]->send });

    if($r->info->recv->{redis_version} ge "1.3.0") {

      # Set values used later
      no_leaks_ok {
        $r->multi;
        $r->set("foo$_" => "bar$_") for 1 .. 10;
        $r->exec->recv;
      } "multi/set";

      # Check values
      no_leaks_ok {
        $r->multi;
        $r->mget(map { "foo$_" } 1 .. 5);
        $r->mget(map { "foo$_" } 6 .. 10);
        my $res = $r->exec->recv;
      } "multi/mget";

      # Reproduce github issue 6:
      # http://github.com/miyagawa/AnyEvent-Redis/issues/issue/6
      no_leaks_ok {
        $r->blpop('a' .. 'z', 1)->recv;
      } "blpop";

      $r->all_cv->end;
      $r->all_cv->recv;
      done_testing;

    } else {
      plan skip_all => "No support for MULTI in this server version";
    }
};

use strict;
use Test::More;
use t::Redis;

test_redis {
    my $sub = shift;
    my $port = shift;

    my $info = $sub->info->recv;
    if($info->{redis_version} lt "1.3.10") {
      # Test::TCP needs to cleanup, plan skip_all calls exit(0), see:
      # https://rt.cpan.org/Ticket/Display.html?id=60657
      print "1..0 # SKIP No PUBLISH/SUBSCRIBE support in this Redis version\n";
      # Hoop jumping to output our own TAP...
      $Test::Builder::Test->no_ending(1);
      $Test::Builder::Test->no_header(1);
      return;
    }

    my $pub = AnyEvent::Redis->new(host => "127.0.0.1", port => $port);

    my $all_cv = AE::cv;

    # $pub is for publishing
    # $sub is for subscribing

    my $x = 0;
    my $expected_x = 0;

    my $count = 0;
    my $expected_count = 10;

    my $sub1_cv = $sub->subscribe("test.1", sub {
            my($message, $chan) = @_;
            $x += $message;
            if(++$count == $expected_count) {
                $sub->unsubscribe("test.1");
                is $x, $expected_x, "Messages received, values as expected";
            }
        });
    $all_cv->begin;
    $sub1_cv->cb(sub { $sub1_cv->recv; $all_cv->end });

    for(1 .. $expected_count) {
        my $cv = $pub->publish("test.1" => $_);
        $expected_x += $_;
        # Need to be sure a client has subscribed
        $expected_x = 0, redo unless $cv->recv;
    }

    # Pattern subscription
    my $y = 0;
    my $expected_y = 0;

    my $count2 = 0;
    my $expected_count2 = 10;

    my $sub2_cv = $sub->psubscribe("test.*", sub {
            my($message, $chan) = @_;
            $y += $message;
            if(++$count2 == $expected_count2) {
                $sub->punsubscribe("test.*");
                is $y, $expected_y, "Messages received, values as expected";
            }
        });
    $all_cv->begin;
    $sub2_cv->cb(sub { $sub2_cv->recv; $all_cv->end });

    for(1 .. $expected_count2) {
        my $cv = $pub->publish("test.$_" => $_);
        $expected_y += $_;
        # Need to be sure a client has subscribed
        $expected_y = 0, redo unless $cv->recv;
    }

    $all_cv->recv;
    done_testing;
};


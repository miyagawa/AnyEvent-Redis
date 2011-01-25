package AnyEvent::Redis::Error;

use strict;
use warnings;

sub message {
    ${ $_[0] }
}

1;

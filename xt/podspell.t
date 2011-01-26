use Test::More;
eval q{
    use Test::Spelling;
    add_stopwords(qw( multi unsubscribe unsubscribing versa ));
};
plan skip_all => "Test::Spelling is not installed." if $@;
add_stopwords(<DATA>);
set_spell_cmd("aspell -l en list");
all_pod_files_spelling_ok('lib');
__DATA__
Tatsuhiko
Miyagawa
AnyEvent
Redis
Aylward
Barratt
Brocard
Chia
Kao
Leadbeater
Zawodny
cuny
franck
hget
hostname
hset
liang
lpop
lpush

# ZoneLocal plugin for MogileFS, by hachi

package MogileFS::Plugin::RecordDeletes;

use strict;
use warnings;

# use MogileFS::Worker::Query;
# use MogileFS::Network;
# use MogileFS::Util qw/error/;

use MogileFS::Server;

sub load {
    MogileFS::register_global_hook(
        'cmd_delete',
        sub {
            my $args = shift;
            my $dmid = $args->{dmid};
            my $key = $args->{key};

            defined($key) && length($key) or return 0;
            my $fid = MogileFS::FID->new_from_dmid_and_key($dmid, $key)
                or return 0;

            Mgd::log('info', "Deleting fid $fid: domain $dmid key $key");
            return 1;
        });

    return 1;
}

sub unload {
    MogileFS::unregister_global_hook('cmd_delete');
    return 1;
}

1;

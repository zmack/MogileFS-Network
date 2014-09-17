# ZoneLocal plugin for MogileFS, by hachi

package MogileFS::Plugin::RecordDeletes;

use strict;
use warnings;
# use Carp;
use MogileFS::Server;
use MogileFS::Store;

MogileFS::Store->add_extra_tables("deleted_key");

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
            return add_deleted_key($fid->id, $dmid, $key);
        });

    return 1;
}

sub unload {
    MogileFS::unregister_global_hook('cmd_delete');
    return 1;
}

sub add_deleted_key {
    my ($fidid, $dmid, $dkey) = @_;
    my $store = Mgd::get_store();
    my $dbh = $store->dbh;

    my $updated = eval {
        $dbh->do("INSERT INTO deleted_key (fid, dmid, dkey) VALUES (?, ?, ?)", {}, $fidid, $dmid, $dkey);
    };

    if ($@ || $dbh->err || $updated < 1) {
        Mgd::log("error", "Failed to record deleted key: $@");
        return 0;
    } else {
        return $updated;
    }
}

{
    package MogileFS::Store;

    sub TABLE_deleted_key {
        q{CREATE TABLE `deleted_key` (
              `fid` BIGINT UNSIGNED NOT NULL PRIMARY KEY,
              `dmid` SMALLINT UNSIGNED NOT NULL,
              `dkey` varchar(255) DEFAULT NULL,
              INDEX by_key(dkey)
          );};
    }
}

1;

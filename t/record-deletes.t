#!/usr/bin/perl

use strict;
use warnings;
use Test::More;
use Data::Dumper;

use MogileFS::Test;
use MogileFS::Worker::Query;
use MogileFS::Plugin::RecordDeletes;

# Create a temp data store.
my $sto = eval { temp_store(); };
if (!$sto) {
    plan skip_all => "Can't create temporary test database: $@";
    exit 0;
}
my $store = Mgd::get_store;
isa_ok($store, 'MogileFS::Store');

# Register our plugin
MogileFS::Plugin::RecordDeletes->load;

# Create a query worker.
open(my $null, "+>", "/dev/null") or die $!;
my $query = MogileFS::Worker::Query->new($null);
isa_ok($query, 'MogileFS::Worker::Query');
my $sent_to_parent;
no strict 'refs';
*MogileFS::Worker::Query::send_to_parent = sub {
    $sent_to_parent = $_[1];
};
use strict;

# Create a domain.
my $domain_factory = MogileFS::Factory::Domain->get_factory;
ok($domain_factory, "Got a domain factory");
my $domain = $domain_factory->set({ dmid => 1, namespace => "test_domain" });
ok($domain, "Made a domain object");
is($domain->id, 1, "Domain ID is 1");
is($domain->name, "test_domain", "Domain name is test_domain");
is($store->create_domain("test_domain"), 1, "Domain ID in the DB is 1");

# Create a key.
my $fidid = $store->register_tempfile(dmid => 1, key => "test_key", devids => "1");
$store->replace_into_file(fidid => $fidid, dmid => 1, key => "test_key",
                          classid => 0, devcount => 1);
ok($fidid, "fid id is $fidid");
my $fid = MogileFS::FID->new($fidid);
ok($fid->exists, "Fid has been created.");

ok($query->cmd_delete({ domain => "test_domain", key => "test_key" }), "Deleted the key");

done_testing();

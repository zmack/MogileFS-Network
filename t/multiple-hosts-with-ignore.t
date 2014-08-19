#!/usr/bin/perl

use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);

use MogileFS::Server;
use MogileFS::Util qw(error_code);
use MogileFS::ReplicationPolicy::MultipleHostsWithIgnore;
use MogileFS::Test;

plan tests => 10;

is(rr("min=2 h1[d1=X d2=X] f2[d3=_]", [3]),
   "all_good");

is(rr("min=2 h1[d1=X d2=X] f2[d3=_]", []),
   "ideal(3)");

# Not sure this is super a-ok kosher
is(rr("min=3 h1[d1=X d2=X] h3[d4=_ d5=_] f2[d3=X]", [3]),
   "all_good");

is(rr("min=2 h1[d1=X]", []),
   "temp_fail");

is(rr("min=2 h1[d1=X] f2[d3=_]", [3]),
   "temp_fail");

is(rr("min=2 h1[d1=_ d2=_ d3=_] h2[d4=X]", [4]),
   "ideal(1,2,3)");

is(rr("min=2 h1[d1=_ d2=_ d3=_] h2[d4=X] h3[d5=_]", [4,5]),
   "ideal(1,2,3)");

is(rr("min=2 h1[d1=_ d2=_ d3=_] h2[d4=X] h3[d5=_]", [4,5]),
   "ideal(1,2,3)");

is(rr("min=2 h1[d1=_ d2=_ d3=_] h2[d4=X] h3[d5=_]", [3,4,5]),
   "ideal(1,2)");

is(rr("min=4 h1[d1=_ d2=_ d3=_] h2[d4=X] h3[d5=_]", [3,4]),
   "ideal(1,2,5)");

sub rr {
    my $state = $_[0];
    my $ignore_devices = $_[1];
    my $ostate = $state; # original

    MogileFS::Factory::Host->t_wipe;
    MogileFS::Factory::Device->t_wipe;
    MogileFS::Config->set_config_no_broadcast("min_free_space", 100);
    my $hfac = MogileFS::Factory::Host->get_factory;
    my $dfac = MogileFS::Factory::Device->get_factory;

    my $min = 2;
    if ($state =~ s/^\bmin=(\d+)\b//) {
        $min = $1;
    }

    my $hosts   = {};
    my $devs    = {};
    my $candidate_devs = {};
    my $on_devs = [];

    my $parse_error = sub {
        die "Can't parse:\n   $ostate\n"
    };
    while ($state =~ s/\b(h|f)(\d+)(?:=(.+?))?\[(.+?)\]//) {
        my ($type, $n, $opts, $devstr) = ($1, $2, $3, $4);
        my $host_ip = "127.0.0.1";
        $opts ||= "";
        die "dup host $n" if $hosts->{$n};

        if ($type eq "f") {
          $host_ip = "10.0.0.1"
        }

        my $h = $hosts->{$n} = $hfac->set({ hostid => $n,
            status => ($opts || "alive"), observed_state => "reachable",
            hostname => $n, hostip => $host_ip });

        foreach my $ddecl (split(/\s+/, $devstr)) {
            $ddecl =~ /^d(\d+)=([_X!])(?:,(\w+))?$/
                or $parse_error->();
            my ($dn, $on_not, $status) = ($1, $2, $3);
            die "dup device $dn" if $devs->{$dn};
            my $d = $devs->{$dn} = $dfac->set({ devid => $dn,
                hostid => $h->id, observed_state => "writeable",
                status => ($status || "alive"), mb_total => 1000,
                mb_used => 100, });
            if ($on_not ne "!") { # ! means "the file isn't here, but avoid this device in the policy"
                $candidate_devs->{$dn} = $d;
            }
            if ($on_not eq "X" && $d->dstate->should_have_files) {
                push @$on_devs, $d;
            }
        }
    }
    $parse_error->() if $state =~ /\S/;

    # my $polclass = "MogileFS::ReplicationPolicy::MultipleHostsWithIgnore";
    # my $pol = $polclass->new($ignore_devices);

    my $pol = MogileFS::ReplicationPolicy->new_from_policy_string("MultipleHostsWithIgnore(" . join(",", @$ignore_devices) . ")");
    my $rr = $pol->replicate_to(
                                fid      => 1,
                                on_devs  => $on_devs,
                                all_devs => $candidate_devs, # In the case of rebalance, all_devs is all candidate devs, not strictly all devices
                                failed   => {},
                                min      => $min,
                                );
    return $rr->t_as_string;
}


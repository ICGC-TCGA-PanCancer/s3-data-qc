#!/usr/bin/perl

use GNOS::Download;

# usage: perl -I path_to_gnos_download_package this_script <other_arguments>

my $pem = shift;
my $url = shift;
my $gnos_id = shift;
my $max_attempts = shift;
my $timeout_minutes = shift;
my $max_children = shift;
my $rate_limit_mbytes = shift;
my $ktimeout = shift;

if (!$pem || !$url || !$gnos_id) {
  die "Error! Usage: please specify GNOS key file, full download URL, and gnos_id as command line arguments to run this tool.\n";
}

my $ret_val = GNOS::Download->run_download($pem, $url, $gnos_id, $retries, $timeout_min, $max_children, $rate_limit_mbytes, $k_timeout_min, $min_mbyte_download_rate);

exit ($ret_val);

#!/usr/bin/perl

# mcabber_merge_history.pl - merge mcabber history files
# Copyright (C) 2015 Benjamin Abendroth
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

use strict;
use warnings;
use sort qw(stable _mergesort);
use autodie;
use Getopt::Long qw(:config gnu_getopt auto_version);
use File::Copy qw(copy move);
use File::Temp qw(tempfile);

sub merge_file($$$);
sub merge_file_inplace($$);
sub merge_dirs($$$);
sub merge_dirs_inplace($$);
sub print_help();

my $opt_inplace = 0;
my $opt_parallel = 0;

GetOptions(
   'help|h'       => \&print_help,
   'inplace|i'    => \$opt_inplace,
   'parallel|p=i' => \$opt_parallel,
) or die "Error on commandline\n";

if ($opt_parallel) {
   eval {require Parallel::ForkManager}
      or die "You need to install Parallel::ForkManager to use --parallel\n";
}

# when using --inplace we don't want a third argument
if ($#ARGV < 2 - $opt_inplace) {
   die "Missing arguments\n";
}

if ($#ARGV > 2 - $opt_inplace) {
   die "Too many arguments\n";
}

if (-d $ARGV[0]) {
   if (! -d $ARGV[1]) {
      die "Not a directory: $ARGV[1]\n";
   }

   if ($opt_inplace) {
      merge_dirs_inplace($ARGV[0], $ARGV[1]);
   }
   elsif ($ARGV[0] eq $ARGV[2] or $ARGV[1] eq $ARGV[2]) {
      die "Output directory is input directory\n";
   }
   else {
      merge_dirs($ARGV[0], $ARGV[1], $ARGV[2]);
   }
}
elsif (-f $ARGV[0]) {
   if (! -f $ARGV[1]) {
      die "Not a file: $ARGV[1]\n";
   }

   if ($opt_inplace) {
      merge_file_inplace($ARGV[0], $ARGV[1]);
   }
   elsif ($ARGV[0] eq $ARGV[2] or $ARGV[1] eq $ARGV[2]) {
      die "Output file is input file\n";
   }
   else {
      merge_file($ARGV[0], $ARGV[1], $ARGV[2]);
   }
}
else {
   die "No such file or directory: $ARGV[0]\n";
}

# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

sub print_help()
{
   require Pod::Usage;
   Pod::Usage::pod2usage(-exitstatus => 0, -verbose => 2);
}

#
# Read an history entry
#  Input:
#     $1: Filehandle
#
#  Output:
#     @entry: History entry as array
#
sub readline_hist($)
{
   my $fh = shift;
   my $line = readline($fh) || return;

   my $status =       substr($line, 0, 2);
   my $timestamp =    substr($line, 3, 18);
   my $lines_follow = substr($line, 22, 3);
   my $msg =          substr($line, 25);

   $msg =~ s/^\s*//;
   $msg =~ s/\s*$/\n/;

   for (1..$lines_follow) {
      ($line = readline($fh)) =~ s/\s*$/\n/;
      $msg .= $line;
   }

   return ($status, $timestamp, $lines_follow, $msg);
}

#
# Read complete history into array.
# The history will be presorted by timestamp, because
# mcabber sometimes writes wrong status-timestamps
#  Input:
#     $1: Filename
#
#  Output:
#     @lines: Array of history entries (see readline_hist)
#
sub read_histfile($)
{
   open(my $fh, "<", shift);

   my @lines;

   while ( my (@line) = readline_hist($fh) ) {
      push @lines, [@line];
   }

   close($fh);

   return sort {$a->[1] cmp $b->[1]} @lines;
}

#
# Check the timestamp order of history entries.
# Dies if timestamps are in the wrong order.
# (only for debugging)
#  Input:
#     $1: Filename
#
sub check_timestamp_order($)
{
   open (my $fh, "<", shift);

   my ($oldTs, $ts) = ("", "");

   while ($ts = (readline_hist($fh))[1]) {
      die if $ts lt $oldTs;
      $oldTs = $ts;
   }
}

#
# List all files (and only files) in a directory.
#  Input:
#     $1: Directory
#
#  Output:
#     @files: Array of found found files (basename, without directory)
#
sub find_files($)
{
   my $dir = shift;
   my @files;

   opendir(my $dh, $dir);
   for (readdir($dh)) {
      next if ($_ eq '.' or $_ eq '..');
      next if (! -f "$dir/$_");

      push @files, $_;
   }
   closedir $dh;

   return @files;
}

#
# Merge history dirs inplace.
#  Input:
#     $1: First history dir (inplace)
#     $2: Second history dir
#
sub merge_dirs_inplace($$)
{
   my ($dir1, $dir2) = @_;

   my $fork_manager;
   if ($opt_parallel) {
      $fork_manager = new Parallel::ForkManager($opt_parallel);
   }

   for (find_files($dir2)) {
      if (! -e "$dir1/$_") {
         copy("$dir2/$_", "$dir1/$_");
      }
      else {
         if ($opt_parallel) {
            $fork_manager->start and next;
         }

         merge_file_inplace("$dir1/$_", "$dir2/$_");

         if ($opt_parallel) {
            $fork_manager->finish;
         }
      }
   }

   if ($opt_parallel) {
      $fork_manager->wait_all_children;
   }
}

#
# Merge history dirs
#  Input:
#     $1: First history dir
#     $2: Second history dir
#     $3: Output directory
#
sub merge_dirs($$$)
{
   my ($dir1, $dir2, $odir) = @_;

   die "Could not create output directory '$odir': $@\n"
      if ! (-d $odir || mkdir $odir);

   my $fork_manager;
   if ($opt_parallel) {
      $fork_manager = new Parallel::ForkManager($opt_parallel);
   }

   for (find_files($dir2)) {
      if (-e "$dir1/$_") {

         if ($opt_parallel) {
            $fork_manager->start and next;
         }

         merge_file("$dir1/$_", "$dir2/$_", "$odir/$_");

         if ($opt_parallel) {
            $fork_manager->finish;
         }
      }
      else {
         copy("$dir2/$_", "$odir/$_");
      }
   }

   for (find_files($dir1)) {
      if (! -e "$odir/$_") {
         copy("$dir1/$_", "$odir/$_");
      }
   }

   if ($opt_parallel) {
      $fork_manager->wait_all_children;
   }
}

#
# Merge history files (inplace).
#  Input: 
#     $1: First input file (inplace)
#     $2: Second input file
#
sub merge_file_inplace($$)
{
   my ($file1, $file2) = @_;
   my ($temp_fh, $temp_file) = tempfile();

   merge_file($file1, $file2, $temp_file);
   move($temp_file, $file1);
}

#
# Merge history files.
#  Input: 
#     $1: First input file
#     $2: Second input file
#     $3: Output file
#
sub merge_file($$$)
{
   my ($file1, $file2, $outfile) = (@_);
   print STDERR "$file1 & $file2 > $outfile\n";

   my $out_fh;
   if ($outfile eq '-') {
      $out_fh = *STDOUT;
   }
   else {
      open ($out_fh, ">", $outfile);
   }

   my (@history1) = read_histfile($file1);
   my (@history2) = read_histfile($file2);

   while (@history1 && @history2)
   {
      my $ts_cmp = ($history1[0]->[1] cmp $history2[0]->[1]);

      if ($ts_cmp <= 0)
      {
         if ($ts_cmp == 0 && $history1[0]->[3] eq $history2[0]->[3]) {
            shift @history2;
         }

         print {$out_fh} join(" ", @{shift @history1});
      }
      else
      {
         print {$out_fh} join(" ", @{shift @history2});
      }
   }

   if (@history1) {
      print {$out_fh} map {join(" ", @$_)} @history1;
   }
   elsif (@history2) {
      print {$out_fh} map {join(" ", @$_)} @history2;
   }

   close $out_fh;
}

__END__

=pod

=head1 NAME

mcabber_merge_history.pl - merge mcabber history files

=head1 SYNOPSIS

=over 8
 
mcabber_merge_history.pl
I<DIR> I<DIR> I<OUTPUT_DIR>

mcabber_merge_history.pl
I<FILE> I<FILE> I<OUTPUT_FILE>

mcabber_merge_history.pl
B<--inplace|-i> I<DIR> I<DIR>

mcabber_merge_history.pl
B<--inplace|-i> I<FILE> I<FILE>

=back

=head1 OPTIONS

=head2 Basic Startup Options

=over

=item B<--help>

Display this help text and exit.

=item B<--version>

Display the script version and exit.

=back

=head2 Options

=over

=item B<--parallel|-p> I<parallel operations>

Merge I<parallel operations> files at a time. This requires the Module Parallel::ForkManager.

=item B<--inplace|-i>

Don't use an output file/directory, instead merge the result into
the first file/directory given on commandline.

=back

=head1 AUTHOR

Written by Benjamin Abendroth.

=cut


package DBIx::Admin::BackupRestore;

# Documentation:
#	POD-style documentation is at the end. Extract it with pod2html.*.
#
# Reference:
#	Object Oriented Perl
#	Damian Conway
#	Manning
#	1-884777-79-1
#	P 114
#
# Note:
#	o Tab = 4 spaces || die.
#
# Author:
#	Ron Savage <ron@savage.net.au>
#	Home page: http://savage.net.au/index.html
#
# Licence:
#	Australian copyright (c) 2003 Ron Savage.
#
#	All Programs of mine are 'OSI Certified Open Source Software';
#	you can redistribute them and/or modify them under the terms of
#	The Artistic License, a copy of which is available at:
#	http://www.opensource.org/licenses/index.html

use strict;
use warnings;

use Carp;

require 5.005_62;

require Exporter;

our @ISA = qw(Exporter);

# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.

# This allows declaration	use DBIx::Admin::BackupRestore ':all';
# If you do not need this, moving things directly into @EXPORT or @EXPORT_OK
# will save memory.
our %EXPORT_TAGS = ( 'all' => [ qw(

) ] );

our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );

our @EXPORT = qw(

);
our $VERSION = '1.02';

my(%_decode_xml) =
(
	'&amp;'		=> '&',
	'&lt;'		=> '<',
	'&gt;'		=> '>',
	'&quot;'	=> '"',
);

my(%_encode_xml) =
(
	'&' => '&amp;',
	'<' => '&lt;',
	'>' => '&gt;',
	'"' => '&quot;',
);

# -----------------------------------------------

# Preloaded methods go here.

# -----------------------------------------------

# Encapsulated class data.

{
	my(%_attr_data) =
	(
		_clean			=> 0,
		_dbh			=> '',
		_skip_tables	=> [],
		_verbose		=> 0,
	);

	sub _default_for
	{
		my($self, $attr_name) = @_;

		$_attr_data{$attr_name};
	}

	sub _standard_keys
	{
		keys %_attr_data;
	}

}	# End of encapsulated class data.

# -----------------------------------------------

sub decode_xml
{
	my($self, $s) = @_;

	for my $key (keys %_decode_xml)
	{
		$s =~ s/$key/$_decode_xml{$key}/eg;
	}

	$s;

}	# End of decode_xml.

# -----------------------------------------------

sub encode_xml
{
	my($self, $str)	= @_;
	$str			=~ s/([&<>"])/$_encode_xml{$1}/eg;

	$str;

}	# End of encode_xml.

# -----------------------------------------------

sub backup
{
	my($self, $database)	= @_;
	$$self{'_xml'}			= qq|<?xml version = "1.0"?>\n|;
	$$self{'_xml'}			.= qq|<DBI database = "|. $self -> encode_xml($database) . qq|">\n|;

	my($table_name, $sql, $sth, $column_name, $data, $i, $field);

	for $table_name (@{$$self{'_tables'} })
	{
		$sql			= "select * from $table_name";
		$$self{'_xml'}	.= qq|\t<RESULTSET statement = "| . $self -> encode_xml($sql) . qq|">\n|;
		$sth			= $$self{'_dbh'} -> prepare($sql) || die("Can't prepare($sql): $DBI::errstr");

		print STDERR "Backup table: $table_name. \n" if ($$self{'_verbose'});

		$sth -> execute() || die("Can't execute($sql): $DBI::errstr");

		$column_name = $$sth{'NAME'};

		while ($data = $sth -> fetch() )
		{
			$$self{'_xml'}	.= "\t\t<ROW>\n";
			$i				= - 1;

			for $field (@$data)
			{
				$i++;

				if (defined($field) )
				{
					$field			=~ tr/\x20-\x7E//cd if ($$self{'_clean'});
					$$self{'_xml'}	.= "\t\t\t<" . $$column_name[$i] . '>' . $self -> encode_xml($field) . '</' . $$column_name[$i] . ">\n";
				}
			}

			$$self{'_xml'} .= "\t\t</ROW>\n";
		}

		die("Can't fetchrow_hashref($sql): $DBI::errstr") if ($DBI::errstr);

		$$self{'_xml'} .= "\t</RESULTSET>\n";
	}

	$$self{'_xml'} .= "</DBI>\n";

}	# End of backup.

# -----------------------------------------------

sub new
{
	my($caller, %arg)		= @_;
	my($caller_is_obj)		= ref($caller);
	my($class)				= $caller_is_obj || $caller;
	my($self)				= bless({}, $class);

	for my $attr_name ($self -> _standard_keys() )
	{
		my($arg_name) = $attr_name =~ /^_(.*)/;

		if (exists($arg{$arg_name}) )
		{
			$$self{$attr_name} = $arg{$arg_name};
		}
		elsif ($caller_is_obj)
		{
			$$self{$attr_name} = $$caller{$attr_name};
		}
		else
		{
			$$self{$attr_name} = $self -> _default_for($attr_name);
		}
	}

	die('You must call new as new(dbh => $dbh)') if (! $$self{'_dbh'});

	$self -> tables();

	$$self{'_skip'}{@{$$self{'_skip_tables'} } }	= (1) x @{$$self{'_skip_tables'} };
	$$self{'_xml'}									= '';

	return $self;

}	# End of new.

# -----------------------------------------------

sub restore
{
	my($self, $file_name)		= @_;
	$$self{'_restored_table'}	= [];

	open(INX, $file_name) || die("Can't open($file_name): $!");

	my($i, $line, $table_name, @key, @value, $key, $value, $sql, $sth);

	while ($line = <INX>)
	{
		next if ($line =~ /^(<\?xml|<DBI|<\/DBI)/);

		if ($line =~ /<RESULTSET.+from\s+(.+)">/)
		{
			$table_name = $self -> decode_xml($1);

			if (! $$self{'_skip'}{$table_name})
			{
				push @{$$self{'_restored_table'} }, $table_name;

				print STDERR "Restore table: $table_name. \n" if ($$self{'_verbose'});
			}
		}
		elsif ($line =~ /<ROW>/)
		{
			@key	= ();
			@value	= ();

			while ( ($line = <INX>) !~ m|</ROW>|)
			{
				($key, $value) = ($1, $self -> decode_xml($2) ) if ($line =~ m|^\s*<(.+?)>(.*?)</\1>|);

				if ($key =~ /timestamp/)
				{
					$value = '19700101' if ($value =~ /^0000/);
					$value = substr($value, 0, 4) . '-' . substr($value, 4, 2) . '-' . substr($value, 6, 2) . ' 00:00:00';
				}

				push(@key, $key);
				push(@value, $value);
			}

			# There may be a different number of fields from one row to the next.
			# Remember, only non-null fields are output by sub backup().

			if (! $$self{'_skip'}{$table_name})
			{
				$sql = "insert into $table_name (" . join(', ', @key) . ') values (' . join(', ', ('?') x @key) . ')';
				$sth = $$self{'_dbh'} -> prepare($sql) || die("Can't prepare($sql): $DBI::errstr");

				$sth -> execute(@value) || die("Can't execute($sql): $DBI::errstr");
				$sth -> finish();
			}
		}
	}

	close INX;

	[sort @{$$self{'_restored_table'} }];

}	# End of restore.

# -----------------------------------------------

sub tables
{
	my($self)			= @_;
	my($quote)			= $$self{'_dbh'} -> get_info(29) || ''; # SQL_IDENTIFIER_QUOTE_CHAR.
	$$self{'_tables'}	||= [sort map{s/$quote(.+)$quote/$1/; $_} $$self{'_dbh'} -> tables('%', '%', '%', 'table')];

}	# End of tables.

# -----------------------------------------------

1;

__END__

=head1 NAME

C<DBIx::Admin::BackupRestore> - Back-up all tables in a db to XML, and restore them

=head1 Synopsis

	use DBIx::Admin::BackupRestore;

	# Backup.

	open(OUT, "> $file_name") || die("Can't open(> $file_name): $!");
	print OUT DBIx::Admin::BackupRestore -> new(dbh => $dbh) -> backup('db_name');
	close OUT;

	# Restore.

	DBIx::Admin::BackupRestore -> new(dbh => $dbh) -> restore($file_name);

=head1 Description

C<DBIx::Admin::BackupRestore> is a pure Perl module.

It exports all data in all tables from one database to an XML file.

Then that file can be imported into another database, possibly under a different database
server.

Warning: It is designed on the assumption you have a stand-alone script which creates an
appropriate set of empty tables on the destination database server. You run that script,
and then run this module in 'restore' mode.

This module is used almost daily to transfer a MySQL database under MS Windows to a Postgres
database under Linux.

Similar modules are discussed below.

=head1 Distributions

This module is available both as a Unix-style distro (*.tgz) and an
ActiveState-style distro (*.ppd). The latter is shipped in a *.zip file.

See http://savage.net.au/Perl-modules.html for details.

See http://savage.net.au/Perl-modules/html/installing-a-module.html for
help on unpacking and installing each type of distro.

=head1 Constructor and initialization

new(...) returns a C<DBIx::Admin::BackupRestore> object.

This is the class's contructor.

Usage: DBIx::Admin::BackupRestore -> new().

This method takes a set of parameters. Only the dbh parameter is mandatory.

For each parameter you wish to use, call new as C<new(param_1 => value_1, ...)>.

=over 4

=item clean

The default value is 0.

If new is called as C<new(clean => 1)>, the backup phase deletes any characters outside the
range 20 .. 7E (hex).

The restore phase ignores this parameter.

This parameter is optional.

=item dbh

This is a database handle.

This parameter is mandatory.

=item skip_tables

The default value is [].

If new is called as C<new(skip_tables => ['some_table_name'])>, the restore phase
does not restore the tables named in the call to C<new()>.

This option is designed to work with CGI scripts using the module CGI::Sessions.

Now, the CGI script can run with the current CGI::Session data, and stale CGI::Session
data is not restored from the XML file.

This parameter is optional.

=item verbose

The default value is 0.

If new is called as C<new(verbose => 1)>, the backup and restore phases both print the names
of the tables to STDERR.

When beginning to use this module, you are strongly encouraged to use the verbose option
as a progress monitor.

This parameter is optional.

=back

=head1 Method: backup($database_name)

Returns a potentially-huge string of XML.

You would normally write this straight to disk.

The database name is passed in here to help decorate the XML.

=head1 Method: restore($file_name)

Returns an array ref of imported table names. They are sorted by name.

Opens and reads the given file, presumably one output by a previous call to backup().

If the incoming data is going in to a column of type timestamp, then the data is fiddled
in the following manner:

=over 4

=item Data matching /^0000/ is converted to 19700101

=item Data is converted to the format YYYY-MM-DD 00:00:00

=back

This transformation could easily be make optional. Just ask!

=head1 Example code

See the examples/ directory in the distro.

There are 2 demo programs:

=over 4

=item backup-db.pl

=item restore-db.pl

=back

=head1 Related Modules

On CPAN I can see 4 modules which obviously offer similar features - there may be others.

=over 4

=item DBIx::Copy

=item DBIx::Dump

=item DBIx::Migrate

=item DBIx::XML_RDB

=back

Of these, DBIx::XML_RDB is the only one I have experimented with. My thanks to Matt Sergeant
for that module.

I have effectively extended his module to automatically handle all tables, and to handle
importing too.

=head1 Required Modules

Carp.

=head1 Changes

See Changes.txt.

=head1 Author

C<DBIx::Admin::BackupRestore> was written by Ron Savage I<E<lt>ron@savage.net.auE<gt>>
in 2004.

Home page: http://savage.net.au/index.html

=head1 Copyright

Australian copyright (c) 2004, Ron Savage. All rights reserved.

	All Programs of mine are 'OSI Certified Open Source Software';
	you can redistribute them and/or modify them under the terms of
	The Artistic License, a copy of which is available at:
	http://www.opensource.org/licenses/index.html

=cut

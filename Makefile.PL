use ExtUtils::MakeMaker;

# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.

WriteMakefile
(
	($] ge '5.005') ?
	(
		'AUTHOR' => 'Ron Savage (ron@savage.net.au)',
		'ABSTRACT' => 'Back-up all tables in a db to XML, and restore them',
    ) : (),
	clean				=>
	{
		FILES			=> 'blib/* Makefile MANIFEST DBIx-Admin-BackupRestore-*'
	},
	dist				=>
	{
		COMPRESS		=> 'gzip',
		SUFFIX			=> 'gz'
	},
	DISTNAME			=> 'DBIx-Admin-BackupRestore',
	NAME				=> 'DBIx::Admin::BackupRestore',
	PREREQ_PM			=> {Carp => 0},
	VERSION_FROM		=> 'BackupRestore.pm',
);

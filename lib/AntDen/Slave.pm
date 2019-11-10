package AntDen::Slave;
use strict;
use warnings;
use Carp;
use YAML::XS;

use Sys::Hostname;
use File::Basename;

use AE;
use POSIX qw( :sys_wait_h );
use AntDen::Slave::DB;

use Data::Dumper;
our %tasktimeout;

sub new
{
    my ( $class, %this ) = @_;

    die "error db undefind" unless $this{db};
    die "error task undefind"
        unless $this{task} && -d $this{task};

    my %statusid = (
		init => 1,
		starting => 2,
		running => 3,
		stopping => 4,
		stoped => 5,
	);
	my %idstatus = map{ $statusid{$_} => $_ }keys %statusid;
	$this{statusid} = \%statusid;
	$this{idstatus} = \%idstatus;

    $this{db} = AntDen::Slave::DB->new( $this{db} );
    bless \%this, ref $class || $class;
}

sub run
{
    my $this = shift;

    my $cv = AE::cv;

    my ( $db, $statusid, $idstatus ) = @$this{qw( db statusid idstatus )};

	my $chld = AnyEvent->signal( signal => "CHLD", cb => sub{
        while((my $pid = waitpid(-1, WNOHANG)) >0)
        {
			warn "CHLD $pid exit;\n";
        }
	});

    my $ctrw = AnyEvent->timer ( after => 1, interval => 1, cb => sub{

        my @work = $db->selectTaskWork( $statusid->{running} );

		for ( @work )
        {
            my ( $taskid, $status, $expect, $executeid, $msg ) = @$_;
            print "taskid: $taskid status: $idstatus->{$status} expect: $idstatus->{$expect}\n";

			if( $idstatus->{$status} eq 'init' )
			{
				if( $idstatus->{$expect} eq 'stoped' )
				{
					$db->updateTaskStatus( $statusid->{stoped}, $taskid );
					$db->commit();
				}
				else
				{
					$tasktimeout{$taskid}{starting} = time + 60;

                    $db->updateTaskStatus( $statusid->{starting}, $taskid );
                    $db->commit;


					$status = $statusid->{starting};

					$executeid = eval{ $this->_startTask( $taskid ) };
					if ( $@ )
					{
						warn "start tsak fail: $@";
						$db->updateTaskMsg( 'start tsak fail', $taskid );
						$db->commit;
						next;
					}

					$db->updateExecuteid( $executeid, $taskid );
					$db->commit;
				}
			}

			if( $idstatus->{$status} eq 'starting' )
			{
				unless( $executeid )
				{
					$db->updateTaskMsg( 'no executeid', $taskid );
					$db->updateTaskStatus( $statusid->{stoped}, $taskid );
					$db->commit();
					next;
				}

				my $tstatus = eval{ $this->_getTaskStatus( $taskid, $executeid ); };
				if( $@ )
				{
					warn "get tsak status fail: $@";
					$db->updateTaskMsg( 'get task status fail', $taskid );
					$db->commit;
					next;
				}

				if( $tstatus eq 'running' )
				{
					$db->updateTaskStatus( $statusid->{running}, $taskid );
					$db->commit;
				}
				else{
                    if( $tasktimeout{$taskid}{starting} < time )
				    {
				    	$db->updateTaskStatus( $statusid->{stoped}, $taskid );
						$db->updateTaskMsg( 'start timeout', $taskid );
				    	$db->commit();
				    }
				}
			}

			if( $idstatus->{$status} eq 'running' )
			{
				 if( $idstatus->{$expect} eq 'running' )
				 {
				     my $tstatus = eval{ $this->_getTaskStatus( $taskid, $executeid ); };
				     if( $@ )
				     {
				         warn "get tsak status fail: $@";
				     	 $db->updateTaskMsg( 'get task status fail', $taskid );
				     	 $db->commit;
				     	 next;
				     }
					 if( $tstatus eq 'stoped' )
					 {
						 $db->updateTaskStatus( $statusid->{stopping}, $taskid );
						 $db->commit;

						 $status = $statusid->{stopping};
					 }

				 }
				 else
				 {
				     $tasktimeout{$taskid}{stopping} = time + 60;

                     $db->updateTaskStatus( $statusid->{stopping}, $taskid );
                     $db->commit;


				     $status = $statusid->{stopping};
		 		 }
			}

			if( $idstatus->{$status} eq 'stopping' )
			{
                 eval{ $this->_stopTask( $taskid, $executeid ); };
				 if( $@ )
				 {
					$db->updateTaskMsg( 'stopping code err', $taskid );
					$db->commit();
					next;
				 }

				 my $tstatus = eval{ $this->_getTaskStatus( $taskid, $executeid ); };
				 if( $@ )
				 {
				 	warn "get tsak status fail: $@";
					$db->updateTaskMsg( 'get task status fail', $taskid );
				 	$db->commit;
				 	next;
				 }

				 if( $tstatus eq 'stoped' )
				 {
				 	$db->updateTaskStatus( $statusid->{stoped}, $taskid );
				 	$db->commit;
				 }
			}
        }
    });
   $cv->recv;
}

sub _stopTask
{
    my ( $this, $taskid, $executeid ) = @_;
    my ( $db, $task ) = @$this{qw( db task )};

    my $conf = eval{ YAML::XS::LoadFile "$this->{task}/$taskid" };
	die "load config fail: $@" if $@;

	die "nofind executer on config" 
 		unless $conf->{task} && ref $conf->{task} eq 'HASH' && $conf->{task}{executer};

    my $executer = $conf->{task}{executer};

    my $code = $this->_executer( $executer, 'stop' );
	die "load code $executer/stop fail"
        unless $code && ref $code eq 'CODE';

	&$code( executeid => $executeid );
}

sub _startTask
{
    my ( $this, $taskid ) = @_;
    my ( $db, $task ) = @$this{qw( db task )};

    my $conf = eval{ YAML::XS::LoadFile "$this->{task}/$taskid" };
	die "load config fail: $@" if $@;

	die "nofind executer on config" 
 		unless $conf->{task} && ref $conf->{task} eq 'HASH' && $conf->{task}{executer};

    my $executer = $conf->{task}{executer};

    my $code = $this->_executer( $executer, 'start' );
	die "load code $executer/start fail"
        unless $code && ref $code eq 'CODE';

    my $status = &$code( resources => $conf->{resources}, 
            param => $conf->{task}{param} );
	die "start task code return error" unless defined $status;
	return $status;
}

sub _getTaskStatus
{
    my ( $this, $taskid, $executeid ) = @_;
    my $task = $this->{db};

    my $conf = eval{ YAML::XS::LoadFile "$this->{task}/$taskid" };
	die "load task $taskid config fail: $@" if $@;

	die "nofind executer on config" 
    	unless $conf->{task} && ref $conf->{task} eq 'HASH' && $conf->{task}{executer};

    my $executer = $conf->{task}{executer};

    my $code = $this->_executer( $executer, 'status' );
	die "load code $executer/status fail" unless $code && ref $code eq 'CODE';

    my $status = &$code( executeid => $executeid );

	die "get status error"
		unless $status && ( $status eq 'running' || $status eq 'stoped' );

	return $status;
}

sub _executer
{
    my ( $this, $name, $cmd ) = @_;
    unless( $this->{executer}{$name}{$cmd} )
    {
        my $code = do "$this->{code}/executer/$name/$cmd";
        $this->{executer}{$name}{$cmd} = $code 
            if $code && ref $code eq 'CODE';
    }

    return $this->{executer}{$name}{$cmd};

}

=head3 start( $conf )

  conf
  ---
  taskid: J.20191105.171023.154687.409.002
  resources:
    CPU:0: 2
    GPU:1: 1
    GPU:3: 1
  task:
    param:
      exec: sleep 100
    executer: exec
    
=cut

sub start
{
	my ( $this, $conf ) = @_;
    my ( $db, $task ) = @$this{qw( db task )};
	unless( -f "$task/$conf->{taskid}" )
	{
		eval{ YAML::XS::DumpFile "$task/$conf->{taskid}", $conf };
        die "dump config fail $@" if $@;
	}

    $db->startTask( $conf->{taskid} );
    $this->{db}->commit;
}

=head3 stop( $conf )

    conf
    ---
    taskid: J.20191105.171023.154687.409.002

=cut

sub stop
{
	my ( $this, $conf ) = @_;
    print "stop : $conf->{taskid}\n";

	$this->{db}->stopTask( $conf->{taskid} );
    $this->{db}->commit;
}

sub status
{
	my $this = shift;
    my @r = $this->{db}->selectTask();
    return \@r;
}

sub baseinfo
{
    my $this = shift;
    my %res;
    for my $name ( grep{ -f }glob "$this->{code}/resources/*" )
    {
        my $code = do $name;
        die "load code $name fail\n" unless $code && ref $code eq 'CODE';
        $name = basename $name;
        my $data = &$code();
        if( ref $data eq 'HASH' )
        {
            map{ $res{"$name:$_"} = $data->{$_} }keys %$data; 
        }
        else
        {
            $res{$name} = $data;
        }
    }
    return +{
        machine => +{
            hostname => Sys::Hostname::hostname,
            env => 'web1.0',
        },
        resources => \%res,
    }
}
1;

package AntDen::Controller;
use strict;
use warnings;
use Carp;
use YAML::XS;

use Time::HiRes 'gettimeofday';
use AntDen::Scheduler::DB;

use MYDan::Agent::Client;
use Data::Dumper;

sub new
{
    my ( $class, %this ) = @_;
    die "error db undefind" unless $this{db};
    die "error code undefind" unless $this{code};

    my %statusid = (
		standby => 1,
	    init => 2,
	    starting1 => 3,
	    starting => 4,
	    running => 5,
	    stopping1 => 6,
	    stopping => 7,
	    stoped => 8,
	);

	my %idstatus = map{ $statusid{$_} => $_ }keys %statusid;
	$this{statusid} = \%statusid;
	$this{idstatus} = \%idstatus;

    $this{db} = AntDen::Scheduler::DB->new( $this{db} );
    bless \%this, ref $class || $class;
}

sub run
{
    my $this = shift;

    my ( $db, $config, $idstatus, $statusid ) = @$this{qw( db config idstatus statusid )};


	my %slavedeletetaskid;
    while(1)
    {
		my @hostip = $db->selectControllerHostip();
		if( @hostip > 0 )
		{
			my ( $res, $err ) = $this->_getSlaveStatus( [map{$_->[0]}@hostip], [ keys %slavedeletetaskid ] );
		    %slavedeletetaskid = ();
			for my $tid ( keys %$res )
			{
				if( $res->{$tid} eq 'stoped' )
				{
					$slavedeletetaskid{$tid} = 1;
					$this->callback( $tid );
				}

				my $s = $statusid->{$res->{$tid}};
				$db->updateControllerStatusSafe($s, $tid, $s);
				$db->commit;
			}

		}
        my @work = $db->selectControllerWork();
    
        for ( @work )
        {
            #`taskid`,`hostip`,`status`,`expect` 
            my ( $taskid, $hostip, $status, $expect ) = @$_;
            print "taskid: $taskid status: $idstatus->{$status} expect: $idstatus->{$expect}\n";

			if( $idstatus->{$status} eq 'init' )
			{
				if( $idstatus->{$expect} eq 'stoped' )
				{
					$db->updateControllerStatus( $statusid->{stoped}, $taskid );
					$db->commit();
				}
				else
				{
        	        if( $this->_controlSlave( $taskid, $hostip, 'start' ) )
					{
						$db->updateControllerStatus( $statusid->{starting1}, $taskid );
						$db->commit();
					}
				}
			}

			if( $idstatus->{$status} eq 'running' )
			{
        	    if( $this->_controlSlave( $taskid, $hostip, 'stop' ) )
				{
					$db->updateControllerStatus( $statusid->{stopping1}, $taskid );
					$db->commit();
				}
			}
        }
        sleep 1;
	}

}

sub _getSlaveStatus
{
    my ( $this, $ip, $deletetasks ) = @_;

    use MYDan::Util::OptConf;
    my %o =  MYDan::Util::OptConf->load()->dump('agent');
    $o{user} = 'antden';
    $o{sudo} = 'root';
    my %query = ( 
        code => 'antden', 
        argv => +{ 
            ctrl => 'status', 
			deletetask => $deletetasks,
        }, 
        map{ $_ => $o{$_} }qw( user sudo ) 
    );

    my %result = MYDan::Agent::Client->new( @$ip )->run( %o, query => \%query );
	print Dumper \%result;
	#return ( defined $result{$hostip} &&  $result{$hostip} =~ /--- 0\n$/ ) ? 1 : 0;
	my ( %res, %err );
	for my $ip ( keys %result )
	{
		if( $result{$ip} =~ s/--- 0\n$// )
		{
			my $x = eval{ YAML::XS::Load $result{$ip} };
			if( $x && ref $x eq 'HASH' )
			{
				#$res{$ip} = $x;
				map{ $res{$_} = $x->{$_} } keys %$x;
			}
			else
			{
				$err{$ip} = 1;
			}
		}
		else
		{
			$err{$ip} = 1;
		}
	}
	return ( \%res, \%err );
}

# return 1 success
# return 0 fail
sub _controlSlave
{
    #ctrl = start or stop
    my ( $this, $taskid, $hostip, $ctrl ) = @_;

    my $config = YAML::XS::LoadFile "$this->{conf}/task/$taskid";

    print Dumper '_controlSlave', \@_;
    use MYDan::Util::OptConf;
    my %o =  MYDan::Util::OptConf->load()->dump('agent');
    $o{user} = 'antden';
    $o{sudo} = 'root';
    my %query = ( 
        code => 'antden', 
        argv => +{ 
            ctrl => $ctrl, 
            config => + { %$config, taskid => $taskid } 
        }, 
        map{ $_ => $o{$_} }qw( user sudo ) 
    );

    my %result = MYDan::Agent::Client->new( $hostip)->run( %o, query => \%query );
	print Dumper \%result;
    return ( defined $result{$hostip} &&  $result{$hostip} =~ /--- 0\n$/ ) ? 1 : 0;
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
    
  ip
  ---
=cut

sub start
{
	my ( $this, $conf, $ip ) = @_;

	eval{ YAML::XS::DumpFile "$this->{conf}/task/$conf->{taskid}",
   		+{ task => $conf->{task}, resources => $conf->{resources}}; };
	die "dump config fail: $@" if $@;

	$this->{db}->insertController( $conf->{taskid}, $ip );
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

	$this->{db}->updateControllerExpect( $this->{statusid}{stoped}, $conf->{taskid} );
    $this->{db}->commit;
}

=head3 callback( $conf )

	taskid: J.20191105.171023.154687.409.002

=cut

sub callback
{
	my ( $this, $taskid ) = @_;

	$this->{db}->deleteAllocated( $taskid );
	$this->{db}->commit;
}





1;

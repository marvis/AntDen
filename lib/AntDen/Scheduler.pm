package AntDen::Scheduler;
use strict;
use warnings;
use Carp;
use POSIX;
use AE;
use YAML::XS;
use Time::HiRes 'gettimeofday';

use AntDen::Scheduler::DB;

use Data::Dumper;

sub new
{
    my ( $class, %this ) = @_;
    map{ die "error $_ undefind" unless $this{$_} }qw( db code conf );
    $this{db} = AntDen::Scheduler::DB->new( $this{db} );
    bless \%this, ref $class || $class;
}



=head3 startJob( $conf )

  conf
  ---
  -
    executer: exec
    exec: sleep 100
    resources:
      count: 1
      ip: '' #
      env: 'web1.0'
      resources:
        CPU: 2
        GPU: 2
  -
    executer: exec
    exec: sleep 100
    resources:
      count: 1
      ip: '' #
      env: 'web1.0'
      resources:
        CPU: 2
        GPU: 2
=cut

sub startJob
{
	my ( $this, $conf, $nice ) = @_;
    $nice = 5 unless $nice && $nice =~ /^\d+$/;

	die "conf err" unless $conf && @$conf > 0;

    my $db = $this->{db};
    my ($sec ,$usec ) = gettimeofday;
    my $jobid = sprintf "%s.%06d.%03d", POSIX::strftime( "J.%Y%m%d.%H%M%S", localtime( $sec ) ), $usec, rand( 1000 );

    my %code;

    my $id = 1;
    my ( %env, $env, %resources, $resources );

    for my $config ( @$conf )
    {
        die "task too long\n" if $id >= 1000;

        map{ die "$_ undefined.\n" unless $config->{$_} }qw( executer resources );
        die "resources not Hash.\n" unless ref $config->{resources} eq 'HASH';
        $config->{resources}{count} ||= 1;
        map{ die "resources.$_ undefined.\n" unless $config->{resources}{$_} }qw( resources env );
        die "resources.resources not Hash.\n" unless ref $config->{resources}{resources} eq 'HASH';
        die "resources.resources null.\n" unless keys %{$config->{resources}{resources}}; 

        unless( $code{$config->{executer}} )
        {
            my $c = "$this->{code}/executer/$config->{executer}/checkparams";
            my $code = do $c;
            die "load code $c fail" unless $code && ref $code eq 'CODE';
            $code{$config->{executer}} = $code;
        }

        eval{ &{$code{$config->{executer}}}( %$config ); };
        die "check executer params fail: $@" if $@;

        $env{$config->{resources}{env}} = 1;
        my $r = $config->{resources}{resources};
        map{ $resources{$_} += $r->{$_} * $config->{resources}{count} }keys %$r;
        
    }

    $env = join ',', sort keys %env;
    $resources = join ',', map{ "$_:$resources{$_}" } sort keys %resources;

    eval{ YAML::XS::DumpFile "$this->{conf}/job/$jobid", $conf };
    die "save job config fail: $@" if $@;

    $db->insertQueue( $jobid, $env, $nice, $resources, POSIX::strftime( "%Y%m%d.%H%M%S", localtime($sec) ) );
    $this->{db}->commit;

    return $jobid;
}

=head3 queuingJob

    jobid

=cut 
sub queuingJob
{
    my ( $this, $jobid ) = @_;
    $this->{db}->queuingJob( $jobid );
    $this->{db}->commit();
}

=head3 pauseJob

    jobid

=cut 
sub pauseJob
{
    my ( $this, $jobid ) = @_;
    $this->{db}->pauseJob( $jobid );
    $this->{db}->commit();
}

=head3 pauseJob

    jobid

=cut 
sub cancelJob
{
    my ( $this, $jobid ) = @_;
    $this->{db}->cancelJob( $jobid );
    $this->{db}->commit();
}

=head3 reniceJob

    jobid

=cut 
sub reniceJob
{
    my ( $this, $jobid, $nice ) = @_;
    die "nice format err" unless defined $nice && $nice =~ /^\d+$/ && $nice >=0 && $nice<=9;
    $this->{db}->updateQueueNice( $nice, $jobid );
    $this->{db}->commit();
}

=head3 stopJob

    jobid

=cut 
sub stopJob
{
    my ( $this, $jobid ) = @_;
    
    $this->{db}->stopJobExpect( $jobid );
    $this->{db}->commit();
}

sub run
{
    my $this = shift;

    my $cv = AE::cv;

    my ( $db, $config ) = @$this{qw( db config )};

    my $queuew = AnyEvent->timer ( after => 1, interval => 1, cb => sub{

        my @jobid = $db->selectQueueWork();

        my $cnt = 1;
        for ( @jobid )
        {
            my ( $jobid ) = @$_;
            print "jobid: $jobid\n";
            last unless $this->schedulerJob( $jobid );

            last if  $cnt ++ >= 10;
        }

    });
   $cv->recv;
}


=head3 schedulerJob

    jobid

=head3
    
    return 1 is success
    return 0 is fail

=cut 
sub schedulerJob
{
    my ( $this, $jobid ) = @_;
    

    my $config = eval{ YAML::XS::LoadFile "$this->{conf}/job/$jobid" };
    if( $@ )
    {
        warn "load config $this->{conf}/job/$jobid fail: $@";
        return 0;
    }


	#config
    #$VAR2 = [
    #          {
    #            'resources' => {
    #                             'resources' => {
    #                                              'GPU' => 2,
    #                                              'CPU' => 2
    #                                            },
    #                             'env' => 'web1.0',
    #                             'count' => 2
    #                           },
    #            'executer' => 'exec',
    #            'exec' => 'sleep 999'
    #          },
    #          {
    #            'exec' => 'sleep 100',
    #            'executer' => 'exec',
    #            'resources' => {
    #                             'count' => 1,
    #                             'resources' => {
    #                                              'CPU' => 2,
    #                                              'GPU' => 2
    #                                            },
    #                             'env' => 'web1.0'
    #                           }
    #          }
    #        ];

	my @config;
	my $id = 1;
	for my $conf ( @$config )
	{
		for( 1 .. $conf->{resources}{count} )
		{
			my $taskid = sprintf "%s.%03d", $jobid, $id ++;
			push @config, +{ %$conf, taskid => $taskid }
		}
	}

	return $this->buyResources( $jobid, \@config );
}






####################################



#
#conf = +{
#    machine => +{
#        ip => '127.0.0.2',
#        hostname => 'feng-pc-123',
#        env => 'env123',
#        status => 'ok1111',
#        heartbeat => '1572700378',
#    },
#    resources => +
#    {
#        'GPU:0' => 1,
#        'GPU:1' => 1,
#        'GPU:2' => 1,
#        'GPU:3' => 1,
#        'CPU' => 1024,
#        'MEM' => 1024,
#    }
#}
sub addMachine
{
    my ( $this, $conf ) = @_;
	my $db = $this->{db};

	return if my $machine = $db->selectMachineByIp( $conf->{machine}{ip} );

	$db->insertMachine( map{ $conf->{machine}{$_} || die 'err' }@{$db->column('machine')} );

	for my $k ( keys %{$conf->{resources} } )
	{
		my ( $name, $id ) = split /:/, $k, 2;
		$id ||= 0;

		$db->insertResources( $conf->{machine}{ip}, $name, $id, $conf->{resources}{$k} );
	}

	$db->commit();
}

# machineip, envname
sub updateMachineEnv
{
	my ( $this, $ip, $envname ) = @_;
    my $db = $this->{db};
	$db->updateMachineEnv( $envname, $ip );
	$db->commit();
}

sub updateMachineHeartbeat
{
	my ( $this, $ip ) = @_;
    my $db = $this->{db};
	$db->updateMachineHeartbeat( time, $ip );
	$db->commit();
}

=head3

$VAR1 = [
          {
            'resources' => {
                             'count' => 2,
                             'resources' => {
                                              'GPU' => 2,
                                              'CPU' => 2
                                            },
                             'env' => 'web1.0'
                           },
            'executer' => 'exec',
            'taskid' => 'J.20191107.121142.976660.931.001',
            'exec' => 'sleep 999'
          },
          {
            'taskid' => 'J.20191107.121142.976660.931.002',
            'exec' => 'sleep 999',
            'resources' => $VAR1->[0]{'resources'},
            'executer' => 'exec'
          },
          {
            'exec' => 'sleep 100',
            'taskid' => 'J.20191107.121142.976660.931.003',
            'executer' => 'exec',
            'resources' => {
                             'resources' => {
                                              'GPU' => 2,
                                              'CPU' => 2
                                            },
                             'count' => 1,
                             'env' => 'web1.0'
                           }
          }
        ];

=cut
sub buyResources
{

    my ( $this, $jobid, $conf ) = @_;
    my $db = $this->{db};

	#machine status is active
	#ip,hostname,env,name,id,value

	my @resources = $db->selectResourcesAndActiveMachineInfo();
	my ( %res, %machine );
	for my $res ( @resources )
	{
		my ( $ip, $hostname, $env, $name, $id, $value ) = @$res;
		$machine{$ip} = +{ hostname => $hostname, env => $env };
		push @{$res{$ip}{total}}, +{ name => $name, id => $id, value => $value };
		$res{$ip}{free}{$name} += $value;
	}

	# [ machineip, resourcesname, resourcesid, resourcesvalue ], []
	my @allocated = $db->selectAllocated();
	for my $allocated ( @allocated )
	{
		my ( $machineip, $resourcesname, $resourcesid, $resourcesvalue ) 
            = @$allocated;
		push @{$res{$machineip}{used}}, +{ 
			name => $resourcesname, 
			id => $resourcesid, 
			value => $resourcesvalue 
		};
		$res{$machineip}{free}{$resourcesname} -= $resourcesvalue;
	}



	my @rs = ();
	for my $c ( @$conf )
	{
		#{ 
		#	uuid => 'machineuuid',
		# 	res = [
		#		[ 'GPU', '01', 1 ]
		#		[ 'CPU', '0', 200 ]
		#	]
		#

#          {
#            'exec' => 'sleep 100',
#            'taskid' => 'J.20191107.121142.976660.931.003',
#            'executer' => 'exec',
#            'resources' => {
#                             'resources' => {
#                                              'GPU' => 2,
#                                              'CPU' => 2
#                                            },
#                             'count' => 1,
#                             'env' => 'web1.0'
#                           }
#          }
#

#		my %rs = ( jobid => $c->{jobid}, env => $c->{env} );
#

		#_findResources rename to _markResourcesOnDB
		my $result = _findResources( \%machine, \%res, $c->{taskid}, $c->{resources}, 1 );
        print Dumper $result;
		unless( $result && $result->{ip} )
		{
		    $result = _findResources( \%machine, \%res, $c->{taskid}, $c->{resources}, 0 );
            if( $result && $result->{uuid} )
            {
                $db->updateMachineEnv( 
                    "$machine{$result->{uuid}}{env}.to.$c->{env}.by.$c->{taskid}",
                    $result->{ip}, 
                );
         }
		}
		unless( $result && $result->{ip} )
		{
			$db->rollback();
			return;
		}

        #标记资源的使用
        for( @{$result->{res}} )
        {
            push @{$res{$result->{ip}}{used}},
                +{ name => $_->[0], id => $_->[1], value => $_->[2] };
            $res{$result->{ip}}{free}{$_->[0]} -= $_->[2];
        }


		my %rrrsss;
		for my $r ( @{$result->{res} } )
		{
			#r => name id value
			$rrrsss{resources}{"$r->[0]:$r->[1]"} = $r->[2];
			$db->insertAllocatedByJobid( $result->{ip},  @$r, $c->{taskid} );
		}
 		my %tmp = %$c;
		delete $tmp{executer};
		delete $tmp{resources};
		delete $tmp{taskid};

		my %controllerConfig = ( task => +{ param => \%tmp, executer => $c->{executer}  }, resources => \%rrrsss );

		YAML::XS::DumpFile "$this->{conf}/task/$c->{taskid}", \%controllerConfig;
		$db->insertController( $c->{taskid}, $result->{ip});
	}
    $db->updateQueueStatus2Allocated( $jobid );
	$db->commit();
}

#sub recycleResources
#{
#    my ( $this, $jobid ) = @_;
#    my $db = $this->{db};
#	$db->deleteAllocatedByJobId( $jobid );
#    $db->commit();
#}

#conf = [
#	+{
#		jobid => 'J.20191111.010101.000000.000.000',
#		env => 'web1',
#		resources => +{
#			'GPU' => 2,
#			'CPU' => 100,
#		}
#		ip => '' #may not exist
#		
#	}
#	+{}
#]
sub applyResources
{
    my ( $this, $conf ) = @_;
    my $db = $this->{db};

	# machine status is active
    #[ uuid, name, id, value, hostname, ip, env ], [ .. ] ..
	my @resources = $db->selectResourcesAndActiveMachineInfo();
	my ( %res, %machine );
	for my $res ( @resources )
	{
		my ( $ip, $name, $id, $value, $hostname, $env ) = @$res;
my $uuid; ##ip
		$machine{$uuid} = +{ hostname => $hostname, ip => $ip, env => $env };
		push @{$res{$uuid}{total}}, +{ name => $name, id => $id, value => $value };
		$res{$uuid}{free}{$name} += $value;

	}
	# [ machineuuid, resourcesname, resourcesid, resourcesvalue, jobuuid ], []
	my @allocated = $db->selectAllocated();
	for my $allocated ( @allocated )
	{
#machineuuid => ip
		my ( $machineuuid, $resourcesname, $resourcesid, $resourcesvalue, $jobuuid ) 
            = @$allocated;
		push @{$res{$machineuuid}{used}}, +{ 
			name => $resourcesname, 
			id => $resourcesid, 
			value => $resourcesvalue 
		};
		$res{$machineuuid}{free}{$resourcesname} -= $resourcesvalue;
	}

	my @rs = ();
	for my $c ( @$conf )
	{
		#{ 
		#	uuid => 'machineuuid',
		# 	res = [
		#		[ 'GPU', '01', 1 ]
		#		[ 'CPU', '0', 200 ]
		#	]
		#

		my %rs = ( jobid => $c->{jobid}, env => $c->{env} );
		my $jobid = $c->{jobid};
		my $env = $c->{env};

		#_findResources rename to _markResourcesOnDB
		my $result = _findResources( \%machine, \%res, $c, 1 );
		unless( $result && $result->{uuid} )
		{
		 	$result = _findResources( \%machine, \%res, $c, 0 );
            if( $result && $result->{uuid} )
            {
                $db->updateMachineEnv( 
                    "$machine{$result->{uuid}}{env}.to.$env.by.$jobid",
                    $result->{uuid}, 
                );
         }
		}
		unless( $result && $result->{uuid} )
		{
			$db->rollback();
			return;
		}

        #标记资源的使用
        for( @{$result->{res}} )
        {
            push @{$res{$result->{uuid}}{used}},
                +{ name => $_->[0], id => $_->[1], value => $_->[2] };
            $res{$result->{uuid}}{free}{$_->[0]} -= $_->[2];
        }


		my %rrss = ( ip => $machine{$result->{uuid}}{ip} );
		for my $r ( @{$result->{res} } )
		{
			$rrss{resources}{"$r->[0]:$r->[1]"} = $r->[2];
			$db->insertAllocatedByJobid( 
				$result->{uuid},  @$r, $jobid
			 );
		}
		$rs{resources} = \%rrss;
		push @rs, \%rs;
	}

	$db->commit();
	return \@rs;
}

sub _findResources
{
	my ( $machine, $resources, $taskid, $config, $checkenv ) = @_;
    #param:
	#machine => +{ machineip => +{ hostname => $hostname, env => $env  }}
	#resources => +{
	#	machineip => +{
	#		total => [ +{ name => '', id => '', value => '' } ]
	#		used => [ +{ name => '', id => '', value => '' } ]
	#		free => +{ name1 => 'value1', name2 => 'value2' }
	#	}
	#}
	#
    #taskid => 'J.20191111.010101.000000.000.000',
	#config => +{
    #       env => 'web1',
    #       resources => +{
	#			GPU => 2,
	#			CPU => 100,
	#		}
	# 		ip => '', may not exist
    #   }
	#checkenv 0 or 1
	
	#result:
 	#    +{ 
	#    	uuid => 'machineuuid',
	#     	res = [
	#    		[ 'GPU', '01', 1 ]
	#    		[ 'CPU', '0', 200 ]
	#    	]
	#    }
	#     or 
	#     +{
	#		 msg => ''
	#		}
	#


    return +{ msg => "nofind ip $config->{ip}" } if $config->{ip} && ! $machine->{$config->{ip}};
	my $machineip;

	for my $ip ( keys %$resources )
	{

		next if $config->{ip} && $config->{ip} ne $ip;
		#if( $checkenv )
		#{
			next unless $machine->{$ip}{env} eq $config->{env};
		#}
		#else
		#{
		#	next unless ! $resources->{$uuid}{used} || @{$resources->{$uuid}{used}} == 0;
		#}

		my $free = $resources->{$ip}{free};
		my $discontent;
		for my $resourcesname ( keys %{$config->{resources}} )
		{
			my $resourcesvalue = $config->{resources}{$resourcesname};
			unless ($free->{$resourcesname} && $free->{$resourcesname} >= $config->{resources}{$resourcesname} )
			{
				$discontent = 1;
				last;
			}
		}
		unless( $discontent )
		{
			$machineip = $ip;
			last;
		}
	}
	return +{ msg => "Not enough resources" } unless $machineip;




	#result:
 	#    [
	#    	[ 'GPU', '01', 1 ]
	#    	[ 'CPU', '0', 200 ]
	#    }


 	my $r = _getResourcesByMachineUuid(  $resources->{$machineip}, $config->{resources} );
    return +{ ip => $machineip, res => $r };
}

#param:
#resources => +{
#		total => [ +{ name => '', id => '', value => '' } ]
#		used => [ +{ name => '', id => '', value => '' } ]
#		free => +{ name1 => 'value1', name2 => 'value2' }
#	}
#}
#config => +{
#		GPU => 2,
#		CPU => 100,
#	}
#}

#result:
#    [
#    	[ 'GPU', '01', 1 ]
#    	[ 'CPU', '0', 200 ]
#    }
#
sub _getResourcesByMachineUuid
{
	my ( $resources, $config ) = @_;
	my ( $total, $used ) = @$resources{qw( total used )};

	my %free; 
	map{ $free{$_->{name}}{$_->{id}} += $_->{value}; }@$total;
	map{ $free{$_->{name}}{$_->{id}} -= $_->{value}; }@$used;

	my $result = [];

	my %match = %$config;
	for my $name ( keys %free )
	{
        next unless $match{$name};
		for my $id ( keys %{$free{$name}} )
		{
			next unless my $value = $free{$name}{$id};
            last unless $match{$name};

			if( $match{$name} > $value )
			{
				$match{$name} -= $value;
				push @$result, [ $name, $id, $value ];
			}
			else
			{
				push @$result, [ $name, $id, delete $match{$name} ];
			}
			
		}
	}
	die "sys error" if keys %match;
	return $result;
}

1;

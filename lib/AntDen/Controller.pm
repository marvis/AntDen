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
    $this{db} = AntDen::Scheduler::DB->new( $this{db} );
    bless \%this, ref $class || $class;
}

sub run
{
    my $this = shift;

#    my $cv = AE::cv;

    my ( $db, $config ) = @$this{qw( db config )};

#    my $ctrw = AnyEvent->timer ( after => 1, interval => 1, cb => sub{

while(1)
{
        my @work = $db->selectControllerWork();

        
        for ( @work )
        {
            #`taskid`,`hostip`,`status`,`expect` 
            my ( $taskid, $hostip, $status, $expect ) = @$_;
            print "taskid: $taskid status: $status expect: $expect\n";
            
                if( $expect eq 'running' )
                {
                    if( $status eq 'init' )
                    {
                        if( $this->_controlSlave( $taskid, $hostip, 'start' ) )
                        {
                            $db->updateControllerStatus( 'starting', $taskid );
                            $db->commit;
                        }

                    }
                    else
                    {
                        next;
                    }
                }
                else #$expect eq 'stoped'
                {
                    if( $status eq 'init' || $status eq 'running' )
                    {
                        if( $this->_controlSlave( $taskid, $hostip, 'stop' ) )
                        {
                            $db->updateControllerStatus( 'stopping', $taskid );
                            $db->commit;
                        }
                    }
                    else
                    {
                        next;
                    }
                }
            
        }
        sleep 1;
}

#        my @running = $db->selectTaskRunning();
#        for ( @running )
#        {
#            my ( $taskid, $executeid ) = @$_;
#            $this->_updateTaskStatus( $taskid, $executeid );
#        }

#    });
#   $cv->recv;
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



#conf => +{
#	task => +{
#		type => 'exec',
#		param => +{
#			exec => 'sleep 100',
#		}
#	},
#	count => 1,
#	scheduler => +{
#		algorithm => 'mydan',
#       env => 'web1.0',
#       resources => +{
#           'GPU' => 2,
#           'CPU' => 100,
#       }
#       ip => '' #may not exist
#	}
#}
#
#result:
#	J.20191111.010101.000000.000.000
#
sub start
{
    my ( $this, $conf ) = @_;
	my ($sec ,$usec ) = gettimeofday;
	my $db =  $this->{db};

	my @applyResources;
	for( 1..$conf->{count})
	{
		my %ar = ( env => $conf->{scheduler}{env}, resources => $conf->{scheduler}{resources} );
		$ar{ip} = $conf->{scheduler}{ip} if $conf->{scheduler}{ip};
		my $jobid = sprintf "%s.%06d.%03d.%03d", POSIX::strftime( "J.%Y%m%d.%H%M%S", localtime( $sec ) ), $usec, rand( 1000 ), $_;
		$ar{jobid} = $jobid;
		$db->insertTask( $jobid, 'init', $$ );

		push @applyResources, \%ar;
	}
	$db->commit();

	$this->_wait2applyResources();
	my $algorithm = $conf->{resources}{algorithm} || 'mydan';
	my $code = "$this->{code}/scheduler/$algorithm/applyResources";
    print "code: $code\n";
	$code = do $code;
	
	die "load code fail" unless $code && ref $code eq 'CODE';

#$VAR1 = [
#          {
#            'jobid' => 'J.20191105.121610.527870.599.001',
#            'resources' => {
#                             'ip' => '127.0.0.1',
#                             'resources' => {
#                                              'GPU:2' => 1,
#                                              'GPU:1' => 1,
#                                              'CPU:0' => 2
#                                            }
#                           },
#            'env' => 'web1.0'
#          },
#          {
#            'jobid' => 'J.20191105.121610.527870.097.002',
#            'env' => 'web1.0',
#            'resources' => {
#                             'resources' => {
#                                              'CPU:0' => 2,
#                                              'GPU:1' => 1,
#                                              'GPU:2' => 1
#                                            },
#                             'ip' => '127.0.0.1'
#                           }
#          }
#        ];
#	or 
#         undef
#
	my $r = &$code( \@applyResources );
	die "applyResources fail" unless $r;
	map{
		$db->updateTaskStatus( 'gotResources', $_->{jobid} );
	}@$r;
	$db->commit();

	for my $re ( @$r )
	{
		print "start task: $re->{jobid}\n";
		my %config = ( %$re, task => $conf->{task} );
		print Dumper \%config;
        YAML::XS::DumpFile "/opt/mydan/dan/job/slave/conf/start/$config{jobid}", \%config;
	}
}

sub _wait2applyResources
{
	return 1;
}
sub stop
{
    my ( $this, $uuid ) = @_;
}

sub clog
{
    my ( $this, $uuid ) = @_;
}

sub log
{
    my ( $this, $uuid ) = @_;
}


1;

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

sub new
{
    my ( $class, %this ) = @_;

    die "error db undefind" unless $this{db};
    die "error task undefind"
        unless $this{task} && -d $this{task};

    $this{db} = AntDen::Slave::DB->new( $this{db} );
    bless \%this, ref $class || $class;
}

sub run
{
    my $this = shift;

    my $cv = AE::cv;

    my $db = $this->{db};

	my $chld = AnyEvent->signal( signal => "CHLD", cb => sub{
        while((my $pid = waitpid(-1, WNOHANG)) >0)
        {
			warn "CHLD $pid exit;";
        }
	});

    my $ctrw = AnyEvent->timer ( after => 1, interval => 1, cb => sub{

        my @work = $db->selectTaskWork();

        for ( @work )
        {
            my ( $taskid, $status, $expect, $executeid, $msg ) = @$_;
            print "taskid: $taskid status: $status expect: $expect\n";

            if( $expect eq 'running' )
            {
                if( $status eq 'init' )
                {
                    $db->updateTaskStatus( 'starting', $taskid );
                    $db->commit;
                    $this->_controlJob( $taskid, $executeid, 'start' );
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
                    $db->updateTaskStatus( 'stopping', $taskid );
                    $db->commit;
                    $this->_controlJob( $taskid, $executeid, 'stop' );
                }
                else
                {
                    next;
                }
            }

        }

        my @running = $db->selectTaskRunning();
        for ( @running )
        {
            my ( $taskid, $executeid ) = @$_;
            $this->_updateTaskStatus( $taskid, $executeid );
        }

    });
   $cv->recv;
}


sub _taskErr
{
    my ( $this, $taskid, $msg  ) = @_;
    $this->{db}->updateTaskStatusAndMsg( 'stoped', $msg, $taskid );
    $this->{db}->commit;
}

sub _controlJob
{
    my ( $this, $taskid, $executeid, $control  ) = @_;
    my ( $db, $task ) = @$this{qw( db task )};
    print Dumper $taskid, $executeid, $control ;

    my $conf = eval{ YAML::XS::LoadFile "$this->{task}/$taskid" };
    if( $@ )
    {
        $this->_taskErr( $taskid, 'load config fail' );
        warn "load config fail";
        return;
    }

    unless( $conf->{task} && ref $conf->{task} eq 'HASH' && $conf->{task}{executer} )
    {
        warn 'nofind executer on config';
        $this->_taskErr( $taskid, 'nofind executer on config' );
        return;
    }

    my $executer = $conf->{task}{executer};

    my $code = $this->_executer( $executer, $control );
    unless( $code && ref $code eq 'CODE' )
    {
        $this->_taskErr( $taskid, "load code $executer/$control fail" );
        return;
    }

    if( $control eq 'start' )
    {
        $executeid = &$code( resources => $conf->{resources}, 
            param => $conf->{task}{param} );
        $db->updateExecuteid( $executeid, $taskid );
        $db->commit;
    }
    else
    {
        unless( $executeid )
        {

            return;
        }
        &$code( executeid => $executeid );
    }
    $this->_updateTaskStatus( $taskid, $executeid );
}

sub _updateTaskStatus
{
    my ( $this, $taskid, $executeid ) = @_;
    my ( $db, $task ) = @$this{qw( db task )};

    my $conf = eval{ YAML::XS::LoadFile "$this->{task}/$taskid" };
    if( $@ )
    {
        $this->_taskErr( $taskid, 'load config fail' );
        return;
    }

    unless( $conf->{task} && ref $conf->{task} eq 'HASH' && $conf->{task}{executer} )
    {
        $this->_taskErr( $taskid, 'nofind executer on config' );
        return;
    }

    my $executer = $conf->{task}{executer};

    my $code = $this->_executer( $executer, 'status' );
    unless( $code && ref $code eq 'CODE' )
    {
        $this->_taskErr( $taskid, "load code $executer/status fail" );
        return;
    }

    my $status = &$code( executeid => $executeid );

    unless( $status && ( $status eq 'running' || $status eq 'stoped' ) )
    {
        $this->_taskErr( $taskid, "get status error" );
        return;
    }

    $db->updateTaskStatus( $status, $taskid );
    $db->commit;
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

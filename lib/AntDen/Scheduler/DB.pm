package AntDen::Scheduler::DB;
use strict;
use warnings;
use Carp;
use DBI;

my %define = (
    machine => [
        ip => 'TEXT NOT NULL',
        hostname => 'TEXT NOT NULL',
        env => 'TEXT NOT NULL',
        status => 'TEXT NOT NULL',
        heartbeat => 'TEXT NOT NULL',
    ],
    resources => [
        ip => 'TEXT NOT NULL', #uniq
        name => 'TEXT NOT NULL',
        id => 'TEXT NOT NULL',
        value => 'TEXT NOT NULL',
    ],
    allocated => [
        ip => 'TEXT NOT NULL',
        name => 'TEXT NOT NULL',
        id => 'TEXT NOT NULL',
        value => 'TEXT NOT NULL',
        taskid => 'TEXT NOT NULL',
    ],
    queue => [
        id => 'INTEGER PRIMARY KEY AUTOINCREMENT',
        jobid => 'TEXT NOT NULL',
        env => 'TEXT NOT NULL',
        nice => 'INTEGER NOT NULL',
        taskcount => 'INTEGER NOT NULL',
        resources => 'TEXT NOT NULL',
        status => 'TEXT NOT NULL', #queuing,cancelled,pause,allocated
    ],
    controller => [
        id => 'INTEGER PRIMARY KEY AUTOINCREMENT',
        taskid => 'TEXT NOT NULL',
        hostip => 'TEXT NOT NULL',
        status => 'INTEGER NOT NULL',#init, running, stoped,starting,stopping
        expect => 'INTEGER NOT NULL', #running stoped
    ],
);

my %stmt = (

    insertQueue => "insert into queue ( `jobid`,`env`,`nice`,`taskcount`,`resources`,`status` ) values(?,?,?,?,?,'queuing')",
    queuingJob => "update queue set status='queuing' where jobid=? and status =='pause'",
    pauseJob => "update queue set status='pause' where jobid=? and status =='queuing'",
    cancelJob => "update queue set status='cancelled' where jobid=? and status !='allocated'",

    showQueue => "select `id`,`jobid`,`env`,`nice`,`taskcount`,`resources`,`status` from queue where status!='allocated'",

    updateQueueNice => "update queue set nice=? where jobid=?",
    stopJobExpect => "update controller set expect=8 where taskid like ?",

	##
    insertMachine => "insert into machine (`ip`,`hostname`,`env`,`status`,`heartbeat`) values(?,?,?,?,?)",
    insertResources => "insert into `resources` ( `ip`,`name`,`id`,`value`) values(?,?,?,?)",

    updateMachineEnv => "update machine set env=? where ip=?",
    updateMachineHeartbeat => "update machine set heartbeat=? where ip=?",


    updateQueueStatus2Allocated => "update queue set status='allocated' where jobid=?",

    selectQueueWork => "select `jobid` from queue where status='queuing' order by nice,id",

    selectMachineByIp => "select * from machine where ip=?",

    #ip, name, id, value, hostname, env
    selectResourcesAndActiveMachineInfo => "select resources.ip,hostname,env,name,id,value from machine,resources where machine.ip=resources.ip and status='active'",
    selectAllocated => "select ip,name,id,value from allocated",
    deleteAllocated => "delete from allocated where taskid=?",
    insertAllocatedByJobid => "insert into allocated values(?,?,?,?,?)",

    selectControllerWork => "select `taskid`,`hostip`,`status`,`expect` from controller where expect>status",
    selectControllerHostip => "select distinct `hostip` from controller",

    updateControllerStatusSafe => "update controller set status=? where taskid=? and status<?",
    updateControllerStatus => "update controller set status=? where taskid=?",
    updateControllerExpect => "update controller set expect=? where taskid=?",


    insertController => "insert into controller (`taskid`,`hostip`,`status`,`expect`)values(?,?,2,5)",
);

sub new
{
    my ( $class, $db ) = splice @_, 0, 2;

    $db = DBI->connect
    ( 
        "DBI:SQLite:dbname=$db", '', '',
        { RaiseError => 1, PrintWarn => 0, PrintError => 0, AutoCommit => 0 }
    );

    my $self = bless { db => $db }, ref $class || $class;

    map { $self->create( $_ ) } keys %define;
    $self->stmt();

    return $self;
}

sub column
{
    my ( $self, $table ) = splice @_;
    return $self->{column}{$table};
}
sub create
{
    my ( $self, $table ) = splice @_;
    my %exist = $self->exist();

    my @define = @{$define{$table}};
    my %column = @{$define{$table}};
    my @column = map { $define[ $_ << 1 ] } 0 .. @define / 2 - 1;

    $self->{column}{$table} = \@column;
    my $db = $self->{db};
    my $neat = DBI::neat( $table );

    unless ( $exist{$table} )
    {
        $db->do
        (
            sprintf "CREATE TABLE $neat ( %s )",
            join ', ', map { "$_ $column{$_}" } @column
        );
        $db->commit();
    }

    return $self;
}

sub stmt
{
    my $self = shift;
    my $db = $self->{db};
    map{ $self->{stmt}{$_} =  $db->prepare( $stmt{$_} ) }keys %stmt;
    return $self;
}

sub AUTOLOAD
{
    my $self = shift;
    return unless our $AUTOLOAD =~ /::(\w+)$/;
    my $name = $1;
    die "sql $name undef" unless my $stmt = $self->{stmt}{$name};
    my @re = @{ $self->execute( $stmt, @_ )->fetchall_arrayref };
    $self->commit() if $name =~ /^select/;
    return @re;
}

sub DESTROY
{
   my $self = shift;
   %$self = ();
}

sub exist
{
    my $self = shift;
    my $exist = $self->{db}->table_info( undef, undef, undef, 'TABLE' )
        ->fetchall_hashref( 'TABLE_NAME' );
    return %$exist; 
}

sub do
{
    my $self = shift;
    $self->execute( $self->{db}->prepare( @_ ) );
}

sub execute
{
    my ( $self, $stmt ) = splice @_, 0, 2;
    while ( $stmt )
    {
        eval { $stmt->execute( @_ ) };
        last unless $@;
        confess $@ if $@ !~ /locked/;
    }
    return $stmt;
}

sub commit
{
    shift->{db}->commit();
}
sub rollback
{
    shift->{db}->rollback();
}
1;

package AntDen::Slave::DB;
use strict;
use warnings;
use Carp;
use DBI;

my %define = (
    task => [
        id => 'INTEGER PRIMARY KEY AUTOINCREMENT',
        taskid => 'TEXT NOT NULL UNIQUE',
        status => 'INTEGER NOT NULL', #init: 1 starting: 2 running: 3 stopping: 4 stoped: 5
        expect => 'INTEGER NOT NULL', #running: 3 stoped: 5
        executeid => 'TEXT NOT NULL',
        msg => 'TEXT NOT NULL',
    ]
);

my %stmt = (
    startTask => "insert into `task` (`taskid`,`status`, `expect`, `executeid`,`msg`) values(?,1,3,'','')",
    stopTask => "update task set expect=5 where taskid=? and expect=3",

    selectTaskWork => "select `taskid`,`status`,`expect`,`executeid`,`msg` from task where expect > status or status==?",

    updateExecuteid => "update task set executeid=?,status=3 where taskid=?",
    updateTaskStatus => "update task set status=? where taskid=?",
    updateTaskMsg => "update task set msg=? where taskid=?",

    selectTask => "select `taskid`,`status`,`expect`,`executeid`,`msg` from task",
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
    map{ $self->{stmt}{$_} = $db->prepare( $stmt{$_} ) }keys %stmt;
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

package AntDen::Controller::DB;
use strict;
use warnings;
use Carp;
use DBI;

my %define = (
    task => [
        id => 'INTEGER PRIMARY KEY AUTOINCREMENT',
        taskid => 'TEXT NOT NULL',
        status => 'TEXT NOT NULL',
        pid => 'TEXT NOT NULL',
    ]
);

my %stmt = (
    insertTask => "insert into `task` (`taskid`,`status`, `pid`) values(?,?,?)",
    updateTaskStatus => "update `task` set status=? where taskid=?"
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

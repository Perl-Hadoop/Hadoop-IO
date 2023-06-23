package Hadoop::IO::SequenceFile::HDFSWriter;

use 5.010;
use strict;
use warnings;

use constant {
    DEFAULT_CHUNK_SIZE => 2**23, # 8 MB
};

=head1 NAME

Hadoop::IO::SequenceFile::HDFSWriter - buffering HDFS file writer.

=head1 DESCRIPTION

This is a simple wrapper around WebHDFS providing a more file-like interface.

=head1 METHODS

=over

=item new

    $class->new($webhdfs, $path, %options)

Construct new HDFSFile writer. Arguments:

=over

=item $webhdfs

An instance of L<Net::Hadoop::WebHDFS>.

=item $path

HDFS path to the destination file. If it doesn't exist it will be created. Can not point at a directory.

=item %opts

Other options:

=over

=item truncate

If true, file will be truncated to zero length. If not set or false, do nothing
here, and write calls just append to the existing file.

=item chunk_size

Buffer this many bytes before actually writing to HDFS. Default is 8 Mb.

=item on_write

This coderef will be called without arguments before each request to WebHDFS service.

=back

=back

=cut

sub new {
    my ($class, $hdfs, $path, %opts) = @_;

    my $truncate = delete $opts{truncate};
    my $chunk_size = delete $opts{chunk_size} // DEFAULT_CHUNK_SIZE;
    my $on_write = delete $opts{on_write};

    die "$class: unknown options: " . join(", ", map "'$_'", keys %opts) if %opts;

    $hdfs->create($path, "", overwrite => "true")
        if $truncate;

    return bless {
        hdfs => $hdfs,
        path => $path,
        chunk_size => $chunk_size,
        on_write => $on_write,
        data => "",
    }, $class;
}

=item write

    $writer->write($data)

Append C<$data> to the file. Buffers up to C<chunk_size> (see L</new>) bytes
before flushing out to HDFS.

Call L</flush> after last write to ensure all data reaches the destination file.

=cut

sub write {
    my ($self, $data, $flush) = @_;

    $self->{data} .= $data;

    if (length $self->{data} >= $self->{chunk_size} || $flush) {
        if (my $cb = $self->{on_write}) {
            $cb->();
        }
        $self->{hdfs}->append($self->{path}, substr $self->{data}, 0, $self->{chunk_size}, "");
    }
}

=item flush

    $writer->flush()

Flush buffered data out to HDFS. Normally you only need to call this once at
the end of all writes.

=cut

sub flush {
    my ($self) = @_;

    $self->write("", 1);
}

=item rename

    $writer->rename($new_name)

Rename destination file to a new name. If C<$new_name> contains '/' character,
path is treated as absolute and used as is, otherwise only last component of
the path is updated, ie file is renamed but stays in the same directory.

=cut

sub rename {
    my ($self, $new_name) = @_;

    if ($new_name !~ m#/#) {
        $new_name = $self->{path} =~ s/[^\/]*$/$new_name/r;
    }

    $self->{hdfs}->delete($new_name)
        or die "pre-rename delete failed";

    $self->{hdfs}->rename($self->{path}, $new_name)
        or die "rename failed";

    $self->{path} = $new_name;
}

=back

=cut

1;

__END__

=pod

=encoding utf8

=cut

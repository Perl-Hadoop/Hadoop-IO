package Hadoop::IO::SequenceFile::BytesWriteable;

use 5.010;
use strict;
use warnings;

=head1 NAME

Hadoop::IO::SequenceFile::BytesWriteable - Hadoop compatible BytesWritable serializer.

=cut

use constant {
    CLASS_NAME => "org.apache.hadoop.io.BytesWritable",
};

=head1 METHODS

=over

=item $class->class_name() -> $string

Get java class name for BytesWriteable.

=cut

sub class_name { CLASS_NAME }

=item $class->encode($data) -> $encoded

Encode a perl string, containing (possibly) binary data into the format compatible with how BytesWriteable is serialized by Hadoop.

=cut

sub encode {
    my ($self, $data) = @_;
    my $len = pack "L>", length $data;
    return $len . $data;
}

=back

=cut

1;

__END__

=pod

=encoding utf8

=cut

package Hadoop::IO::SequenceFile::Text;

use 5.010;
use strict;
use warnings;

use constant {
    CLASS_NAME => "org.apache.hadoop.io.Text",
};

sub class_name { CLASS_NAME }

sub encode {
    my ($self, $data) = @_;
    my $len = _pack_varint(length $data);
    return $len . $data;
}

sub _pack_varint {
    my $value = shift;

    if ($value >= -112 && $value <= 127) {
        return pack "c", $value;
    }

    my $sign;

    if ($value < 0) {
        $sign = -1;
        $value = -$value;
    } else {
        $sign = 1;
    }

    my $pack = pack("Q>", $value) =~ s/^\x00+//r;
    my $mark = pack("c", ($sign > 0 ? -112 : -120) - length $pack);

    return $mark . $pack;
}

1;

__END__

=pod

=encoding utf8

=head1 NAME

Hadoop::IO::SequenceFile::Text - Hadoop compatible Text serializer.

=head1 METHODS

=over 8

=item $class->class_name() -> $string

Get java class name for Text.

=item $class->encode($data) -> $encoded

Encode a perl string, containing (possibly) binary data into the format compatible with how Text is serialized by Hadoop.

=back

=cut


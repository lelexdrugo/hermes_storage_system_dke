#!/usr/bin/perl

use IO::Socket;
$message="Hello, Welcome to Perl";
print("$message \n");

$sock = IO::Socket::INET->new(Proto => "tcp", PeerAddr => "hermes", PeerPort => "65000") or die "$@\n";
print $sock "GET /actuator/health HTTP/1.1\r\n";
print $sock "Host: hermes:65000\r\n";
print $sock "Connection: close\r\n\r\n";
while (my $line = $sock->getline ) {
    if ($line =~ /UP/) {exit;}
}
close $sock;
exit 1;
import urllib2
contents = urllib2.urlopen("http://hermes:65000/actuator/healthcheck").read()
print contents
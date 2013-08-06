from sys import argv
from time import sleep

n = int(argv[1])

print "I'm going to sleep for %i seconds." % n
sleep(n)
print "That was a great %i second nap!" % n

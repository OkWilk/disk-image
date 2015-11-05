import sys
from time import sleep

delay = 0.2
for arg in sys.argv:
    sleep(delay)
    print(str(arg))
    sleep(delay)

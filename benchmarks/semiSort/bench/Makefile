include common/parallelDefs

INCLUDE = -Icommon -Ipbbslib

isortCheck: semisortCheck.C 
	$(CC) $(CFLAGS) $(LFLAGS) $(INCLUDE) -o semisortCheck semisortCheck.C

clean :
	rm -f isortCheck


# Makefile for lib


include $(SEISFS_HOME)/Makefile.config

LIB = $(TARGETA)

ARCH = 	$(LIB)(seisfs_util_io.o)	\
		$(LIB)(seisfs_util_string.o)	\
		$(LIB)(seisfs_util_log.o)


$(LIB)	:	$(ARCH)

list	:
	$(AR) tv $(LIB)

clean:
	rm -f $(LIB)
	rm -f *.o 
	rm -f .depend

.depend:
	touch .depend
	makedepend -- $(INCPATH) -- -f .depend *.cpp *.c
include .depend

.PRECIOUS:	 $(LIB)

# Makefile for lib


include $(GCACHE_HOME)/Makefile.config

LIB = $(TARGETA)

ARCH = 	$(LIB)(gcache_hdfs_utils.o)	\
		$(LIB)(gcache_io.o)	\
		$(LIB)(gcache_log.o)	\
		$(LIB)(gcache_seis_utils.o)	\
		$(LIB)(gcache_string.o)	\
		$(LIB)(gcache_seis_traceupdater.o)	\
		$(LIB)(gcache_seis_tracewriter.o)	

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

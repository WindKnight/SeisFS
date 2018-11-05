# Makefile for lib


include $(GCACHE_HOME)/Makefile.config

LIB = $(TARGETA)

ARCH = 	$(LIB)(gcache_seis_subtracecompactor.o)	\
		$(LIB)(gcache_seis_subtracereader.o)	\
		$(LIB)(gcache_seis_subtraceupdater.o)	\
		$(LIB)(gcache_seis_tracecompactor.o)	\
		$(LIB)(gcache_seis_tracereader.o)	\
		$(LIB)(gcache_seis_traceupdater.o)	\
		$(LIB)(gcache_seis_tracewriter.o)	
all : $(LIB)
	@-rm -f all
	@touch $@


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

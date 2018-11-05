# Makefile for lib


include $(GCACHE_HOME)/Makefile.config

LIB = $(TARGETA)

ARCH = 	$(LIB)(gcache_seis_data.o)	\
		$(LIB)(gcache_seis_error.o)	\
		$(LIB)(gcache_seis_kvinfo.o)	\
		$(LIB)(gcache_seis_merger.o)	\
		$(LIB)(gcache_seis_reader.o)	\
		$(LIB)(gcache_seis_storage.o)	\
		$(LIB)(gcache_seis_truncater.o)	\
		$(LIB)(gcache_seis_writer.o)	
all : $(LIB)
	@touch all
	@rm -f all

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

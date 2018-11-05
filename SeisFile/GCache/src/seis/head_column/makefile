# Makefile for lib


include $(GCACHE_HOME)/Makefile.config

LIB = $(TARGETA)

ARCH = 	$(LIB)(gcache_seis_subheadcompactor_column.o)	\
		$(LIB)(gcache_seis_subheadreader_column.o)	\
		$(LIB)(gcache_seis_subheadupdater_column.o)	\
		$(LIB)(gcache_seis_headcompactor_column.o)	\
		$(LIB)(gcache_seis_headreader_column.o)	\
		$(LIB)(gcache_seis_headupdater_column.o)	\
		$(LIB)(gcache_seis_headwriter_column.o)	

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

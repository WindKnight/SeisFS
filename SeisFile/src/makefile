# Makefile for lib


include ../Makefile.config

LIB = $(TARGETA)

ARCH = 	$(LIB)(seisfile.o)	\
		$(LIB)(seisfile_speed_reader.o)	\
		$(LIB)(seisfile_prl_reader.o)	\
		$(LIB)(seisfile_dis_reader.o)

all : $(LIB)
	@-rm -f all
	@touch $@

$(LIB)	: $(ARCH)

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

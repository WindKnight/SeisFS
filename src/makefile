# Makefile for lib


include $(SEISFS_HOME)/Makefile.config

LIB = $(TARGETA)

ARCH = 	$(LIB)(seisfs_head_filter.o)	\
	$(LIB)(seisfs_row_filter.o)

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

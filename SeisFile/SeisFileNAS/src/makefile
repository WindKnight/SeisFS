# Makefile for lib


include ../Makefile.config

#INCPATH += -I$(GEFILE_HOME_CSSL)/include
#INCPATH += -I/usr/include/libxml2

LIB = $(TARGETA)

ARCH = 	$(LIB)(seisfile_nas.o)	\
		$(LIB)(seisfile_nas_headreader.o)	\
		$(LIB)(seisfile_nas_tracereader.o)	\
		$(LIB)(seisfile_nas_writer.o) \
		$(LIB)(seisfile_nas_meta.o)	\
		$(LIB)(seisfile_nas_reader.o) \
		$(LIB)(seisfile_nas_traceupdater.o)	\
		$(LIB)(seisfile_nas_headupdater.o)	\
		$(LIB)(seisfile_nas_kvinfo.o)	\
		$(LIB)(seisfile_nas_headmerger.o)	\
		$(LIB)(seisfile_nas_tracemerger.o) \
		$(LIB)(seisfile_nas_tracewriter.o)	\
		$(LIB)(seisfile_nas_headwriter.o)

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

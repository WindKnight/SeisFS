# Makefile for lib


include $(SEISCACHE_HOME)/Makefile.config

LIB = $(TARGETA)

ARCH = 	$(LIB)(seiscache_key.o)	\
		$(LIB)(seiscache_row_filter_by_key.o)	\
		$(LIB)(seiscache_kv_info.o)	\
		$(LIB)(seiscache_order.o)	\
		$(LIB)(seiscache_error.o)	\
		$(LIB)(seiscache_data.o)	\
		$(LIB)(seiscache_writer.o)	\
		$(LIB)(seiscache_trace_writer.o)	\
		$(LIB)(seiscache_head_writer.o)	\
		$(LIB)(seiscache_writerbase.o)	\
		$(LIB)(seiscache_reader.o)	\
		$(LIB)(seiscache_trace_reader.o)	\
		$(LIB)(seiscache_head_reader.o)	\
		$(LIB)(seiscache_readerbase.o)	\
		$(LIB)(seiscache_updater.o)	\
		$(LIB)(seiscache_head_updater.o)	\
		$(LIB)(seiscache_trace_updater.o)	\
		$(LIB)(seiscache_updaterbase.o)	\
		$(LIB)(seiscache_speed_readerbase.o)	\
		$(LIB)(seiscache_speed_reader.o)	\
		$(LIB)(seiscache_head_speed_reader.o)	\
		$(LIB)(seiscache_trace_speed_reader.o)	\
		$(LIB)(seiscache_meta.o)

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

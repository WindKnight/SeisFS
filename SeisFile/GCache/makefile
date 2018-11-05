include Makefile.config


all : GCache  dlib 

GCache :
	test -d $(GCACHE_HOME)/lib || mkdir $(GCACHE_HOME)/lib
	cd src/seis ;$(MAKE)
	cd src/seis/trace ;$(MAKE)
	cd src/seis/head_row ;$(MAKE)
	cd src/seis/head_column ;$(MAKE)
	cd src/util ;$(MAKE)
	
dlib :
	test -d .obj || mkdir -p .obj
	-cd .obj ; rm *.o -f ; $(AR) -x $(TARGETA)
	-$(CXXLINKER) $(LDFLAGS) -o $(TARGETDNAME).$(VERSION) .obj/*.o $(LIBS)
	-mv $(TARGETDNAME).$(VERSION) $(TARGETDPATH)
	test -d $(TARGETDPATH)/$(TARGETDNAME) || rm -f $(TARGETDPATH)/$(TARGETDNAME)
	-ln -s $(TARGETDNAME).$(VERSION) $(TARGETDPATH)/$(TARGETDNAME)
#	-echo "INPUT $(TARGETDNAME).$(VERSION)" >  $(TARGETDPATH)/$(TARGETDNAME)
	
clean:
	-cd src/seis;$(MAKE) clean
	-cd src/seis/trace;$(MAKE) clean
	-cd src/seis/head_row ;$(MAKE) clean
	-cd src/seis/head_column ;$(MAKE) clean
	-cd src/util;$(MAKE) clean
	-rm -rf .obj ./lib

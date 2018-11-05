include Makefile.config


all : SeisCacheCompile  dlib 

SeisCacheCompile :
	test -d $(SEISCACHE_HOME)/lib || mkdir $(SEISCACHE_HOME)/lib
	cd src ;$(MAKE)
	
dlib :
	test -d .obj || mkdir -p .obj
	-cd .obj ; rm *.o -f ; $(AR) -x $(TARGETA)
	-$(CXXLINKER) $(LDFLAGS) -o $(TARGETDNAME).$(VERSION) .obj/*.o $(LIBS)
	-mv $(TARGETDNAME).$(VERSION) $(TARGETDPATH)
	test -d $(TARGETDPATH)/$(TARGETDNAME) || rm -f $(TARGETDPATH)/$(TARGETDNAME)
	-ln -s $(TARGETDNAME).$(VERSION) $(TARGETDPATH)/$(TARGETDNAME)
#	-echo "INPUT $(TARGETDNAME).$(VERSION)" >  $(TARGETDPATH)/$(TARGETDNAME)
	
install:
	
clean:
	-cd src; $(MAKE) clean
	-rm -rf .obj ./lib

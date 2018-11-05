include Makefile.config

.PHONY : SeisFSCompile SeisFile SeisCache

all : SeisFSCompile  dlib SeisFile SeisCache

SeisFSCompile :
	test -d $(SEISFS_HOME)/lib || mkdir $(SEISFS_HOME)/lib
	cd src ;$(MAKE)
	cd src/util ; $(MAKE)
	
dlib :
	test -d .obj || mkdir -p .obj
	-cd .obj ; rm *.o -f ; $(AR) -x $(TARGETA)
	-$(CXXLINKER) $(LDFLAGS) -o $(TARGETDNAME).$(VERSION) .obj/*.o $(LIBS)
	-mv $(TARGETDNAME).$(VERSION) $(TARGETDPATH)
	test -d $(TARGETDPATH)/$(TARGETDNAME) || rm -f $(TARGETDPATH)/$(TARGETDNAME)
	-ln -s $(TARGETDNAME).$(VERSION) $(TARGETDPATH)/$(TARGETDNAME)
#	-echo "INPUT $(TARGETDNAME).$(VERSION)" >  $(TARGETDPATH)/$(TARGETDNAME)
	
SeisFile :
	cd SeisFile;$(MAKE)

SeisCache :
	cd SeisCache;$(MAKE)
install:
	
clean:
	-cd src; $(MAKE) clean
	-cd src/util ; $(MAKE) clean
	-cd SeisFile;$(MAKE) clean
	-cd SeisCache;$(MAKE) clean
	-rm -rf .obj ./lib ./bin

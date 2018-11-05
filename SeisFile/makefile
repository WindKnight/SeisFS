include Makefile.config


.PHONY : all SeisFileNAS SeisFileHDFS GCache SeisFileCompile DisFetch

all : SeisFileNAS SeisFileHDFS SeisFileCompile dlib

SeisFileNAS:
	cd SeisFileNAS;$(MAKE)

SeisFileHDFS:
	cd SeisFileHDFS;$(MAKE)

GCache:
	cd GCache ; $(MAKE)

SeisFileCompile :
	test -d $(SEISFILE_HOME)/lib || mkdir $(SEISFILE_HOME)/lib
	cd src ;$(MAKE)
	
dlib :
	test -d .obj || mkdir -p .obj
	-cd .obj ; rm *.o -f ; $(AR) -x $(TARGETA)
	-$(CXXLINKER) $(LDFLAGS) -o $(TARGETDNAME).$(VERSION) .obj/*.o $(LIBS)
	-mv $(TARGETDNAME).$(VERSION) $(TARGETDPATH)
	test -d $(TARGETDPATH)/$(TARGETDNAME) || rm -f $(TARGETDPATH)/$(TARGETDNAME)
	-ln -s $(TARGETDNAME).$(VERSION) $(TARGETDPATH)/$(TARGETDNAME)
#	-echo "INPUT $(TARGETDNAME).$(VERSION)" >  $(TARGETDPATH)/$(TARGETDNAME)
	
DisFetch:
	cd DisFetch ; $(MAKE)
	
install:
	
clean:
	-cd SeisFileNAS; $(MAKE) clean
	-cd SeisFileHDFS; $(MAKE) clean
#	-cd GCache ; $(MAKE) clean
	-cd src; $(MAKE) clean
#	-cd DisFetch ; $(MAKE) clean
	-rm -rf .obj ./lib

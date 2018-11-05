include Makefile.config


all : SeisFileNAS  dlib 

SeisFileNAS :
	test -d $(SEISFILENAS_HOME)/lib || mkdir $(SEISFILENAS_HOME)/lib
	-cd src ;$(MAKE)
	
dlib :
	test -d .obj || mkdir -p .obj
	-cd .obj ; rm *.o -f ; $(AR) -x $(TARGETA)
	-$(CXXLINKER) $(LDFLAGS) -o $(TARGETDNAME).$(VERSION) .obj/*.o $(LIBS)
	-mv $(TARGETDNAME).$(VERSION) $(TARGETDPATH)
	test -d $(TARGETDPATH)/$(TARGETDNAME) || rm -f $(TARGETDPATH)/$(TARGETDNAME)
	-ln -s $(TARGETDNAME).$(VERSION) $(TARGETDPATH)/$(TARGETDNAME)
#	-echo "INPUT $(TARGETDNAME).$(VERSION)" >  $(TARGETDPATH)/$(TARGETDNAME)
	
clean:
	-cd src; $(MAKE) clean
	-rm -rf .obj ./lib

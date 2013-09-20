.PHONY: clean all java-src

all: CCouch3.jar

java-src:

java-bin: java-src
	rm -rf java-bin
	find java-src -name *.java >java-src.lst
	mkdir -p java-bin
	javac -source 1.6 -target 1.6 -sourcepath java-src -d java-bin @java-src.lst

CCouch3.jar: java-bin
	jar -ce togos.ccouch3.CCouch3Command -C java-bin . >CCouch3.jar

clean:
	rm -rf java-bin CCouch3.jar ccouch3.jar

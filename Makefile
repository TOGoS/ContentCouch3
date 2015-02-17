.PHONY: clean default
.DELETE_ON_ERROR:

default: CCouch3.jar.urn

CCouch3.jar: $(shell find src/main)
	rm -rf "target"
	find src/main -name *.java >java-src.lst
	mkdir -p "target"
	javac -source 1.6 -target 1.6 -sourcepath src/main/java -d target @java-src.lst
	jar -ce togos.ccouch3.CCouch3Command -C target . >CCouch3.jar

%.urn: % CCouch3.jar
	java -jar CCouch3.jar id "$<" >"$@"

clean:
	rm -rf target CCouch3.jar ccouch3.jar CCouch3.jar.urn java-bin

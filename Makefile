# May need to override this on Windows to the full path to git.exe, e.g. by running
# > set git_exe="C:\Program Files (x86)\Git\bin\git.exe"
# before running make.
git_exe?=git

default: CCouch3.jar.urn

.PHONY: \
	clean \
	default \
	update-libraries

.DELETE_ON_ERROR:

CCouch3.jar: $(shell find src/main)
	rm -rf "target"
	find src/main ext-lib/*/src/main -name *.java >java-src.lst
	mkdir -p "target"
	javac -source 1.6 -target 1.6 -sourcepath src/main/java -d target @java-src.lst
	jar -ce togos.ccouch3.CCouch3Command -C target . >CCouch3.jar

%.urn: % CCouch3.jar
	java -ea -jar CCouch3.jar id "$<" >"$@"

clean:
	rm -rf target CCouch3.jar ccouch3.jar CCouch3.jar.urn java-bin

update-libraries:
	${git_exe} subtree pull --prefix=ext-lib/ByteBlob https://github.com/TOGoS/ByteBlob.git master

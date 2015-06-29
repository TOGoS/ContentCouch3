# May need to override this on Windows to the full path to git.exe, e.g. by running
# > set git_exe="C:\Program Files (x86)\Git\bin\git.exe"
# before running make.
git_exe ?= git
javac   := javac -source 1.6 -target 1.6
ccouch3 := java -jar CCouch3.jar
# On windows, this may need to be changed to ';'
pathsep := :
temporary_files = \
	.*-src.lst \
	CCouch3.jar \
	ccouch3.jar \
	CCouch3.jar.urn \
	ext-lib/JUnit.jar \
	java-bin \
	java-src.lst \
	target

default: CCouch3.jar.urn

.PHONY: \
	clean \
	default \
	update-libraries

.DELETE_ON_ERROR:

CCouch3.jar: $(shell find src/main)
	rm -rf target/main
	find src/main ext-lib/*/src/main -name *.java >.java-main-src.lst
	mkdir -p target/main
	${javac} -sourcepath src/main/java -d target/main @.java-main-src.lst
	jar -ce togos.ccouch3.CCouch3Command -C target/main . >CCouch3.jar

ext-lib/JUnit.jar: ext-lib/JUnit.jar.urn | CCouch3.jar
	${ccouch3} cache -remote-repo @build-resource-repos.lst -sector build `cat "$<"`
	${ccouch3} copy -repo ~/.ccouch `cat "$<"` "$@"

CCouch3.jar.urn: CCouch3.jar
	java -ea -jar CCouch3.jar id "$<" >"$@"

clean:
	rm -rf ${temporary_files}

update-libraries:
	${git_exe} subtree pull --prefix=ext-lib/ByteBlob https://github.com/TOGoS/ByteBlob.git master

unit_test_dependencies := ext-lib/JUnit.jar

test-dependencies: ${unit_test_dependencies}

run-unit-tests: ${unit_test_dependencies}
	rm -rf target/test
	find src/main src/test ext-lib/*/src/main -name *.java >.java-test-src.lst
	mkdir -p target/test
	${javac} -cp ext-lib/JUnit.jar -sourcepath src/main/java -sourcepath src/test/java -d target/test @.java-test-src.lst
	find src/test -name *Test.java | sed -e s=src/test/java/==g -e s/.java// -e s=/=.=g | xargs \
		java -cp target/test${pathsep}ext-lib/JUnit.jar junit.textui.TestRunner

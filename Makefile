# May need to override this on Windows to the full path to git.exe, e.g. by running
# > set git_exe="C:\Program Files (x86)\Git\bin\git.exe"
# before running make.

git_exe ?= git
find_exe ?= find
# On windows, this may need to be changed to ';'
pathsep ?= :

touch   := touch
javac   := javac -source 1.6 -target 1.6
ccouch3 := java -jar CCouch3.jar

temporary_files = \
	.*-src.lst \
	CCouch3.jar \
	ccouch3.jar \
	CCouch3.jar.urn \
	ext-lib/JUnit.jar \
	fetch-jar \
	java-bin \
	java-src.lst \
	target
fetch_deps = .ccouch-repos.lst
fetch = java -jar TJFetcher.jar -repo @.ccouch-repos.lst

.PHONY: default
default: CCouch3.jar.urn

.DELETE_ON_ERROR:

target/main.built: $(shell "${find_exe}" src/main ext-lib/*/src/main)
	rm -rf target/main
	"${find_exe}" src/main ext-lib/*/src/main -name '*.java' >.java-main-src.lst
	mkdir -p target/main
	${javac} -sourcepath src/main/java -d target/main @.java-main-src.lst
	${touch} "$@" 

CCouch3.jar: target/main.built
	rm -f "$@"
	jar -ce togos.ccouch3.CCouch3Command -C target/main . >CCouch3.jar

CCouch3.jar.urn: CCouch3.jar
	java -ea -jar CCouch3.jar id "$<" >"$@"

.PHONY: publish-jar
publish-jar: CCouch3.jar CCouch3.jar.urn
	publish CCouch3.jar

.ccouch-repos.lst: | .ccouch-repos.lst.example
	cp "$|" "$@"

ext-lib/JUnit.jar: ext-lib/JUnit.jar.urn ${fetch_deps} | CCouch3.jar
	${fetch} urn:bitprint:TEJJ6FSEFBCPNJFBDLRC7O7OICYU252P.XZMJNNSDGM456CHQCJI22DUDWYK6ZASNPLJ26GA -o "$@"

.PHONY: fetch-jar
fetch-jar: ${fetch_deps}
	rm -f CCouch3.jar
	git checkout CCouch3.jar.urn
	${fetch} @CCouch3.jar.urn -o CCouch3.jar

.PHONY: clean
clean:
	rm -rf ${temporary_files}

.PHONY: update-libraries
update-libraries:
	${git_exe} subtree pull --prefix=ext-lib/ByteBlob https://github.com/TOGoS/ByteBlob.git master

unit_test_dependencies := ext-lib/JUnit.jar

.PHONY: test-dependencies
test-dependencies: ${unit_test_dependencies}

.PHONY: run-unit-tests
run-unit-tests: ${unit_test_dependencies}
	rm -rf target/test
	"${find_exe}" src/main src/test ext-lib/*/src/main -name *.java >.java-test-src.lst
	mkdir -p target/test
	${javac} -cp ext-lib/JUnit.jar -sourcepath src/main/java -sourcepath src/test/java -d target/test @.java-test-src.lst
	"${find_exe}" src/test -name *Test.java | sed -e s=src/test/java/==g -e s/.java// -e s=/=.=g | xargs \
		java -cp target/test${pathsep}ext-lib/JUnit.jar junit.textui.TestRunner

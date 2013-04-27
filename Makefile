CCouch3.jar:
	find java-src -name *.java >java-src.lst
	mkdir -p java-bin
	javac -source 1.6 -target 1.6 -sourcepath java-src -d java-bin @java-src.lst
	jar -ce togos.ccouch3.CCouch3Command -C java-bin . >CCouch3.jar

clean:
	rm -rf java-bin CCouch3.jar ccouch3.jar

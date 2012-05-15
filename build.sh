#!/bin/sh

find java-src -name *.java >java-src.lst
mkdir -p java-bin
javac -sourcepath java-src -d java-bin @java-src.lst
jar -ce togos.ccouch3.CCouch3Command -C java-bin . >ccouch3.jar

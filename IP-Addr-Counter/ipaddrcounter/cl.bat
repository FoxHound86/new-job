javac IPAddrCounter.java
jar   -cfm ipaddrcounter.jar manifest.txt *.class
java  -jar ipaddrcounter.jar -h
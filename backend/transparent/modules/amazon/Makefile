#
# Unix/Linux makefile for COS 333 project
#

.SUFFIXES: .java .class
SRCS = \
	AmazonParser.java

#
# Compile and link options
#

JC=javac
JFLAGS=-cp jsoup-1.7.2.jar

#
# Compile command
#

%.class : %.java
		${JC} ${JFLAGS} $<

#
# GNU Make: targets that don't build files
#

.PHONY: default all clean

#
# Make targets
#

default: all

all: ${SRCS:.java=.class}

clean:
		${RM} -f *.class


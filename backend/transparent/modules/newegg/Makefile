#
# Unix/Linux makefile for COS 333 project
#

.SUFFIXES: .java .class
SRCS = \
	NeweggParser.java

#
# Compile and link options
#

JC=javac
JFLAGS=

#
# Compile command
#

.java.class:
		${JC} ${JFLAGS} -cp json-smart-1.1.1.jar:jsoup-1.7.2.jar $*.java

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


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
		${JC} ${JFLAGS} $*.java

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


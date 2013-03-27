#
# Unix/Linux makefile for COS 333 project
#

.SUFFIXES: .java .class
SRCS = \
	core/Core.java

#
# Compile and link options
#

JC=javac
JFLAGS=
MODULES=newegg

#
# Compile command
#

.java.class:
		${JC} ${JFLAGS} $*.java

#
# GNU Make: targets that don't build files
#

.PHONY: default all clean modules core

#
# Make targets
#

default: all

all: core modules

modules:
		${MAKE} -C modules/${MODULES} all

core: ${SRCS:.java=.class}

clean:
		${RM} -f core/*.class; ${MAKE} -C modules/${MODULES} clean


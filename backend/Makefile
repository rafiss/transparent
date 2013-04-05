#
# Unix/Linux makefile for COS 333 project
#

SRCS = \
	transparent/core/Module.java \
	transparent/core/Sandbox.java \
	transparent/core/NoSandbox.java \
	transparent/core/Core.java \
	transparent/core/Interruptable.java \
	transparent/core/InterruptableInputStream.java \
	transparent/core/InterruptedStreamException.java \
	transparent/core/ModuleThread.java \
	transparent/core/StreamPipe.java \
	transparent/core/database/Database.java \
	transparent/core/database/MariaDBDriver.java

#
# Compile and link options
#

JC=javac
JFLAGS=
MODULES=newegg

#
# Compile command
#

%.class : %.java
		${JC} ${JFLAGS} $<

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
		${MAKE} -C transparent/modules/${MODULES} all

core: ${SRCS:.java=.class}

clean:
		${RM} -f core/*.class; ${MAKE} -C transparent/modules/${MODULES} clean


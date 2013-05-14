#
# Unix/Linux makefile for COS 333 project
#

SRCS = \
	transparent/core/Module.java \
	transparent/core/Sandbox.java \
	transparent/core/NoSandbox.java \
	transparent/core/Core.java \
	transparent/core/Task.java \
	transparent/core/Server.java \
	transparent/core/Console.java \
	transparent/core/PriceTrigger.java \
	transparent/core/PriceHistory.java \
	transparent/core/Interruptable.java \
	transparent/core/InterruptableInputStream.java \
	transparent/core/InterruptedStreamException.java \
	transparent/core/ModuleThread.java \
	transparent/core/StreamPipe.java \
	transparent/core/ProductID.java \
	transparent/core/database/Database.java \
	transparent/core/database/MariaDBDriver.java

#
# Compile and link options
#

JC=javac
JFLAGS=-cp lib/mariadb-java-client-1.1.1.jar:lib/jansi-1.9.jar:lib/jline-1.0.jar:lib/jcurses.jar:lib/simple-5.1.1.jar:lib/json-smart-1.1.1.jar:lib/commons-pool-1.6.jar:lib/jedis-2.1.0.jar:.
MODULES=newegg amazon

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

modules: ; $(foreach dir,$(MODULES),${MAKE} -C transparent/modules/$(dir) all &&) :

core: ${SRCS:.java=.class}

clean:
	$(foreach dir,$(MODULES),${MAKE} -C transparent/modules/$(dir) clean &&) :
	${RM} -f transparent/core/*.class;
	${RM} -f transparent/core/database/*.class;

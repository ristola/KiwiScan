#
#
#   Makefile
#
#   Copyright (C) 2023-2025 by Matt Roberts.
#   License: GNU GPL3 (www.gnu.org)
#
#

CXX ?= c++

# various build target groups
TARGETS1=ft8modem
TARGETS2=ft8encode
TARGETS3=test_abstime test_encode cpucores
TARGETS4=test_decode decimate
TARGETS5=test_cosine test_es
TARGETS6=af2udp
TARGETS7=ft8tail

# these are the Python scripts to install
SCRIPTS=ft8cat ft8collect ft8encode ft8modem ft8qso ft8report ft8sdr ft8swl ft8range onlycalls

# these are the python support modules to install
SCRIPT_MODULES=\
	alltxt.py bands.py callsigns.py computefb.py \
	conflicts.py fdutils.py fragment.py messages.py \
	parsing.py pretty.py rigctld.py spots.py prefixes.py \
	prefixes.txt tailall.py tailfile.py udpaf.py version.py \
	gridsquares.py

# ---- Installation layout (portable) ----
PREFIX  ?= /usr/local
DESTDIR ?=
BINDIR  := $(DESTDIR)$(PREFIX)/bin
LIBDIR  := $(DESTDIR)$(PREFIX)/lib/ft8modem

# Portable install helpers
INSTALL         ?= install
INSTALL_PROGRAM ?= $(INSTALL) -m 0755
INSTALL_SCRIPT  ?= $(INSTALL) -m 0755
INSTALL_DATA    ?= $(INSTALL) -m 0644

# dependencies
TARGETS=$(TARGETS1) $(TARGETS2) $(TARGETS3) $(TARGETS4) $(TARGETS5) $(TARGETS6) $(TARGETS7)

# ==== Objects (removed nlimits.o — no source present) ====
OBJECTS1=encode.o pipes.o snddev.o
OBJECTS2=encode.o
OBJECTS3=encode.o
OBJECTS4=pipes.o
OBJECTS5=
OBJECTS6=
OBJECTS7=

# libs (pkg-config supplies frameworks/paths on macOS)
LIBS1=$(shell pkg-config --libs rtaudio sndfile) -lpthread -lm
LIBS2=$(shell pkg-config --libs sndfile) -lm
LIBS3=-lm
LIBS4=$(shell pkg-config --libs sndfile) -lpthread -lm
LIBS5=$(shell pkg-config --libs sndfile) -lm
LIBS6=-lm
LIBS7=

# build flags for C/C++ debugging
CDEBUG=-Wall -ggdb -D_DEBUG

# --- IMPORTANT FLAGS ---
CXXFLAGS += -std=c++17

UNAME_S := $(shell uname -s)
RTAUDIO_PLATFORM_DEFINE :=
ifeq ($(UNAME_S),Darwin)
RTAUDIO_PLATFORM_DEFINE := -D__MACOSX_CORE__
endif
ifeq ($(UNAME_S),Linux)
RTAUDIO_PLATFORM_DEFINE := -D__LINUX_ALSA__
endif

# Pull in header search paths/defines for sndfile & rtaudio
CPPFLAGS += $(shell pkg-config --cflags rtaudio sndfile) -pthread $(RTAUDIO_PLATFORM_DEFINE)

# rule patterns
.cpp.o:
	$(CXX) -Wall $(CFLAGS) $(CDEBUG) -c $(CPPFLAGS) $(CXXFLAGS) $<
.cc.o:
	$(CXX) -Wall $(CFLAGS) $(CDEBUG) -c $(CPPFLAGS) $(CXXFLAGS) $<
.c.o:
	cc -Wall $(CFLAGS) $(CDEBUG) -c $<

# default target
all: $(TARGETS)

# target group rules
$(TARGETS1): %: %.o $(OBJECTS1)
	$(CXX) $(CFLAGS) $(CDEBUG) -o $@ $@.o $(OBJECTS1) $(LIBS1)

$(TARGETS2): %: %.o $(OBJECTS2)
	$(CXX) $(CFLAGS) $(CDEBUG) -o $@ $@.o $(OBJECTS2) $(LIBS2)

$(TARGETS3): %: %.o $(OBJECTS3)
	$(CXX) $(CFLAGS) $(CDEBUG) -o $@ $@.o $(OBJECTS3) $(LIBS3)

$(TARGETS4): %: %.o $(OBJECTS4)
	$(CXX) $(CFLAGS) $(CDEBUG) -o $@ $@.o $(OBJECTS4) $(LIBS4)

$(TARGETS5): %: %.o $(OBJECTS5)
	$(CXX) $(CFLAGS) $(CDEBUG) -o $@ $@.o $(OBJECTS5) $(LIBS5)

$(TARGETS6): %: %.o $(OBJECTS6)
	$(CXX) $(CFLAGS) $(CDEBUG) -o $@ $@.o $(OBJECTS6) $(LIBS6)

$(TARGETS7): %: %.o $(OBJECTS7)
	cc $(CFLAGS) $(CDEBUG) -o $@ $@.o $(OBJECTS7) $(LIBS7)

.PHONY: all clean strip rebuild install uninstall dep depend ignore

# meta-targets
clean:
	rm -f $(TARGETS) core *.o *.wav
	rm -f Makefile.bak decoded.txt jt9_wisdom.dat timer.out avemsg.txt gmon.out
	rm -f ALL.TXT ALL_WSPR.TXT hashtable.txt wspr_spots.txt wspr_timer.out wspr_wisdom.dat
	rm -rf __pycache__

strip: $(TARGETS)
	strip --strip-unneeded $(TARGETS)

rebuild:
	$(MAKE) -j1 clean
	$(MAKE)

install: strip
	@echo "Installing to $(PREFIX) (override with PREFIX=/some/where). Use sudo if needed."
	# ensure dirs
	$(INSTALL) -d "$(BINDIR)" "$(LIBDIR)"
	# install the compiled executables into $(BINDIR)
	for i in $(TARGETS1) $(TARGETS2) $(TARGETS6) $(TARGETS7) ; do \
	  $(INSTALL_PROGRAM) "$$i" "$(BINDIR)/$$i" ; \
	done
	# install the scripts into $(LIBDIR)
	for i in $(SCRIPTS) ; do \
	  $(INSTALL_SCRIPT) "$$i" "$(LIBDIR)/$$i" ; \
	done
	# create/refresh ABSOLUTE symlinks in $(BINDIR) for ALL scripts
	for i in $(SCRIPTS) ; do \
	  ln -sf "$(LIBDIR)/$$i" "$(BINDIR)/$$i" ; \
	done
	# install python support modules into $(LIBDIR)
	for i in $(SCRIPT_MODULES) ; do \
	  $(INSTALL_DATA) "$$i" "$(LIBDIR)/$$i" ; \
	done

uninstall:
	# remove symlinks for scripts
	for i in $(SCRIPTS) ; do rm -vf "$(BINDIR)/$$i" ; done
	# remove installed binaries
	for i in $(TARGETS1) $(TARGETS2) $(TARGETS6) $(TARGETS7) ; do rm -vf "$(BINDIR)/$$i" ; done
	# remove script files & modules from libdir
	for i in $(SCRIPTS) $(SCRIPT_MODULES) ; do rm -vf "$(LIBDIR)/$$i" ; done

dep depend:
	makedepend -Y *.c *.cc *.cpp 2>/dev/null

ignore:
	svn propset svn:ignore -F .svnignore .

# EOF - everything below this line is from 'makedepend'
# DO NOT DELETE

af2udp.o: version.h
cpucores.o: cores.h stype.h
decimate.o: decimate.h sf.h stype.h
encode.o: stype.h jt65.h encode.h
ft8encode.o: encode.h mfsk.h shape.h nlimits.h IFilter.h osc.h es.h sf.h
ft8encode.o: stype.h presets.h
ft8modem.o: snddev.h sc.h mfsk.h shape.h nlimits.h IFilter.h osc.h es.h
ft8modem.o: decode.h sf.h stype.h clock.h cores.h locker.h pathtools.h
ft8modem.o: pipes.h encode.h FirFilter.h WindowFunctions.h FilterTypes.h
ft8modem.o: FilterUtils.h decimate.h presets.h level.h version.h
pipes.o: stype.h locker.h pipes.h
snddev.o: snddev.h sc.h mfsk.h shape.h nlimits.h IFilter.h osc.h es.h
snddev.o: decode.h sf.h stype.h clock.h cores.h locker.h pathtools.h pipes.h
snddev.o: encode.h FirFilter.h WindowFunctions.h FilterTypes.h FilterUtils.h
snddev.o: decimate.h presets.h level.h
test_abstime.o: clock.h
test_cosine.o: shape.h nlimits.h IFilter.h
test_decode.o: decode.h sf.h stype.h clock.h cores.h locker.h pathtools.h
test_decode.o: pipes.h
test_encode.o: encode.h
test_es.o: es.h nlimits.h IFilter.h
nlimits.o: nlimits.h
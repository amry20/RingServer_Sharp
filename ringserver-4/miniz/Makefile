
# Build environment can be configured the following
# environment variables:
#   CC : Specify the C compiler to use
#   CFLAGS : Specify compiler options to use

# This Makefile is NOT part of miniz, it is distributed with ringserver
# to build the miniz library sources needed by ringserver.
#
# See README-ringserver.md for more information.

MINIZ_SRCS = $(wildcard *.c)
MINIZ_OBJS = $(MINIZ_SRCS:.c=.o)

# Miniz build options — disable unused ZIP/stdio/time/zlib-compatible code
CFLAGS += -DMINIZ_NO_ARCHIVE_APIS -DMINIZ_NO_STDIO -DMINIZ_NO_TIME \
          -DMINIZ_NO_ZLIB_COMPATIBLE_NAMES

all: $(MINIZ_OBJS)

# Standard object building
.c.o:
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(MINIZ_OBJS)

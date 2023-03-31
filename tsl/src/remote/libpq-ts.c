/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <pg_config.h>
#include <postgres_fe.h>
#include <port/pg_bswap.h>

#ifdef WIN32
/* The internal libpq header file has a reference to pthread-win32.h which is
 * not installed with the other files on WIN32. To avoid including the missing
 * file, undefine thread safety. It is not needed for our purposes anyway. */
#ifdef ENABLE_THREAD_SAFETY
#define ENABLE_THREAD_SAFETY_SETTING ENABLE_THREAD_SAFETY
#undef ENABLE_THREAD_SAFETY
#endif
#endif /* WIN32 */

#include <internal/libpq-int.h>

#ifdef ENABLE_THREAD_SAFETY_SETTING
#define ENABLE_THREAD_SAFETY ENABLE_THREAD_SAFETY_SETTING
#endif

#include "libpq-ts.h"

/*
 * This file contains Timescale versions of functions that are available in
 * the internal libpq API and header.
 *
 * In the <internal/libpq-int.h> header, it is stated that it is possible for
 * applications to include the internal API at their own risk (the risk being
 * potential breakage between PG versions). However, the libpq library exludes
 * the internal functions from the list of exported symbols for some reason,
 * so they need to be provided here with Timescale naming.
 *
 * Timescale uses the internal API for better control over send buffers, e.g.,
 * to construct bigger CopyData messages in libpq buffers without having to
 * maintain additional buffers outside libpq, which would increase memory
 * usage and require additional data copying across the different buffers.
 */

/*
 * Grow the output buffer.
 *
 * Returns -1 on failure, 0 if buffer could not be grown, or 1 on success.
 */
int
ts_grow_output_buffer(PGconn *conn, size_t len)
{
	if ((conn->outBufSize - conn->outCount - 5) < (int64) len)
	{
		if (PQflush(conn) < 0)
			return -1;

		if (ts_check_out_buffer_space(conn->outCount + 5 + (size_t) len, conn))
			return PQisnonblocking(conn) ? 0 : -1;
	}

	return 1;
}

/*
 * Timescale version of pqCheckOutBufferSpace().
 */
int
ts_check_out_buffer_space(size_t bytes_needed, PGconn *conn)
{
	int newsize = conn->outBufSize;
	char *newbuf;

	/* Quick exit if we have enough space */
	if (bytes_needed <= (size_t) newsize)
		return 0;

	/*
	 * If we need to enlarge the buffer, we first try to double it in size; if
	 * that doesn't work, enlarge in multiples of 8K.  This avoids thrashing
	 * the malloc pool by repeated small enlargements.
	 *
	 * Note: tests for newsize > 0 are to catch integer overflow.
	 */
	do
	{
		newsize *= 2;
	} while (newsize > 0 && bytes_needed > (size_t) newsize);

	if (newsize > 0 && bytes_needed <= (size_t) newsize)
	{
		newbuf = realloc(conn->outBuffer, newsize);
		if (newbuf)
		{
			/* realloc succeeded */
			conn->outBuffer = newbuf;
			conn->outBufSize = newsize;
			return 0;
		}
	}

	newsize = conn->outBufSize;
	do
	{
		newsize += 8192;
	} while (newsize > 0 && bytes_needed > (size_t) newsize);

	if (newsize > 0 && bytes_needed <= (size_t) newsize)
	{
		newbuf = realloc(conn->outBuffer, newsize);
		if (newbuf)
		{
			/* realloc succeeded */
			conn->outBuffer = newbuf;
			conn->outBufSize = newsize;
			return 0;
		}
	}

	/* realloc failed. Probably out of memory */
	//appendPQExpBufferStr(&conn->errorMessage, "cannot allocate memory for output buffer\n");

	return EOF;
}

/*
 * Timescale version of pqPutMsgStart(). Note that the PG/libpq function has a
 * different signature between PG13 and PG14.
 */
int
ts_put_msg_start(char msg_type, PGconn *conn)
{
	int lenPos;
	int endPos;

	/* allow room for message type byte */
	if (msg_type)
		endPos = conn->outCount + 1;
	else
		endPos = conn->outCount;

	/* do we want a length word? */
	lenPos = endPos;
	/* allow room for message length */
	endPos += 4;

	/* make sure there is room for message header */
	if (ts_check_out_buffer_space(endPos, conn))
		return EOF;
	/* okay, save the message type byte if any */
	if (msg_type)
		conn->outBuffer[conn->outCount] = msg_type;
	/* set up the message pointers */
	conn->outMsgStart = lenPos;
	conn->outMsgEnd = endPos;
	/* length word, if needed, will be filled in by pqPutMsgEnd */

	return 0;
}

/*
 * Timescale version of pqPutMsgBytes().
 */
static int
ts_put_msg_bytes(const void *buf, size_t len, PGconn *conn)
{
	/* make sure there is room for it */
	if (ts_check_out_buffer_space(conn->outMsgEnd + len, conn))
		return EOF;
	/* okay, save the data */
	memcpy(conn->outBuffer + conn->outMsgEnd, buf, len);
	conn->outMsgEnd += len;
	/* no Pfdebug call here, caller should do it */
	return 0;
}

/*
 * Timescale version of pqPutnchar().
 */
int
ts_putnchar(const char *s, size_t len, PGconn *conn)
{
	if (ts_put_msg_bytes(s, len, conn))
		return EOF;

	return 0;
}

/*
 * Timescale version of pgPutMsgEnd().
 */
int
ts_put_msg_end(PGconn *conn)
{
	/* Fill in length word if needed */
	if (conn->outMsgStart >= 0)
	{
		uint32 msgLen = conn->outMsgEnd - conn->outMsgStart;

		msgLen = pg_hton32(msgLen);
		memcpy(conn->outBuffer + conn->outMsgStart, &msgLen, 4);
	}

	/* Make message eligible to send */
	conn->outCount = conn->outMsgEnd;

	return 0;
}

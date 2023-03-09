/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres_fe.h>
#include <libpq-fe.h>
#include <internal/libpq-int.h>
#include <port/pg_bswap.h>

/*
 * This file contains functions that are available in the internal libpq API,
 * but are not exported symbols in the shared library.
 *
 * In the internal/libpq-int.h header, it is stated that it is possible for
 * applications to include the internal API at their own risk (the risk being
 * potential breakage between PG versions). However, the library exludes those
 * functions from the list of exported symbols for some reason.
 */
int
pqCheckOutBufferSpace(size_t bytes_needed, PGconn *conn)
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
	appendPQExpBufferStr(&conn->errorMessage, "cannot allocate memory for output buffer\n");
	return EOF;
}

extern int ts_pqPutMsgStart(char msg_type, PGconn *conn);

/*
 * pqPutMsgStart has a different signature between PG13 and PG14, so need to
 * use our own name here.
 */
int
ts_pqPutMsgStart(char msg_type, PGconn *conn)
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
	if (pqCheckOutBufferSpace(endPos, conn))
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

static int
pqPutMsgBytes(const void *buf, size_t len, PGconn *conn)
{
	/* make sure there is room for it */
	if (pqCheckOutBufferSpace(conn->outMsgEnd + len, conn))
		return EOF;
	/* okay, save the data */
	memcpy(conn->outBuffer + conn->outMsgEnd, buf, len);
	conn->outMsgEnd += len;
	/* no Pfdebug call here, caller should do it */
	return 0;
}

int
pqPutnchar(const char *s, size_t len, PGconn *conn)
{
	if (pqPutMsgBytes(s, len, conn))
		return EOF;

	return 0;
}

int
pqPutMsgEnd(PGconn *conn)
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

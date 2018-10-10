#ifndef TIMESCALEDB_BGW_MESSAGE_QUEUE_H
#define TIMESCALEDB_BGW_MESSAGE_QUEUE_H

#include <postgres.h>
#include <storage/dsm.h>


typedef enum BgwMessageType
{
	STOP = 0,
	START,
	RESTART,
} BgwMessageType;

typedef struct BgwMessage
{
	BgwMessageType message_type;

	pid_t		sender_pid;
	Oid			db_oid;
	dsm_handle	ack_dsm_handle;
} BgwMessage;

extern bool bgw_message_send_and_wait(BgwMessageType message, Oid db_oid);

/* called only by the launcher*/
extern void bgw_message_queue_set_reader(void);
extern BgwMessage *bgw_message_receive(void);
extern void bgw_message_send_ack(BgwMessage *message, bool success);

/*called at server startup*/
extern void bgw_message_queue_alloc(void);

/*called in every backend during shmem startup hook*/
extern void bgw_message_queue_shmem_startup(void);
extern void bgw_message_queue_shmem_cleanup(void);


#endif							/* TIMESCALEDB_BGW_MESSAGE_QUEUE_H */

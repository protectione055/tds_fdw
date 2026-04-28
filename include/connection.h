#ifndef TDS_CONNECTION_H
#define TDS_CONNECTION_H

#include "postgres.h"

#include <sybfront.h>
#include <sybdb.h>

#include "options.h"

typedef int (*TdsDblibErrHandler) (DBPROCESS *dbproc,
						   int severity,
						   int dberr,
						   int oserr,
						   char *dberrstr,
						   char *oserrstr);

typedef struct TdsLsConnCacheKey
{
	Oid		serverid;
	Oid		userid;
	Oid		umid;
} TdsLsConnCacheKey;

typedef enum TdsLsConnState
{
	TDS_LS_CONN_DISCONNECTED,
	TDS_LS_CONN_READY,
	TDS_LS_CONN_LEASED,
	TDS_LS_CONN_RESETTING
} TdsLsConnState;

typedef enum TdsLsConnDropReason
{
	TDS_LS_CONN_DROP_NONE,
	TDS_LS_CONN_DROP_CATALOG_INVALIDATED,
	TDS_LS_CONN_DROP_CONNECTION_FAILURE,
	TDS_LS_CONN_DROP_QUERY_TIMEOUT,
	TDS_LS_CONN_DROP_CLEANUP_FAILED
} TdsLsConnDropReason;

typedef struct TdsLsConnCacheEntry TdsLsConnCacheEntry;

typedef struct TdsLsConnLease
{
	TdsLsConnCacheEntry *entry;
	LOGINREC   *login;
	DBPROCESS  *dbproc;
} TdsLsConnLease;

void tdsLinkedServerConnectionCheckout(Oid serverid,
					   Oid userid,
					   TdsDblibErrHandler runtime_err_handler,
					   TdsLsConnLease *lease);
void tdsLinkedServerConnectionCheckin(TdsLsConnLease *lease,
					  bool connection_clean);
void tdsLinkedServerConnectionMarkDrop(TdsLsConnLease *lease,
					   TdsLsConnDropReason reason);
bool tdsLinkedServerConnectionLeaseActive(const TdsLsConnLease *lease);

#endif
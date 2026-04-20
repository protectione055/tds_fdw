/*------------------------------------------------------------------
*
*                               Foreign data wrapper for TDS
*
* Connection lifecycle helpers and backend-local linked-server cache.
*
*----------------------------------------------------------------------------
*/

#include "postgres.h"

#include "catalog/pg_foreign_server.h"
#include "catalog/pg_user_mapping.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/syscache.h"

#include "connection.h"
#include "tds_fdw.h"

#define TDS_LS_RESET_SQLSERVER_SESSION "EXEC sys.sp_reset_connection"

struct TdsLsConnCacheEntry
{
	TdsLsConnCacheKey key;
	LOGINREC   *login;
	DBPROCESS  *dbproc;
	TdsLsConnState state;
	TdsLsConnDropReason drop_reason;
	uint32		server_hashvalue;
	uint32		mapping_hashvalue;
	bool		is_sql_server;
	bool		sqlserver_ansi_mode;
	TdsDblibErrHandler runtime_err_handler;
};

static HTAB *TdsLsConnectionHash = NULL;
static int	TdsDblibEnvRefCount = 0;

static void tdsOpenConnection(TdsFdwOptionSet *option_set,
				  LOGINREC **login,
				  DBPROCESS **dbproc,
				  TdsDblibErrHandler runtime_err_handler);
static void tdsCloseConnection(LOGINREC *login, DBPROCESS *dbproc);
static void tdsAcquireDblibEnv(void);
static void tdsReleaseDblibEnv(void);
static void tdsConfigureMessageHandler(const char *msg_handler);
static void tdsLinkedServerConnectionInitCache(void);
static void tdsLinkedServerInvalCallback(Datum arg, int cacheid, uint32 hashvalue);
static bool tdsLinkedServerConnectionMatchesInvalidation(TdsLsConnCacheEntry *entry,
								 int cacheid,
								 uint32 hashvalue);
static void tdsLinkedServerConnectionDisconnectEntry(TdsLsConnCacheEntry *entry);
static bool tdsLinkedServerConnectionResetSession(TdsLsConnCacheEntry *entry);
static bool tdsExecCleanupSql(TdsLsConnCacheEntry *entry, const char *sql);
static void tdsResetConnectionLocalState(void);

static void
tdsAcquireDblibEnv(void)
{
	if (TdsDblibEnvRefCount == 0)
	{
		tds_clear_signals();
		if (dbinit() == FAIL)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_OUT_OF_MEMORY),
					 errmsg("Failed to initialize DB-Library environment")));
	}

	TdsDblibEnvRefCount++;
}

static void
tdsReleaseDblibEnv(void)
{
	Assert(TdsDblibEnvRefCount > 0);
	TdsDblibEnvRefCount--;

	if (TdsDblibEnvRefCount == 0)
	{
		dbexit();
		tds_clear_signals();
	}
}

static void
tdsConfigureMessageHandler(const char *msg_handler)
{
	if (msg_handler == NULL)
		return;

	if (strcmp(msg_handler, "notice") == 0)
		dbmsghandle(tds_notice_msg_handler);
	else if (strcmp(msg_handler, "blackhole") == 0)
		dbmsghandle(tds_blackhole_msg_handler);
	else
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("Unknown msg handler: %s.", msg_handler)));
}

static void
tdsOpenConnection(TdsFdwOptionSet *option_set,
				  LOGINREC **login,
				  DBPROCESS **dbproc,
				  TdsDblibErrHandler runtime_err_handler)
{
	tds_clear_signals();
	tdsAcquireDblibEnv();

	PG_TRY();
	{
		tdsConfigureMessageHandler(option_set->msg_handler);

		*login = dblogin();
		if (*login == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_OUT_OF_MEMORY),
					 errmsg("Failed to initialize DB-Library login structure")));

		if (tdsSetupConnection(option_set, *login, dbproc) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
					 errmsg("failed to setup TDS connection")));

		dberrhandle(runtime_err_handler ? runtime_err_handler : tds_err_handler);
	}
	PG_CATCH();
	{
		if (*dbproc)
			dbclose(*dbproc);
		if (*login)
			dbloginfree(*login);
		*dbproc = NULL;
		*login = NULL;
		tdsReleaseDblibEnv();
		PG_RE_THROW();
	}
	PG_END_TRY();
}

static void
tdsCloseConnection(LOGINREC *login, DBPROCESS *dbproc)
{
	tdsResetConnectionLocalState();

	if (dbproc)
		dbclose(dbproc);
	if (login)
		dbloginfree(login);

	tdsReleaseDblibEnv();
}

static void
tdsLinkedServerConnectionInitCache(void)
{
	HASHCTL		ctl;

	if (TdsLsConnectionHash != NULL)
		return;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(TdsLsConnCacheKey);
	ctl.entrysize = sizeof(TdsLsConnCacheEntry);
	TdsLsConnectionHash = hash_create("tds_fdw linked-server connections",
					 8,
					 &ctl,
					 HASH_ELEM | HASH_BLOBS);

	CacheRegisterSyscacheCallback(FOREIGNSERVEROID,
					  tdsLinkedServerInvalCallback,
					  (Datum) 0);
	CacheRegisterSyscacheCallback(USERMAPPINGOID,
					  tdsLinkedServerInvalCallback,
					  (Datum) 0);
}

static void
tdsLinkedServerConnectionDisconnectEntry(TdsLsConnCacheEntry *entry)
{
	if (entry->dbproc != NULL || entry->login != NULL)
		tdsCloseConnection(entry->login, entry->dbproc);

	entry->login = NULL;
	entry->dbproc = NULL;
	entry->state = TDS_LS_CONN_DISCONNECTED;
	entry->drop_reason = TDS_LS_CONN_DROP_NONE;
	entry->runtime_err_handler = NULL;
	entry->is_sql_server = false;
	entry->sqlserver_ansi_mode = false;
}

static bool
tdsLinkedServerConnectionMatchesInvalidation(TdsLsConnCacheEntry *entry,
								 int cacheid,
								 uint32 hashvalue)
{
	if (hashvalue == 0)
		return true;

	if (cacheid == FOREIGNSERVEROID)
		return entry->server_hashvalue == hashvalue;
	if (cacheid == USERMAPPINGOID)
		return entry->mapping_hashvalue == hashvalue;

	return false;
}

static void
tdsLinkedServerInvalCallback(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS scan;
	TdsLsConnCacheEntry *entry;

	if (TdsLsConnectionHash == NULL)
		return;

	hash_seq_init(&scan, TdsLsConnectionHash);
	while ((entry = (TdsLsConnCacheEntry *) hash_seq_search(&scan)) != NULL)
	{
		if (!tdsLinkedServerConnectionMatchesInvalidation(entry, cacheid, hashvalue))
			continue;

		if (entry->state == TDS_LS_CONN_READY)
			tdsLinkedServerConnectionDisconnectEntry(entry);
		else if (entry->state == TDS_LS_CONN_LEASED ||
				 entry->state == TDS_LS_CONN_RESETTING)
		{
			if (entry->drop_reason == TDS_LS_CONN_DROP_NONE)
				entry->drop_reason = TDS_LS_CONN_DROP_CATALOG_INVALIDATED;
		}
	}
}

void
tdsLinkedServerConnectionCheckout(Oid serverid,
					  Oid userid,
					  TdsDblibErrHandler runtime_err_handler,
					  TdsLsConnLease *lease)
{
	TdsFdwOptionSet option_set;
	UserMapping *mapping;
	TdsLsConnCacheEntry *entry;
	TdsLsConnCacheKey key;
	bool		found;

	Assert(lease != NULL);
	MemSet(lease, 0, sizeof(TdsLsConnLease));

	tdsLinkedServerConnectionInitCache();
	tdsGetForeignServerOptionsFromCatalogByUser(serverid, userid,
							 &option_set,
							 &mapping);

	key.serverid = serverid;
	key.userid = userid;
	key.umid = mapping->umid;

	entry = hash_search(TdsLsConnectionHash, &key, HASH_ENTER, &found);
	if (!found)
	{
		entry->login = NULL;
		entry->dbproc = NULL;
		entry->state = TDS_LS_CONN_DISCONNECTED;
		entry->drop_reason = TDS_LS_CONN_DROP_NONE;
		entry->server_hashvalue = 0;
		entry->mapping_hashvalue = 0;
		entry->is_sql_server = false;
		entry->sqlserver_ansi_mode = false;
		entry->runtime_err_handler = NULL;
	}

	if (entry->state == TDS_LS_CONN_READY &&
		entry->drop_reason != TDS_LS_CONN_DROP_NONE)
		tdsLinkedServerConnectionDisconnectEntry(entry);

	if (entry->state == TDS_LS_CONN_LEASED ||
		entry->state == TDS_LS_CONN_RESETTING)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("linked-server connection is already in use")));

	if (entry->state == TDS_LS_CONN_DISCONNECTED)
	{
		tdsOpenConnection(&option_set, &entry->login, &entry->dbproc,
					  runtime_err_handler);
		entry->server_hashvalue =
			GetSysCacheHashValue1(FOREIGNSERVEROID,
						  ObjectIdGetDatum(serverid));
		entry->mapping_hashvalue =
			GetSysCacheHashValue1(USERMAPPINGOID,
						  ObjectIdGetDatum(mapping->umid));
		entry->is_sql_server = tdsIsSqlServer(entry->dbproc);
		entry->sqlserver_ansi_mode = option_set.sqlserver_ansi_mode;
		entry->runtime_err_handler = runtime_err_handler;
		entry->state = TDS_LS_CONN_READY;
		entry->drop_reason = TDS_LS_CONN_DROP_NONE;
	}
	else
	{
		tds_clear_signals();
		tdsConfigureMessageHandler(option_set.msg_handler);
		dberrhandle(runtime_err_handler ? runtime_err_handler : tds_err_handler);
	}

	entry->state = TDS_LS_CONN_LEASED;
	lease->entry = entry;
	lease->login = entry->login;
	lease->dbproc = entry->dbproc;
}

static void
tdsResetConnectionLocalState(void)
{
	dbsettime(0);
	tds_remote_proc_query_timed_out = false;
	tds_clear_signals();
}

static bool
tdsExecCleanupSql(TdsLsConnCacheEntry *entry, const char *sql)
{
	RETCODE		erc;
	int			ret_code;

	dberrhandle(tds_err_capture);

	if (dbcmd(entry->dbproc, (char *) sql) == FAIL)
		goto fail;
	if (dbsqlexec(entry->dbproc) == FAIL)
		goto fail;

	while ((erc = dbresults(entry->dbproc)) != NO_MORE_RESULTS)
	{
		if (erc == FAIL)
			goto fail;
		if (erc != SUCCEED)
			continue;

		while ((ret_code = dbnextrow(entry->dbproc)) != NO_MORE_ROWS)
		{
			if (ret_code == FAIL || ret_code == BUF_FULL)
				goto fail;
		}
	}

	dberrhandle(entry->runtime_err_handler ? entry->runtime_err_handler : tds_err_handler);
	return true;

fail:
	dberrhandle(entry->runtime_err_handler ? entry->runtime_err_handler : tds_err_handler);
	return false;
}

static bool
tdsLinkedServerConnectionResetSession(TdsLsConnCacheEntry *entry)
{
	if (!entry->is_sql_server)
		return false;

	if (!tdsExecCleanupSql(entry, TDS_LS_RESET_SQLSERVER_SESSION))
		return false;

	if (entry->sqlserver_ansi_mode)
	{
		if (!tdsExecCleanupSql(entry,
					  "SET CONCAT_NULL_YIELDS_NULL, ANSI_NULLS, ANSI_WARNINGS, "
					  "QUOTED_IDENTIFIER, ANSI_PADDING, ANSI_NULL_DFLT_ON ON"))
			return false;
	}

	return true;
}

void
tdsLinkedServerConnectionCheckin(TdsLsConnLease *lease, bool connection_clean)
{
	TdsLsConnCacheEntry *entry;

	if (lease == NULL || lease->entry == NULL)
		return;

	entry = lease->entry;
	entry->state = TDS_LS_CONN_RESETTING;
	tdsResetConnectionLocalState();

	if (!connection_clean && entry->drop_reason == TDS_LS_CONN_DROP_NONE)
		entry->drop_reason = TDS_LS_CONN_DROP_CLEANUP_FAILED;

	if (entry->drop_reason == TDS_LS_CONN_DROP_NONE)
	{
		if (!tdsLinkedServerConnectionResetSession(entry))
			entry->drop_reason = TDS_LS_CONN_DROP_CLEANUP_FAILED;
	}

	if (entry->drop_reason != TDS_LS_CONN_DROP_NONE)
		tdsLinkedServerConnectionDisconnectEntry(entry);
	else
		entry->state = TDS_LS_CONN_READY;

	lease->entry = NULL;
	lease->login = NULL;
	lease->dbproc = NULL;
}

void
tdsLinkedServerConnectionMarkDrop(TdsLsConnLease *lease,
					  TdsLsConnDropReason reason)
{
	if (lease == NULL || lease->entry == NULL)
		return;

	if (lease->entry->drop_reason == TDS_LS_CONN_DROP_NONE)
		lease->entry->drop_reason = reason;
}

bool
tdsLinkedServerConnectionLeaseActive(const TdsLsConnLease *lease)
{
	return lease != NULL && lease->entry != NULL &&
		lease->dbproc != NULL;
}
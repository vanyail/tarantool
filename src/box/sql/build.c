/*
 * Copyright 2010-2017, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/*
 * This file contains C code routines that are called by the SQLite parser
 * when syntax rules are reduced.  The routines in this file handle the
 * following kinds of SQL syntax:
 *
 *     CREATE TABLE
 *     DROP TABLE
 *     CREATE INDEX
 *     DROP INDEX
 *     creating ID lists
 *     BEGIN TRANSACTION
 *     COMMIT
 *     ROLLBACK
 */
#include "sqliteInt.h"
#include "vdbeInt.h"
#include "tarantoolInt.h"
#include "box/session.h"
#include "box/identifier.h"
#include "box/schema.h"
#include "box/tuple_format.h"

/*
 * This routine is called after a single SQL statement has been
 * parsed and a VDBE program to execute that statement has been
 * prepared.  This routine puts the finishing touches on the
 * VDBE program and resets the pParse structure for the next
 * parse.
 *
 * Note that if an error occurred, it might be the case that
 * no VDBE code was generated.
 */
void
sqlite3FinishCoding(Parse * pParse)
{
	sqlite3 *db;
	Vdbe *v;

	assert(pParse->pToplevel == 0);
	db = pParse->db;
	if (pParse->nested)
		return;
	if (db->mallocFailed || pParse->nErr) {
		if (pParse->rc == SQLITE_OK)
			pParse->rc = SQLITE_ERROR;
		return;
	}

	/* Begin by generating some termination code at the end of the
	 * vdbe program
	 */
	v = sqlite3GetVdbe(pParse);
	assert(!pParse->isMultiWrite
	       || sqlite3VdbeAssertMayAbort(v, pParse->mayAbort));
	if (v) {
		sqlite3VdbeAddOp0(v, OP_Halt);

#if SQLITE_USER_AUTHENTICATION
		if (pParse->nTableLock > 0 && db->init.busy == 0) {
			sqlite3UserAuthInit(db);
			if (db->auth.authLevel < UAUTH_User) {
				sqlite3ErrorMsg(pParse,
						"user not authenticated");
				pParse->rc = SQLITE_AUTH_USER;
				return;
			}
		}
#endif

		/* The cookie mask contains one bit for each database file open.
		 * (Bit 0 is for main, bit 1 is for temp, and so forth.)  Bits are
		 * set for each database that is used.  Generate code to start a
		 * transaction on each used database and to verify the schema cookie
		 * on each used database.
		 */
		if (db->mallocFailed == 0
		    && (DbMaskNonZero(pParse->cookieMask) || pParse->pConstExpr)
		    ) {
			int i;
			assert(sqlite3VdbeGetOp(v, 0)->opcode == OP_Init);
			sqlite3VdbeJumpHere(v, 0);
			Schema *pSchema;
			if (DbMaskTest(pParse->cookieMask, 0) != 0) {
				pSchema = db->mdb.pSchema;
				sqlite3VdbeAddOp4Int(v, OP_Transaction,	/* Opcode */
						     0,	/* P1 */
						     DbMaskTest(pParse->writeMask, 0),	/* P2 */
						     pSchema->schema_cookie,	/* P3 */
						     pSchema->iGeneration	/* P4 */
				    );
				if (pParse->initiateTTrans)
					sqlite3VdbeAddOp0(v, OP_TTransaction);

				if (db->init.busy == 0)
					sqlite3VdbeChangeP5(v, 1);

				VdbeComment((v, "usesStmtJournal=%d",
					     pParse->mayAbort
					     && pParse->isMultiWrite));
			}

			/* Code constant expressions that where factored out of inner loops */
			if (pParse->pConstExpr) {
				ExprList *pEL = pParse->pConstExpr;
				pParse->okConstFactor = 0;
				for (i = 0; i < pEL->nExpr; i++) {
					sqlite3ExprCode(pParse, pEL->a[i].pExpr,
							pEL->a[i].u.
							iConstExprReg);
				}
			}

			/* Finally, jump back to the beginning of the executable code. */
			sqlite3VdbeGoto(v, 1);
		}
	}

	/* Get the VDBE program ready for execution
	 */
	if (v && pParse->nErr == 0 && !db->mallocFailed) {
		assert(pParse->iCacheLevel == 0);	/* Disables and re-enables match */
		/* A minimum of one cursor is required if autoincrement is used
		 *  See ticket [a696379c1f08866]
		 */
		if (pParse->pAinc != 0 && pParse->nTab == 0)
			pParse->nTab = 1;
		sqlite3VdbeMakeReady(v, pParse);
		pParse->rc = SQLITE_DONE;
	} else {
		pParse->rc = SQLITE_ERROR;
	}
}

/*
 * Run the parser and code generator recursively in order to generate
 * code for the SQL statement given onto the end of the pParse context
 * currently under construction.  When the parser is run recursively
 * this way, the final OP_Halt is not appended and other initialization
 * and finalization steps are omitted because those are handling by the
 * outermost parser.
 *
 * Not everything is nestable.  This facility is designed to perform
 * basic DDL operations.  Use care if you decide to try to use this routine
 * for some other purposes.
 */
void
sqlite3NestedParse(Parse * pParse, const char *zFormat, ...)
{
	va_list ap;
	char *zSql;
	char *zErrMsg = 0;
	sqlite3 *db = pParse->db;
	char saveBuf[PARSE_TAIL_SZ];

	if (pParse->nErr)
		return;
	assert(pParse->nested < 10);	/* Nesting should only be of limited depth */
	va_start(ap, zFormat);
	zSql = sqlite3VMPrintf(db, zFormat, ap);
	va_end(ap);
	if (zSql == 0) {
		return;		/* A malloc must have failed */
	}
	pParse->nested++;
	memcpy(saveBuf, PARSE_TAIL(pParse), PARSE_TAIL_SZ);
	memset(PARSE_TAIL(pParse), 0, PARSE_TAIL_SZ);
	sqlite3RunParser(pParse, zSql, &zErrMsg);
	sqlite3DbFree(db, zErrMsg);
	sqlite3DbFree(db, zSql);
	memcpy(PARSE_TAIL(pParse), saveBuf, PARSE_TAIL_SZ);
	pParse->nested--;
}

#if SQLITE_USER_AUTHENTICATION
/*
 * Return TRUE if zTable is the name of the system table that stores the
 * list of users and their access credentials.
 */
int
sqlite3UserAuthTable(const char *zTable)
{
	return sqlite3_stricmp(zTable, "sqlite_user") == 0;
}
#endif

/*
 * Locate the in-memory structure that describes a particular database
 * table given the name of that table and (optionally) the name of the
 * database containing the table.  Return NULL if not found.
 *
 * If zDatabase is 0, all databases are searched for the table and the
 * first matching table is returned.  (No checking for duplicate table
 * names is done.)  The search order is TEMP first, then MAIN, then any
 * auxiliary databases added using the ATTACH command.
 *
 * See also sqlite3LocateTable().
 */
Table *
sqlite3FindTable(sqlite3 * db, const char *zName)
{
#if SQLITE_USER_AUTHENTICATION
	/* Only the admin user is allowed to know that the sqlite_user table
	 * exists
	 */
	if (db->auth.authLevel < UAUTH_Admin
	    && sqlite3UserAuthTable(zName) != 0) {
		return 0;
	}
#endif

	return sqlite3HashFind(&db->mdb.pSchema->tblHash, zName);
}

/*
 * Locate the in-memory structure that describes a particular database
 * table given the name of that table. Return NULL if not found.
 * Also leave an error message in pParse->zErrMsg.
 *
 * The difference between this routine and sqlite3FindTable() is that this
 * routine leaves an error message in pParse->zErrMsg where
 * sqlite3FindTable() does not.
 */
Table *
sqlite3LocateTable(Parse * pParse,	/* context in which to report errors */
		   u32 flags,	/* LOCATE_VIEW or LOCATE_NOERR */
		   const char *zName	/* Name of the table we are looking for */
    )
{
	Table *p;

	/* Read the database schema. If an error occurs, leave an error message
	 * and code in pParse and return NULL.
	 */
	if (SQLITE_OK != sqlite3ReadSchema(pParse)) {
		return 0;
	}

	p = sqlite3FindTable(pParse->db, zName);
	if (p == 0) {
		const char *zMsg =
		    flags & LOCATE_VIEW ? "no such view" : "no such table";
		if ((flags & LOCATE_NOERR) == 0) {
			sqlite3ErrorMsg(pParse, "%s: %s", zMsg, zName);
			pParse->checkSchema = 1;
		}
	}

	return p;
}

/*
 * Locate the table identified by *p.
 *
 * This is a wrapper around sqlite3LocateTable(). The difference between
 * sqlite3LocateTable() and this function is that this function restricts
 * the search to schema (p->pSchema) if it is not NULL. p->pSchema may be
 * non-NULL if it is part of a view or trigger program definition. See
 * sqlite3FixSrcList() for details.
 */
Table *
sqlite3LocateTableItem(Parse * pParse, u32 flags, struct SrcList_item * p)
{
	return sqlite3LocateTable(pParse, flags, p->zName);
}

/*
 * Locate the in-memory structure that describes
 * a particular index given the name of that index
 * and the name of the database that contains the index.
 * Return NULL if not found.
 */
Index *
sqlite3FindIndex(MAYBE_UNUSED sqlite3 * db, const char *zName, Table * pTab)
{
	assert(pTab);

	return sqlite3HashFind(&pTab->idxHash, zName);
}

Index *
sqlite3LocateIndex(sqlite3 * db, const char *zName, const char *zTable)
{
	assert(zName);
	assert(zTable);

	Table *pTab = sqlite3FindTable(db, zTable);

	if (pTab == 0) {
		return 0;
	}

	return sqlite3FindIndex(db, zName, pTab);
}

/*
 * Reclaim the memory used by an index
 */
static void
freeIndex(sqlite3 * db, Index * p)
{
#ifndef SQLITE_OMIT_ANALYZE
	sqlite3DeleteIndexSamples(db, p);
#endif
	sqlite3ExprDelete(db, p->pPartIdxWhere);
	sqlite3ExprListDelete(db, p->aColExpr);
	sqlite3DbFree(db, p->zColAff);
	if (p->isResized)
		sqlite3DbFree(db, (void *)p->azColl);
	sqlite3_free(p->aiRowEst);
	sqlite3DbFree(db, p);
}

/*
 * For the index called zIdxName which is found in the database,
 * unlike that index from its Table then remove the index from
 * the index hash table and free all memory structures associated
 * with the index.
 */
void
sqlite3UnlinkAndDeleteIndex(sqlite3 * db, Index * pIndex)
{
	assert(pIndex != 0);
	assert(&pIndex->pTable->idxHash);

	struct session *user_session = current_session();

	pIndex = sqlite3HashInsert(&pIndex->pTable->idxHash, pIndex->zName, 0);
	if (ALWAYS(pIndex)) {
		if (pIndex->pTable->pIndex == pIndex) {
			pIndex->pTable->pIndex = pIndex->pNext;
		} else {
			Index *p;
			/* Justification of ALWAYS();  The index must be on the list of
			 * indices.
			 */
			p = pIndex->pTable->pIndex;
			while (ALWAYS(p) && p->pNext != pIndex) {
				p = p->pNext;
			}
			if (ALWAYS(p && p->pNext == pIndex)) {
				p->pNext = pIndex->pNext;
			}
		}
		freeIndex(db, pIndex);
	}

	user_session->sql_flags |= SQLITE_InternChanges;
}

/*
 * Reset the schema for the database.
 */
void
sqlite3ResetOneSchema(sqlite3 * db)
{
	Db *pDb;

	/* Case 1:  Reset the single schema of the database  */
	pDb = &db->mdb;
	assert(pDb->pSchema != 0);
	sqlite3SchemaClear(pDb->pSchema);
}

/*
 * Erase all schema information from all attached databases (including
 * "main" and "temp") for a single database connection.
 */
void
sqlite3ResetAllSchemasOfConnection(sqlite3 * db)
{
	struct session *user_session = current_session();
	Db *pDb = &db->mdb;
	if (pDb->pSchema) {
		sqlite3SchemaClear(pDb->pSchema);
	}
	user_session->sql_flags &= ~SQLITE_InternChanges;
}

/*
 * This routine is called when a commit occurs.
 */
void
sqlite3CommitInternalChanges()
{
	current_session()->sql_flags &= ~SQLITE_InternChanges;
}

/*
 * Delete memory allocated for the column names of a table or view (the
 * Table.aCol[] array).
 */
void
sqlite3DeleteColumnNames(sqlite3 * db, Table * pTable)
{
	int i;
	Column *pCol;
	assert(pTable != 0);
	if ((pCol = pTable->aCol) != 0) {
		for (i = 0; i < pTable->nCol; i++, pCol++) {
			sqlite3DbFree(db, pCol->zName);
			sqlite3ExprDelete(db, pCol->pDflt);
			sqlite3DbFree(db, pCol->zColl);
		}
		sqlite3DbFree(db, pTable->aCol);
	}
}

/*
 * Remove the memory data structures associated with the given
 * Table.  No changes are made to disk by this routine.
 *
 * This routine just deletes the data structure.  It does not unlink
 * the table data structure from the hash table.  But it does destroy
 * memory structures of the indices and foreign keys associated with
 * the table.
 *
 * The db parameter is optional.  It is needed if the Table object
 * contains lookaside memory.  (Table objects in the schema do not use
 * lookaside memory, but some ephemeral Table objects do.)  Or the
 * db parameter can be used with db->pnBytesFreed to measure the memory
 * used by the Table object.
 */
static void SQLITE_NOINLINE
deleteTable(sqlite3 * db, Table * pTable)
{
	Index *pIndex, *pNext;
	TESTONLY(int nLookaside;
	    )

	    /* Used to verify lookaside not used for schema */
	    /* Record the number of outstanding lookaside allocations in schema Tables
	     * prior to doing any free() operations.  Since schema Tables do not use
	     * lookaside, this number should not change.
	     */
	    TESTONLY(nLookaside =
		     (db
		      && (pTable->tabFlags & TF_Ephemeral) ==
		      0) ? db->lookaside.nOut : 0);

	/* Delete all indices associated with this table. */
	for (pIndex = pTable->pIndex; pIndex; pIndex = pNext) {
		pNext = pIndex->pNext;
		assert(pIndex->pSchema == pTable->pSchema);
		if ((db == 0 || db->pnBytesFreed == 0)) {
			char *zName = pIndex->zName;
			TESTONLY(Index *
				 pOld =) sqlite3HashInsert(&pTable->idxHash,
							   zName, 0);
			assert(pOld == pIndex || pOld == 0);
		}
		freeIndex(db, pIndex);
	}

	/* Delete any foreign keys attached to this table. */
	sqlite3FkDelete(db, pTable);

	/* Delete the Table structure itself.
	 */
	sqlite3HashClear(&pTable->idxHash);
	sqlite3DeleteColumnNames(db, pTable);
	sqlite3DbFree(db, pTable->zName);
	sqlite3DbFree(db, pTable->zColAff);
	sqlite3SelectDelete(db, pTable->pSelect);
	sqlite3ExprListDelete(db, pTable->pCheck);
	sqlite3DbFree(db, pTable);

	/* Verify that no lookaside memory was used by schema tables */
	assert(nLookaside == 0 || nLookaside == db->lookaside.nOut);
}

void
sqlite3DeleteTable(sqlite3 * db, Table * pTable)
{
	/* Do not delete the table until the reference count reaches zero. */
	if (!pTable)
		return;
	if (((!db || db->pnBytesFreed == 0) && (--pTable->nTabRef) > 0))
		return;
	deleteTable(db, pTable);
}

/*
 * Unlink the given table from the hash tables and the delete the
 * table structure with all its indices and foreign keys.
 */
void
sqlite3UnlinkAndDeleteTable(sqlite3 * db, const char *zTabName)
{
	Table *p;
	Db *pDb;

	assert(db != 0);
	assert(zTabName);
	testcase(zTabName[0] == 0);	/* Zero-length table names are allowed */
	pDb = &db->mdb;
	p = sqlite3HashInsert(&pDb->pSchema->tblHash, zTabName, 0);
	sqlite3DeleteTable(db, p);
}

/*
 * Given a token, return a string that consists of the text of that
 * token.  Space to hold the returned string
 * is obtained from sqliteMalloc() and must be freed by the calling
 * function.
 *
 * Any quotation marks (ex:  "name", 'name', [name], or `name`) that
 * surround the body of the token are removed.
 *
 * Tokens are often just pointers into the original SQL text and so
 * are not \000 terminated and are not persistent.  The returned string
 * is \000 terminated and is persistent.
 */
char *
sqlite3NameFromToken(sqlite3 * db, Token * pName)
{
	char *zName;
	if (pName) {
		zName = sqlite3DbStrNDup(db, (char *)pName->z, pName->n);
		sqlite3NormalizeName(zName);
	} else {
		zName = 0;
	}
	return zName;
}

/*
 * Parameter zName points to a nul-terminated buffer containing the name
 * of a database ("main", "temp" or the name of an attached db). This
 * function returns the index of the named database in db->aDb[], or
 * -1 if the named db cannot be found.
 */
int
sqlite3FindDbName(const char *zName MAYBE_UNUSED)
{
	assert(0 == sqlite3_stricmp("main", zName));
	return 0;
}

/*
 * The token *pName contains the name of a database (either "main" or
 * "temp" or the name of an attached db). This routine returns the
 * index of the named database in db->aDb[], or -1 if the named db
 * does not exist.
 */
int
sqlite3FindDb(sqlite3 * db, Token * pName)
{
	int i;			/* Database number */
	char *zName;		/* Name we are searching for */
	zName = sqlite3NameFromToken(db, pName);
	i = sqlite3FindDbName(zName);
	sqlite3DbFree(db, zName);
	return i;
}

/*
 * This routine is used to check if the UTF-8 string zName is a legal
 * unqualified name for an identifier.
 * Some objects may not be checked, because they are validated in Tarantool.
 * (e.g. table, index, column name of a real table)
 * All names are legal except those that cantain non-printable
 * characters or have length greater than BOX_NAME_MAX.
 */
int
sqlite3CheckIdentifierName(Parse *pParse, char *zName)
{
	ssize_t len = strlen(zName);

	if (len > BOX_NAME_MAX || identifier_check(zName, len) != 0) {
		sqlite3ErrorMsg(pParse,
				"identifier name is invalid: %s",
				zName);
		return SQLITE_ERROR;
	}
	return SQLITE_OK;
}

/*
 * Return the PRIMARY KEY index of a table
 */
Index *
sqlite3PrimaryKeyIndex(Table * pTab)
{
	Index *p;
	for (p = pTab->pIndex; p && !IsPrimaryKeyIndex(p); p = p->pNext) {
	}
	return p;
}

/*
 * Return the column of index pIdx that corresponds to table
 * column iCol.  Return -1 if not found.
 */
i16
sqlite3ColumnOfIndex(Index * pIdx, i16 iCol)
{
	/* TARANTOOL: Data layout is the same in every index.  */
	(void) pIdx;
	return iCol;
}

/*
 * Begin constructing a new table representation in memory.  This is
 * the first of several action routines that get called in response
 * to a CREATE TABLE statement.  In particular, this routine is called
 * after seeing tokens "CREATE" and "TABLE" and the table name. The isTemp
 * flag is true if the table should be stored in the auxiliary database
 * file instead of in the main database file.  This is normally the case
 * when the "TEMP" or "TEMPORARY" keyword occurs in between
 * CREATE and TABLE.
 *
 * The new table record is initialized and put in pParse->pNewTable.
 * As more of the CREATE TABLE statement is parsed, additional action
 * routines will be called to add more information to this record.
 * At the end of the CREATE TABLE statement, the sqlite3EndTable() routine
 * is called to complete the construction of the new table record.
 *
 * @param pParse Parser context.
 * @param pName1 First part of the name of the table or view.
 * @param noErr Do nothing if table already exists.
 */
void
sqlite3StartTable(Parse *pParse, Token *pName, int noErr)
{
	Table *pTable;
	char *zName = 0;	/* The name of the new table */
	sqlite3 *db = pParse->db;
	Vdbe *v;

	/* Do not account nested operations: the count of such
	 * operations depends on Tarantool data dictionary internals,
	 * such as data layout in system spaces.
	 */
	if (!pParse->nested) {
		if ((v = sqlite3GetVdbe(pParse)) == NULL)
			goto begin_table_error;
		sqlite3VdbeCountChanges(v);
	}

	zName = sqlite3NameFromToken(db, pName);

	pParse->sNameToken = *pName;
	if (zName == 0)
		return;
#ifndef SQLITE_OMIT_AUTHORIZATION
	assert(isTemp == 0 || isTemp == 1);
	assert(isView == 0 || (isView == 1 && isTemp == 0));
	{
		static const u8 aCode[] = {
			SQLITE_CREATE_TABLE,
			SQLITE_CREATE_TEMP_TABLE,
			SQLITE_CREATE_VIEW
		};
		char *zDb = db->mdb.zDbSName;
		if (sqlite3AuthCheck
		    (pParse, SQLITE_INSERT, MASTER_NAME, 0, zDb)) {
			goto begin_table_error;
		}
		if (sqlite3AuthCheck(pParse, (int)aCode[isTemp + 2 * isView],
				     zName, 0, zDb)) {
			goto begin_table_error;
		}
	}
#endif

	/*
	 * Make sure the new table name does not collide with an
	 * existing index or table name in the same database.
	 * Issue an error message if it does.
	 */
	if (SQLITE_OK != sqlite3ReadSchema(pParse))
		goto begin_table_error;
	pTable = sqlite3FindTable(db, zName);
	if (pTable) {
		if (!noErr) {
			sqlite3ErrorMsg(pParse,
					"table %s already exists",
					zName);
		} else {
			assert(!db->init.busy || CORRUPT_DB);
			sqlite3CodeVerifySchema(pParse);
		}
		goto begin_table_error;
	}

	pTable = sqlite3DbMallocZero(db, sizeof(Table));
	if (pTable == 0) {
		assert(db->mallocFailed);
		pParse->rc = SQLITE_NOMEM_BKPT;
		pParse->nErr++;
		goto begin_table_error;
	}
	pTable->zName = zName;
	pTable->iPKey = -1;
	pTable->iAutoIncPKey = -1;
	pTable->pSchema = db->mdb.pSchema;
	sqlite3HashInit(&pTable->idxHash);
	pTable->nTabRef = 1;
	pTable->nRowLogEst = 200;
	assert(200 == sqlite3LogEst(1048576));
	assert(pParse->pNewTable == 0);
	pParse->pNewTable = pTable;

	/* If this is the magic _sequence table used by autoincrement,
	 * then record a pointer to this table in the main database structure
	 * so that INSERT can find the table easily.
	 */
#ifndef SQLITE_OMIT_AUTOINCREMENT
	if (!pParse->nested && strcmp(zName, "_SEQUENCE") == 0) {
		pTable->pSchema->pSeqTab = pTable;
	}
#endif

	/* Begin generating the code that will create a new table.
	 * Note in particular that we must go ahead and allocate the
	 * record number for the table entry now.  Before any
	 * PRIMARY KEY or UNIQUE keywords are parsed.  Those keywords will cause
	 * indices to be created and the table record must come before the
	 * indices.  Hence, the record number for the table must be allocated
	 * now.
	 */
	if (!db->init.busy && (v = sqlite3GetVdbe(pParse)) != 0)
		sqlite3BeginWriteOperation(pParse, 1);

	/* Normal (non-error) return. */
	return;

	/* If an error occurs, we jump here */
 begin_table_error:
	sqlite3DbFree(db, zName);
	return;
}

/* Set properties of a table column based on the (magical)
 * name of the column.
 */
#if SQLITE_ENABLE_HIDDEN_COLUMNS
void
sqlite3ColumnPropertiesFromName(Table * pTab, Column * pCol)
{
	if (sqlite3_strnicmp(pCol->zName, "__hidden__", 10) == 0) {
		pCol->colFlags |= COLFLAG_HIDDEN;
	} else if (pTab && pCol != pTab->aCol
		   && (pCol[-1].colFlags & COLFLAG_HIDDEN)) {
		pTab->tabFlags |= TF_OOOHidden;
	}
}
#endif

/*
 * Add a new column to the table currently being constructed.
 *
 * The parser calls this routine once for each column declaration
 * in a CREATE TABLE statement.  sqlite3StartTable() gets called
 * first to get things going.  Then this routine is called for each
 * column.
 */
void
sqlite3AddColumn(Parse * pParse, Token * pName, Token * pType)
{
	Table *p;
	int i;
	char *z;
	char *zType;
	Column *pCol;
	sqlite3 *db = pParse->db;
	if ((p = pParse->pNewTable) == 0)
		return;
#if SQLITE_MAX_COLUMN
	if (p->nCol + 1 > db->aLimit[SQLITE_LIMIT_COLUMN]) {
		sqlite3ErrorMsg(pParse, "too many columns on %s", p->zName);
		return;
	}
#endif
	z = sqlite3DbMallocRaw(db, pName->n + pType->n + 2);
	if (z == 0)
		return;
	memcpy(z, pName->z, pName->n);
	z[pName->n] = 0;
	sqlite3NormalizeName(z);
	for (i = 0; i < p->nCol; i++) {
		if (strcmp(z, p->aCol[i].zName) == 0) {
			sqlite3ErrorMsg(pParse, "duplicate column name: %s", z);
			sqlite3DbFree(db, z);
			return;
		}
	}
	if ((p->nCol & 0x7) == 0) {
		Column *aNew;
		aNew =
		    sqlite3DbRealloc(db, p->aCol,
				     (p->nCol + 8) * sizeof(p->aCol[0]));
		if (aNew == 0) {
			sqlite3DbFree(db, z);
			return;
		}
		p->aCol = aNew;
	}
	pCol = &p->aCol[p->nCol];
	memset(pCol, 0, sizeof(p->aCol[0]));
	pCol->zName = z;
	sqlite3ColumnPropertiesFromName(p, pCol);

	if (pType->n == 0) {
		/* If there is no type specified, columns have the default affinity
		 * 'BLOB'.
		 */
		pCol->affinity = SQLITE_AFF_BLOB;
		pCol->szEst = 1;
	} else {
		zType = z + sqlite3Strlen30(z) + 1;
		memcpy(zType, pType->z, pType->n);
		zType[pType->n] = 0;
		sqlite3Dequote(zType);
		pCol->affinity = sqlite3AffinityType(zType, &pCol->szEst);
		pCol->colFlags |= COLFLAG_HASTYPE;
	}
	p->nCol++;
	pParse->constraintName.n = 0;
}

/*
 * This routine is called by the parser while in the middle of
 * parsing a CREATE TABLE statement.  A "NOT NULL" constraint has
 * been seen on a column.  This routine sets the notNull flag on
 * the column currently under construction.
 */
void
sqlite3AddNotNull(Parse * pParse, int onError)
{
	Table *p;
	p = pParse->pNewTable;
	if (p == 0 || NEVER(p->nCol < 1))
		return;
	p->aCol[p->nCol - 1].notNull = (u8) onError;
}

/*
 * Scan the column type name zType (length nType) and return the
 * associated affinity type.
 *
 * This routine does a case-independent search of zType for the
 * substrings in the following table. If one of the substrings is
 * found, the corresponding affinity is returned. If zType contains
 * more than one of the substrings, entries toward the top of
 * the table take priority. For example, if zType is 'BLOBINT',
 * SQLITE_AFF_INTEGER is returned.
 *
 * Substring     | Affinity
 * --------------------------------
 * 'INT'         | SQLITE_AFF_INTEGER
 * 'CHAR'        | SQLITE_AFF_TEXT
 * 'CLOB'        | SQLITE_AFF_TEXT
 * 'TEXT'        | SQLITE_AFF_TEXT
 * 'BLOB'        | SQLITE_AFF_BLOB
 * 'REAL'        | SQLITE_AFF_REAL
 * 'FLOA'        | SQLITE_AFF_REAL
 * 'DOUB'        | SQLITE_AFF_REAL
 *
 * If none of the substrings in the above table are found,
 * SQLITE_AFF_NUMERIC is returned.
 */
char
sqlite3AffinityType(const char *zIn, u8 * pszEst)
{
	u32 h = 0;
	char aff = SQLITE_AFF_NUMERIC;
	const char *zChar = 0;

	assert(zIn != 0);
	while (zIn[0]) {
		h = (h << 8) + sqlite3UpperToLower[(*zIn) & 0xff];
		zIn++;
		if (h == (('c' << 24) + ('h' << 16) + ('a' << 8) + 'r')) {	/* CHAR */
			aff = SQLITE_AFF_TEXT;
			zChar = zIn;
		} else if (h == (('c' << 24) + ('l' << 16) + ('o' << 8) + 'b')) {	/* CLOB */
			aff = SQLITE_AFF_TEXT;
		} else if (h == (('t' << 24) + ('e' << 16) + ('x' << 8) + 't')) {	/* TEXT */
			aff = SQLITE_AFF_TEXT;
		} else if (h == (('b' << 24) + ('l' << 16) + ('o' << 8) + 'b')	/* BLOB */
			   &&(aff == SQLITE_AFF_NUMERIC
			      || aff == SQLITE_AFF_REAL)) {
			aff = SQLITE_AFF_BLOB;
			if (zIn[0] == '(')
				zChar = zIn;
#ifndef SQLITE_OMIT_FLOATING_POINT
		} else if (h == (('r' << 24) + ('e' << 16) + ('a' << 8) + 'l')	/* REAL */
			   &&aff == SQLITE_AFF_NUMERIC) {
			aff = SQLITE_AFF_REAL;
		} else if (h == (('f' << 24) + ('l' << 16) + ('o' << 8) + 'a')	/* FLOA */
			   &&aff == SQLITE_AFF_NUMERIC) {
			aff = SQLITE_AFF_REAL;
		} else if (h == (('d' << 24) + ('o' << 16) + ('u' << 8) + 'b')	/* DOUB */
			   &&aff == SQLITE_AFF_NUMERIC) {
			aff = SQLITE_AFF_REAL;
#endif
		} else if ((h & 0x00FFFFFF) == (('i' << 16) + ('n' << 8) + 't')) {	/* INT */
			aff = SQLITE_AFF_INTEGER;
			break;
		}
	}

	/* If pszEst is not NULL, store an estimate of the field size.  The
	 * estimate is scaled so that the size of an integer is 1.
	 */
	if (pszEst) {
		*pszEst = 1;	/* default size is approx 4 bytes
		*/
		if (aff < SQLITE_AFF_NUMERIC) {
			if (zChar) {
				while (zChar[0]) {
					if (sqlite3Isdigit(zChar[0])) {
						int v = 0;
						sqlite3GetInt32(zChar, &v);
						v = v / 4 + 1;
						if (v > 255)
							v = 255;
						*pszEst = v;	/* BLOB(k), VARCHAR(k), CHAR(k) -> r=(k/4+1)
						*/
						break;
					}
					zChar++;
				}
			} else {
				*pszEst = 5;	/* BLOB, TEXT, CLOB -> r=5  (approx 20 bytes)
				*/
			}
		}
	}
	return aff;
}

/*
 * The expression is the default value for the most recently added column
 * of the table currently under construction.
 *
 * Default value expressions must be constant.  Raise an exception if this
 * is not the case.
 *
 * This routine is called by the parser while in the middle of
 * parsing a CREATE TABLE statement.
 */
void
sqlite3AddDefaultValue(Parse * pParse, ExprSpan * pSpan)
{
	Table *p;
	Column *pCol;
	sqlite3 *db = pParse->db;
	p = pParse->pNewTable;
	if (p != 0) {
		pCol = &(p->aCol[p->nCol - 1]);
		if (!sqlite3ExprIsConstantOrFunction
		    (pSpan->pExpr, db->init.busy)) {
			sqlite3ErrorMsg(pParse,
					"default value of column [%s] is not constant",
					pCol->zName);
		} else {
			/* A copy of pExpr is used instead of the original, as pExpr contains
			 * tokens that point to volatile memory. The 'span' of the expression
			 * is required by pragma table_info.
			 */
			Expr x;
			sqlite3ExprDelete(db, pCol->pDflt);
			memset(&x, 0, sizeof(x));
			x.op = TK_SPAN;
			x.u.zToken = sqlite3DbStrNDup(db, (char *)pSpan->zStart,
						      (int)(pSpan->zEnd -
							    pSpan->zStart));
			x.pLeft = pSpan->pExpr;
			x.flags = EP_Skip;
			pCol->pDflt = sqlite3ExprDup(db, &x, EXPRDUP_REDUCE);
			sqlite3DbFree(db, x.u.zToken);
		}
	}
	sqlite3ExprDelete(db, pSpan->pExpr);
}


/*
 * Designate the PRIMARY KEY for the table.  pList is a list of names
 * of columns that form the primary key.  If pList is NULL, then the
 * most recently added column of the table is the primary key.
 *
 * A table can have at most one primary key.  If the table already has
 * a primary key (and this is the second primary key) then create an
 * error.
 *
 * Set the Table.iPKey field of the table under construction to be the
 * index of the INTEGER PRIMARY KEY column.
 * Table.iPKey is set to -1 if there is no INTEGER PRIMARY KEY.
 *
 * If the key is not an INTEGER PRIMARY KEY, then create a unique
 * index for the key.  No index is created for INTEGER PRIMARY KEYs.
 */
void
sqlite3AddPrimaryKey(Parse * pParse,	/* Parsing context */
		     ExprList * pList,	/* List of field names to be indexed */
		     int onError,	/* What to do with a uniqueness conflict */
		     int autoInc,	/* True if the AUTOINCREMENT keyword is present */
		     int sortOrder	/* SQLITE_SO_ASC or SQLITE_SO_DESC */
    )
{
	Table *pTab = pParse->pNewTable;
	Column *pCol = 0;
	int iCol = -1, i;
	int nTerm;
	if (pTab == 0)
		goto primary_key_exit;
	if (pTab->tabFlags & TF_HasPrimaryKey) {
		sqlite3ErrorMsg(pParse,
				"table \"%s\" has more than one primary key",
				pTab->zName);
		goto primary_key_exit;
	}
	pTab->tabFlags |= TF_HasPrimaryKey;
	if (pList == 0) {
		iCol = pTab->nCol - 1;
		pCol = &pTab->aCol[iCol];
		pCol->colFlags |= COLFLAG_PRIMKEY;
		nTerm = 1;
	} else {
		nTerm = pList->nExpr;
		for (i = 0; i < nTerm; i++) {
			Expr *pCExpr =
			    sqlite3ExprSkipCollate(pList->a[i].pExpr);
			assert(pCExpr != 0);
			if (pCExpr->op != TK_ID) {
				sqlite3ErrorMsg(pParse, "expressions prohibited in PRIMARY KEY");
				goto primary_key_exit;
			}
			const char *zCName = pCExpr->u.zToken;
			for (iCol = 0; iCol < pTab->nCol; iCol++) {
				if (strcmp
				    (zCName,
				     pTab->aCol[iCol].zName) == 0) {
					pCol = &pTab->aCol[iCol];
					pCol->colFlags |=
					    COLFLAG_PRIMKEY;
					break;
				}
			}
		}
	}
	if (nTerm == 1
	    && pCol
	    && (sqlite3StrICmp(sqlite3ColumnType(pCol, ""), "INTEGER") == 0
		|| sqlite3StrICmp(sqlite3ColumnType(pCol, ""), "INT") == 0)
	    && sortOrder != SQLITE_SO_DESC) {
		assert(autoInc == 0 || autoInc == 1);
		pTab->iPKey = iCol;
		pTab->keyConf = (u8) onError;
		if (autoInc) {
			pTab->iAutoIncPKey = iCol;
			pTab->tabFlags |= TF_Autoincrement;
		}
		if (pList)
			pParse->iPkSortOrder = pList->a[0].sortOrder;
	} else if (autoInc) {
#ifndef SQLITE_OMIT_AUTOINCREMENT
		sqlite3ErrorMsg(pParse, "AUTOINCREMENT is only allowed on an "
				"INTEGER PRIMARY KEY or INT PRIMARY KEY");
#endif
	} else {
		sqlite3CreateIndex(pParse, 0, 0, pList, onError, 0,
				   0, sortOrder, 0, SQLITE_IDXTYPE_PRIMARYKEY);
		pList = 0;
	}

 primary_key_exit:
	sqlite3ExprListDelete(pParse->db, pList);
	return;
}

/*
 * Add a new CHECK constraint to the table currently under construction.
 */
void
sqlite3AddCheckConstraint(Parse * pParse,	/* Parsing context */
			  Expr * pCheckExpr	/* The check expression */
    )
{
#ifndef SQLITE_OMIT_CHECK
	Table *pTab = pParse->pNewTable;
	if (pTab) {
		pTab->pCheck =
		    sqlite3ExprListAppend(pParse, pTab->pCheck, pCheckExpr);
		if (pParse->constraintName.n) {
			sqlite3ExprListSetName(pParse, pTab->pCheck,
					       &pParse->constraintName, 1);
		}
	} else
#endif
	{
		sqlite3ExprDelete(pParse->db, pCheckExpr);
	}
}

/*
 * Set the collation function of the most recently parsed table column
 * to the CollSeq given.
 */
void
sqlite3AddCollateType(Parse * pParse, Token * pToken)
{
	Table *p;
	int i;
	char *zColl;		/* Dequoted name of collation sequence */
	sqlite3 *db;

	if ((p = pParse->pNewTable) == 0)
		return;
	i = p->nCol - 1;
	db = pParse->db;
	zColl = sqlite3NameFromToken(db, pToken);
	if (!zColl)
		return;

	if (sqlite3LocateCollSeq(pParse, db, zColl)) {
		Index *pIdx;
		sqlite3DbFree(db, p->aCol[i].zColl);
		p->aCol[i].zColl = zColl;

		/* If the column is declared as "<name> PRIMARY KEY COLLATE <type>",
		 * then an index may have been created on this column before the
		 * collation type was added. Correct this if it is the case.
		 */
		for (pIdx = p->pIndex; pIdx; pIdx = pIdx->pNext) {
			assert(pIdx->nKeyCol == 1);
			if (pIdx->aiColumn[0] == i) {
				pIdx->azColl[0] = column_collation_name(p, i);
			}
		}
	} else {
		sqlite3DbFree(db, zColl);
	}
}

/**
 * Return name of given column collation from table.
 *
 * @param table Table which is used to fetch column.
 * @param column Number of column.
 * @retval Pointer to collation's name.
 */
char *
column_collation_name(Table *table, uint32_t column)
{
	assert(table != NULL);
	uint32_t space_id = SQLITE_PAGENO_TO_SPACEID(table->tnum);
	struct space *space = space_by_id(space_id);
	/*
	 * It is not always possible to fetch collation directly
	 * from struct space. To be more precise when:
	 * 1. space is ephemeral. Thus, its id is zero and
	 *    it can't be found in space cache.
	 * 2. space is a view. Hence, it lacks any functional
	 *    parts such as indexes or fields.
	 * 3. space is under construction. So, the same as p.1
	 *    it can't be found in space cache.
	 * In cases mentioned above collation is fetched from
	 * SQL specific structures.
	 */
	if (space == NULL || space_index(space, 0) == NULL)
		return table->aCol[column].zColl;

	/* "BINARY" is a name for default collation in SQL. */
	char *coll_name = (char *)sqlite3StrBINARY;
	if (space->format->fields[column].coll != NULL) {
		coll_name = space->format->fields[column].coll->name;
	}
	return coll_name;
}

/**
 * Return name of given column collation from index.
 *
 * @param idx Index which is used to fetch column.
 * @param column Number of column.
 * @retval Pointer to collation's name.
 */
char *
index_collation_name(Index *idx, uint32_t column)
{
	assert(idx != NULL);
	uint32_t space_id = SQLITE_PAGENO_TO_SPACEID(idx->pTable->tnum);
	struct space *space = space_by_id(space_id);
	/*
	 * If space is still under construction, or it is
	 * an ephemeral space, then fetch collation from
	 * SQL internal structure.
	 */
	if (space == NULL)
		return (char *)idx->azColl[column];

	uint32_t index_id = SQLITE_PAGENO_TO_INDEXID(idx->tnum);
	struct index *index = space_index(space, index_id);
	assert(index != NULL && index->def->key_def->part_count >= column);
	struct coll *coll = index->def->key_def->parts[column].coll;
	/* "BINARY" is a name for default collation in SQL. */
	if (coll == NULL)
		return (char *)sqlite3StrBINARY;
	return index->def->key_def->parts[column].coll->name;
}

/*
 * This function returns the collation sequence for database native text
 * encoding identified by the string zName, length nName.
 *
 * If the requested collation sequence is not available, or not available
 * in the database native encoding, the collation factory is invoked to
 * request it. If the collation factory does not supply such a sequence,
 * and the sequence is available in another text encoding, then that is
 * returned instead.
 *
 * If no versions of the requested collations sequence are available, or
 * another error occurs, NULL is returned and an error message written into
 * pParse.
 *
 * This routine is a wrapper around sqlite3FindCollSeq().  This routine
 * invokes the collation factory if the named collation cannot be found
 * and generates an error message.
 *
 * See also: sqlite3FindCollSeq(), sqlite3GetCollSeq()
 */
struct coll *
sqlite3LocateCollSeq(Parse * pParse, sqlite3 * db, const char *zName)
{
	u8 initbusy;
	struct coll *pColl;
	initbusy = db->init.busy;

	pColl = sqlite3FindCollSeq(zName);
	if (!initbusy && (!pColl)) {
		pColl = sqlite3GetCollSeq(pParse, db, pColl, zName);
	}

	return pColl;
}

/*
 * Generate code that will increment the schema cookie.
 *
 * The schema cookie is used to determine when the schema for the
 * database changes.  After each schema change, the cookie value
 * changes.  When a process first reads the schema it records the
 * cookie.  Thereafter, whenever it goes to access the database,
 * it checks the cookie to make sure the schema has not changed
 * since it was last read.
 *
 * This plan is not completely bullet-proof.  It is possible for
 * the schema to change multiple times and for the cookie to be
 * set back to prior value.  But schema changes are infrequent
 * and the probability of hitting the same cookie value is only
 * 1 chance in 2^32.  So we're safe enough.
 *
 * IMPLEMENTATION-OF: R-34230-56049 SQLite automatically increments
 * the schema-version whenever the schema changes.
 */
void
sqlite3ChangeCookie(Parse * pParse)
{
	sqlite3 *db = pParse->db;
	Vdbe *v = pParse->pVdbe;
	sqlite3VdbeAddOp3(v, OP_SetCookie, 0, 0,
			  db->mdb.pSchema->schema_cookie + 1);
}

/*
 * Measure the number of characters needed to output the given
 * identifier.  The number returned includes any quotes used
 * but does not include the null terminator.
 *
 * The estimate is conservative.  It might be larger that what is
 * really needed.
 */
static int
identLength(const char *z)
{
	int n;
	for (n = 0; *z; n++, z++) {
		if (*z == '"') {
			n++;
		}
	}
	return n + 2;
}

/*
 * The first parameter is a pointer to an output buffer. The second
 * parameter is a pointer to an integer that contains the offset at
 * which to write into the output buffer. This function copies the
 * nul-terminated string pointed to by the third parameter, zSignedIdent,
 * to the specified offset in the buffer and updates *pIdx to refer
 * to the first byte after the last byte written before returning.
 *
 * If the string zSignedIdent consists entirely of alpha-numeric
 * characters, does not begin with a digit and is not an SQL keyword,
 * then it is copied to the output buffer exactly as it is. Otherwise,
 * it is quoted using double-quotes.
 */
static void
identPut(char *z, int *pIdx, char *zSignedIdent)
{
	unsigned char *zIdent = (unsigned char *)zSignedIdent;
	int i, j, needQuote;
	i = *pIdx;

	for (j = 0; zIdent[j]; j++) {
		if (!sqlite3Isalnum(zIdent[j]) && zIdent[j] != '_')
			break;
	}
	needQuote = sqlite3Isdigit(zIdent[0])
	    || sqlite3KeywordCode(zIdent, j) != TK_ID
	    || zIdent[j] != 0 || j == 0;

	if (needQuote)
		z[i++] = '"';
	for (j = 0; zIdent[j]; j++) {
		z[i++] = zIdent[j];
		if (zIdent[j] == '"')
			z[i++] = '"';
	}
	if (needQuote)
		z[i++] = '"';
	z[i] = 0;
	*pIdx = i;
}

/*
 * Generate a CREATE TABLE statement appropriate for the given
 * table.  Memory to hold the text of the statement is obtained
 * from sqliteMalloc() and must be freed by the calling function.
 */
static char *
createTableStmt(sqlite3 * db, Table * p)
{
	int i, k, n;
	char *zStmt;
	char *zSep, *zSep2, *zEnd;
	Column *pCol;
	n = 0;
	for (pCol = p->aCol, i = 0; i < p->nCol; i++, pCol++) {
		n += identLength(pCol->zName) + 5;
	}
	n += identLength(p->zName);
	if (n < 50) {
		zSep = "";
		zSep2 = ",";
		zEnd = ")";
	} else {
		zSep = "\n  ";
		zSep2 = ",\n  ";
		zEnd = "\n)";
	}
	n += 35 + 6 * p->nCol;
	zStmt = sqlite3DbMallocRaw(0, n);
	if (zStmt == 0) {
		sqlite3OomFault(db);
		return 0;
	}
	sqlite3_snprintf(n, zStmt, "CREATE TABLE ");
	k = sqlite3Strlen30(zStmt);
	identPut(zStmt, &k, p->zName);
	zStmt[k++] = '(';
	for (pCol = p->aCol, i = 0; i < p->nCol; i++, pCol++) {
		static const char *const azType[] = {
			/* SQLITE_AFF_BLOB    */ "",
			/* SQLITE_AFF_TEXT    */ " TEXT",
			/* SQLITE_AFF_NUMERIC */ " NUM",
			/* SQLITE_AFF_INTEGER */ " INT",
			/* SQLITE_AFF_REAL    */ " REAL"
		};
		int len;
		const char *zType;

		sqlite3_snprintf(n - k, &zStmt[k], zSep);
		k += sqlite3Strlen30(&zStmt[k]);
		zSep = zSep2;
		identPut(zStmt, &k, pCol->zName);
		assert(pCol->affinity - SQLITE_AFF_BLOB >= 0);
		assert(pCol->affinity - SQLITE_AFF_BLOB < ArraySize(azType));
		testcase(pCol->affinity == SQLITE_AFF_BLOB);
		testcase(pCol->affinity == SQLITE_AFF_TEXT);
		testcase(pCol->affinity == SQLITE_AFF_NUMERIC);
		testcase(pCol->affinity == SQLITE_AFF_INTEGER);
		testcase(pCol->affinity == SQLITE_AFF_REAL);

		zType = azType[pCol->affinity - SQLITE_AFF_BLOB];
		len = sqlite3Strlen30(zType);
		assert(pCol->affinity == SQLITE_AFF_BLOB
		       || pCol->affinity == sqlite3AffinityType(zType, 0));
		memcpy(&zStmt[k], zType, len);
		k += len;
		assert(k <= n);
	}
	sqlite3_snprintf(n - k, &zStmt[k], "%s", zEnd);
	return zStmt;
}

/*
 * Estimate the total row width for a table.
 */
static void
estimateTableWidth(Table * pTab)
{
	unsigned wTable = 0;
	const Column *pTabCol;
	int i;
	for (i = pTab->nCol, pTabCol = pTab->aCol; i > 0; i--, pTabCol++) {
		wTable += pTabCol->szEst;
	}
	if (pTab->iPKey < 0)
		wTable++;
	pTab->szTabRow = sqlite3LogEst(wTable * 4);
}

/*
 * Estimate the average size of a row for an index.
 */
static void
estimateIndexWidth(Index * pIdx)
{
	unsigned wIndex = 0;
	int i;
	const Column *aCol = pIdx->pTable->aCol;
	for (i = 0; i < pIdx->nColumn; i++) {
		i16 x = pIdx->aiColumn[i];
		assert(x < pIdx->pTable->nCol);
		wIndex += x < 0 ? 1 : aCol[pIdx->aiColumn[i]].szEst;
	}
	pIdx->szIdxRow = sqlite3LogEst(wIndex * 4);
}

/* Return true if value x is found any of the first nCol entries of aiCol[]
 */
static int
hasColumn(const i16 * aiCol, int nCol, int x)
{
	while (nCol-- > 0)
		if (x == *(aiCol++))
			return 1;
	return 0;
}

/*
 * This routine runs at the end of parsing a CREATE TABLE statement.
 * The job of this routine is to convert both
 * internal schema data structures and the generated VDBE code.
 * Changes include:
 *
 *     (1)  Set all columns of the PRIMARY KEY schema object to be NOT NULL.
 *     (2)  Set the Index.tnum of the PRIMARY KEY Index object in the
 *          schema to the rootpage from the main table.
 *     (3)  Add all table columns to the PRIMARY KEY Index object
 *          so that the PRIMARY KEY is a covering index.  The surplus
 *          columns are part of KeyInfo.nXField and are not used for
 *          sorting or lookup or uniqueness checks.
 */
static void
convertToWithoutRowidTable(Parse * pParse, Table * pTab)
{
	Index *pPk;
	int i, j;
	sqlite3 *db = pParse->db;

	/* Mark every PRIMARY KEY column as NOT NULL (except for imposter tables)
	 */
	if (!db->init.imposterTable) {
		for (i = 0; i < pTab->nCol; i++) {
			if ((pTab->aCol[i].colFlags & COLFLAG_PRIMKEY) != 0) {
				pTab->aCol[i].notNull = ON_CONFLICT_ACTION_ABORT;
			}
		}
	}

	/* Locate the PRIMARY KEY index.  Or, if this table was originally
	 * an INTEGER PRIMARY KEY table, create a new PRIMARY KEY index.
	 */
	if (pTab->iPKey >= 0) {
		ExprList *pList;
		Token ipkToken;
		sqlite3TokenInit(&ipkToken, pTab->aCol[pTab->iPKey].zName);
		pList = sqlite3ExprListAppend(pParse, 0,
					      sqlite3ExprAlloc(db, TK_ID,
							       &ipkToken, 0));
		if (pList == 0)
			return;
		pList->a[0].sortOrder = pParse->iPkSortOrder;
		assert(pParse->pNewTable == pTab);
		sqlite3CreateIndex(pParse, 0, 0, pList, pTab->keyConf, 0, 0, 0,
				   0, SQLITE_IDXTYPE_PRIMARYKEY);
		if (db->mallocFailed)
			return;
		pPk = sqlite3PrimaryKeyIndex(pTab);
		pTab->iPKey = -1;
	} else {
		pPk = sqlite3PrimaryKeyIndex(pTab);

		/*
		 * Remove all redundant columns from the PRIMARY KEY.  For example, change
		 * "PRIMARY KEY(a,b,a,b,c,b,c,d)" into just "PRIMARY KEY(a,b,c,d)".  Later
		 * code assumes the PRIMARY KEY contains no repeated columns.
		 */
		for (i = j = 1; i < pPk->nKeyCol; i++) {
			if (hasColumn(pPk->aiColumn, j, pPk->aiColumn[i])) {
				pPk->nColumn--;
			} else {
				pPk->aiColumn[j++] = pPk->aiColumn[i];
			}
		}
		pPk->nKeyCol = j;
	}
	assert(pPk != 0);
	if (!db->init.imposterTable)
		pPk->uniqNotNull = 1;
}

/*
 * Generate code to determine the new space Id.
 * Assume _schema was open and accessible via iCursor.
 * Fetch the max space id seen so far from _schema and increment it.
 * Return register storing the result.
 */
static int
getNewSpaceId(Parse * pParse, int iCursor)
{
	Vdbe *v = sqlite3GetVdbe(pParse);
	int iRes = ++pParse->nMem;

	sqlite3VdbeAddOp1(v, OP_IncMaxid, iCursor);
	sqlite3VdbeAddOp3(v, OP_Column, iCursor, 1, iRes);
	return iRes;
}

/*
 * Generate VDBE code to create an Index. This is acomplished by adding
 * an entry to the _index table. ISpaceId either contains the literal
 * space id or designates a register storing the id.
 */
static void
createIndex(Parse * pParse,
	    Index * pIndex,
	    int iSpaceId,
	    int iIndexId, const char *zSql, Table * pSysIndex, int iCursor)
{
	Vdbe *v = sqlite3GetVdbe(pParse);
	int iFirstCol = ++pParse->nMem;
	int iRecord = (pParse->nMem += 6);	/* 6 total columns */
	char *zOpts, *zParts;
	int zOptsSz, zPartsSz;

	/* Format "opts" and "parts" for _index entry. */
	zOpts = sqlite3DbMallocRaw(pParse->db,
				   tarantoolSqlite3MakeIdxOpts(pIndex, zSql,
							       NULL) +
				   tarantoolSqlite3MakeIdxParts(pIndex,
								NULL) + 2);
	if (!zOpts)
		return;
	zOptsSz = tarantoolSqlite3MakeIdxOpts(pIndex, zSql, zOpts);
	zParts = zOpts + zOptsSz + 1;
	zPartsSz = tarantoolSqlite3MakeIdxParts(pIndex, zParts);
#if SQLITE_DEBUG
	/* NUL-termination is necessary for VDBE trace facility only */
	zOpts[zOptsSz] = 0;
	zParts[zPartsSz] = 0;
#endif

	if (pParse->pNewTable) {
		int reg;
		/*
		 * A new table is being created, hence iSpaceId is a register, but
		 * iIndexId is literal.
		 */
		sqlite3VdbeAddOp2(v, OP_SCopy, iSpaceId, iFirstCol);
		sqlite3VdbeAddOp2(v, OP_Integer, iIndexId, iFirstCol + 1);

		/* Generate code to save new pageno into a register.
		 * This is runtime implementation of SQLITE_PAGENO_FROM_SPACEID_AND_INDEXID:
		 *   pageno = (spaceid << 10) | indexid
		 */
		pParse->regRoot = ++pParse->nMem;
		reg = ++pParse->nMem;
		sqlite3VdbeAddOp2(v, OP_Integer, 1 << 10, reg);
		sqlite3VdbeAddOp3(v, OP_Multiply, reg, iSpaceId,
				  pParse->regRoot);
		sqlite3VdbeAddOp3(v, OP_AddImm, pParse->regRoot, iIndexId,
				  pParse->regRoot);
	} else {
		/*
		 * An existing table is being modified; iSpaceId is literal, but
		 * iIndexId is a register.
		 */
		sqlite3VdbeAddOp2(v, OP_Integer, iSpaceId, iFirstCol);
		sqlite3VdbeAddOp2(v, OP_SCopy, iIndexId, iFirstCol + 1);
	}
	sqlite3VdbeAddOp4(v,
			  OP_String8, 0, iFirstCol + 2, 0,
			  sqlite3DbStrDup(pParse->db, pIndex->zName),
			  P4_DYNAMIC);
	sqlite3VdbeAddOp4(v, OP_String8, 0, iFirstCol + 3, 0, "tree",
			  P4_STATIC);
	sqlite3VdbeAddOp4(v, OP_Blob, zOptsSz, iFirstCol + 4, MSGPACK_SUBTYPE,
			  zOpts, P4_DYNAMIC);
	/* zOpts and zParts are co-located, hence STATIC */
	sqlite3VdbeAddOp4(v, OP_Blob, zPartsSz, iFirstCol + 5, MSGPACK_SUBTYPE,
			  zParts, P4_STATIC);
	sqlite3VdbeAddOp3(v, OP_MakeRecord, iFirstCol, 6, iRecord);
	sqlite3VdbeAddOp4Int(v, OP_IdxInsert, iCursor, iRecord, iFirstCol, 6);
	/* Do not account nested operations: the count of such
	 * operations depends on Tarantool data dictionary internals,
	 * such as data layout in system spaces. Also do not account
	 * autoindexes - they had been accounted as a part of
	 * CREATE TABLE already.
	 */
	if (!pParse->nested && pIndex->idxType == SQLITE_IDXTYPE_APPDEF)
		sqlite3VdbeChangeP5(v, OPFLAG_NCHANGE);
	sqlite3TableAffinity(v, pSysIndex, 0);
}

/*
 * Generate code to initialize register range with arguments for
 * ParseSchema2. Consumes zSql. Returns the first register used.
 */
static int
makeIndexSchemaRecord(Parse * pParse,
		      Index * pIndex,
		      int iSpaceId, int iIndexId, const char *zSql)
{
	Vdbe *v = sqlite3GetVdbe(pParse);
	int iP4Type;
	int iFirstCol = pParse->nMem + 1;
	pParse->nMem += 4;

	sqlite3VdbeAddOp4(v,
			  OP_String8, 0, iFirstCol, 0,
			  sqlite3DbStrDup(pParse->db, pIndex->zName),
			  P4_DYNAMIC);

	if (pParse->pNewTable) {
		/*
		 * A new table is being created, hence iSpaceId is a register, but
		 * iIndexId is literal.
		 */
		sqlite3VdbeAddOp2(v, OP_SCopy, iSpaceId, iFirstCol + 1);
		sqlite3VdbeAddOp2(v, OP_Integer, iIndexId, iFirstCol + 2);
	} else {
		/*
		 * An existing table is being modified; iSpaceId is literal, but
		 * iIndexId is a register.
		 */
		sqlite3VdbeAddOp2(v, OP_Integer, iSpaceId, iFirstCol + 1);
		sqlite3VdbeAddOp2(v, OP_SCopy, iIndexId, iFirstCol + 2);
	}

	iP4Type = P4_DYNAMIC;
	if (zSql == 0) {
		zSql = "";
		iP4Type = P4_STATIC;
	}
	sqlite3VdbeAddOp4(v, OP_String8, 0, iFirstCol + 3, 0, zSql, iP4Type);
	return iFirstCol;
}

/*
 * Generate code to create a new space.
 * iSpaceId is a register storing the id of the space.
 * iCursor is a cursor to access _space.
 */
static void
createSpace(Parse * pParse,
	    int iSpaceId, char *zStmt, int iCursor, Table * pSysSpace)
{
	Table *p = pParse->pNewTable;
	Vdbe *v = sqlite3GetVdbe(pParse);
	int iFirstCol = ++pParse->nMem;
	int iRecord = (pParse->nMem += 7);
	char *zOpts, *zFormat;
	int zOptsSz, zFormatSz;

	zOpts = sqlite3DbMallocRaw(pParse->db,
				   tarantoolSqlite3MakeTableFormat(p, NULL) +
				   tarantoolSqlite3MakeTableOpts(p, zStmt,
								 NULL) + 2);
	if (!zOpts) {
		zOptsSz = 0;
		zFormat = NULL;
		zFormatSz = 0;
	} else {
		zOptsSz = tarantoolSqlite3MakeTableOpts(p, zStmt, zOpts);
		zFormat = zOpts + zOptsSz + 1;
		zFormatSz = tarantoolSqlite3MakeTableFormat(p, zFormat);
#if SQLITE_DEBUG
		/* NUL-termination is necessary for VDBE-tracing facility only */
		zOpts[zOptsSz] = 0;
		zFormat[zFormatSz] = 0;
#endif
	}

	sqlite3VdbeAddOp2(v, OP_SCopy, iSpaceId, iFirstCol /* spaceId */ );
	sqlite3VdbeAddOp2(v, OP_Integer, effective_user()->uid,
			  iFirstCol + 1 /* owner */ );
	sqlite3VdbeAddOp4(v, OP_String8, 0, iFirstCol + 2 /* name */ , 0,
			  sqlite3DbStrDup(pParse->db, p->zName), P4_DYNAMIC);
	sqlite3VdbeAddOp4(v, OP_String8, 0, iFirstCol + 3 /* engine */ , 0,
			  "memtx", P4_STATIC);
	sqlite3VdbeAddOp2(v, OP_Integer, p->nCol,
			  iFirstCol + 4 /* field_count */ );
	sqlite3VdbeAddOp4(v, OP_Blob, zOptsSz, iFirstCol + 5, MSGPACK_SUBTYPE,
			  zOpts, P4_DYNAMIC);
	/* zOpts and zFormat are co-located, hence STATIC */
	sqlite3VdbeAddOp4(v, OP_Blob, zFormatSz, iFirstCol + 6, MSGPACK_SUBTYPE,
			  zFormat, P4_STATIC);
	sqlite3VdbeAddOp3(v, OP_MakeRecord, iFirstCol, 7, iRecord);
	sqlite3VdbeAddOp4Int(v, OP_IdxInsert, iCursor, iRecord, iFirstCol, 7);
	/* Do not account nested operations: the count of such
	 * operations depends on Tarantool data dictionary internals,
	 * such as data layout in system spaces.
	 */
	if (!pParse->nested)
		sqlite3VdbeChangeP5(v, OPFLAG_NCHANGE);
	sqlite3TableAffinity(v, pSysSpace, 0);
}

/*
 * Generate code to create implicit indexes in the new table.
 * iSpaceId is a register storing the id of the space.
 * iCursor is a cursor to access _index.
 */
static void
createImplicitIndices(Parse * pParse,
		      int iSpaceId, int iCursor, Table * pSysIndex)
{
	Table *p = pParse->pNewTable;
	Index *pIdx, *pPrimaryIdx = sqlite3PrimaryKeyIndex(p);
	int i;

	if (pPrimaryIdx) {
		/* Tarantool quirk: primary index is created first */
		createIndex(pParse, pPrimaryIdx, iSpaceId, 0, NULL, pSysIndex,
			    iCursor);
	} else {
		/*
		 * This branch should not be taken.
		 * If it is, then the current CREATE TABLE statement fails to
		 * specify the PRIMARY KEY. The error is reported elsewhere.
		 */
	}

	/* (pIdx->i) mapping must be consistent with parseTableSchemaRecord */
	for (pIdx = p->pIndex, i = 0; pIdx; pIdx = pIdx->pNext) {
		if (pIdx == pPrimaryIdx)
			continue;
		createIndex(pParse, pIdx, iSpaceId, ++i, NULL, pSysIndex,
			    iCursor);
	}
}

/*
 * Generate code to emit and parse table schema record.
 * iSpaceId is a register storing the id of the space.
 * Consumes zStmt.
 */
static void
parseTableSchemaRecord(Parse * pParse, int iSpaceId, char *zStmt)
{
	Table *p = pParse->pNewTable;
	Vdbe *v = sqlite3GetVdbe(pParse);
	Index *pIdx, *pPrimaryIdx;
	int i, iTop = pParse->nMem + 1;
	pParse->nMem += 4;

	sqlite3VdbeAddOp4(v,
			  OP_String8, 0, iTop, 0,
			  sqlite3DbStrDup(pParse->db, p->zName), P4_DYNAMIC);
	sqlite3VdbeAddOp2(v, OP_SCopy, iSpaceId, iTop + 1);
	sqlite3VdbeAddOp2(v, OP_Integer, 0, iTop + 2);
	sqlite3VdbeAddOp4(v, OP_String8, 0, iTop + 3, 0, zStmt, P4_DYNAMIC);

	pPrimaryIdx = sqlite3PrimaryKeyIndex(p);
	/* (pIdx->i) mapping must be consistent with createImplicitIndices */
	for (pIdx = p->pIndex, i = 0; pIdx; pIdx = pIdx->pNext) {
		if (pIdx == pPrimaryIdx)
			continue;
		makeIndexSchemaRecord(pParse, pIdx, iSpaceId, ++i, NULL);
	}

	sqlite3ChangeCookie(pParse);
	sqlite3VdbeAddParseSchema2Op(v, iTop, pParse->nMem - iTop + 1);
}

int
emitNewSysSequenceRecord(Parse *pParse, int reg_seq_id, const char *seq_name)
{
	Vdbe *v = sqlite3GetVdbe(pParse);
	sqlite3 *db = pParse->db;
	int first_col = pParse->nMem + 1;
	pParse->nMem += 10; /* 9 fields + new record pointer  */

	const long long int min_usigned_long_long = 0;
	const long long int max_usigned_long_long = LLONG_MAX;
	const bool const_false = false;

	/* 1. New sequence id  */
	sqlite3VdbeAddOp2(v, OP_SCopy, reg_seq_id, first_col + 1);
	/* 2. user is  */
	sqlite3VdbeAddOp2(v, OP_Integer, effective_user()->uid, first_col + 2);
	/* 3. New sequence name  */
        sqlite3VdbeAddOp4(v, OP_String8, 0, first_col + 3, 0,
			  sqlite3DbStrDup(pParse->db, seq_name), P4_DYNAMIC);

	/* 4. Step  */
	sqlite3VdbeAddOp2(v, OP_Integer, 1, first_col + 4);

	/* 5. Minimum  */
	sqlite3VdbeAddOp4Dup8(v, OP_Int64, 0, first_col + 5, 0,
			      (unsigned char*)&min_usigned_long_long, P4_INT64);
	/* 6. Maximum  */
	sqlite3VdbeAddOp4Dup8(v, OP_Int64, 0, first_col + 6, 0,
			      (unsigned char*)&max_usigned_long_long, P4_INT64);
	/* 7. Start  */
	sqlite3VdbeAddOp2(v, OP_Integer, 1, first_col + 7);

	/* 8. Cache  */
	sqlite3VdbeAddOp2(v, OP_Integer, 0, first_col + 8);

	/* 9. Cycle  */
	sqlite3VdbeAddOp2(v, OP_Bool, 0, first_col + 9);
	sqlite3VdbeChangeP4(v, -1, (char*)&const_false, P4_BOOL);

	sqlite3VdbeAddOp3(v, OP_MakeRecord, first_col + 1, 9, first_col);

	if (db->mallocFailed)
		return -1;
	else
		return first_col;
}

int
emitNewSysSpaceSequenceRecord(Parse *pParse, int space_id, const char reg_seq_id)
{
	Vdbe *v = sqlite3GetVdbe(pParse);
	const bool const_true = true;
	int first_col = pParse->nMem + 1;
	pParse->nMem += 4; /* 3 fields + new record pointer  */

	/* 1. Space id  */
	sqlite3VdbeAddOp2(v, OP_SCopy, space_id, first_col + 1);
	
	/* 2. Sequence id  */
	sqlite3VdbeAddOp2(v, OP_IntCopy, reg_seq_id, first_col + 2);

	/* 3. True, which is 1 in SQL  */
	sqlite3VdbeAddOp2(v, OP_Bool, 0, first_col + 3);
	sqlite3VdbeChangeP4(v, -1, (char*)&const_true, P4_BOOL);

	sqlite3VdbeAddOp3(v, OP_MakeRecord, first_col + 1, 3, first_col);

	return first_col;
}

/*
 * This routine is called to report the final ")" that terminates
 * a CREATE TABLE statement.
 *
 * The table structure that other action routines have been building
 * is added to the internal hash tables, assuming no errors have
 * occurred.
 *
 * Insert is performed in two passes:
 *  1. When db->init.busy == 0. Byte code for creation of new Tarantool
 *     space and all necessary Tarantool indexes is emitted
 *  2. When db->init.busy == 1. This means that byte code for creation
 *     of new table is executing right now, and it's time to add new entry
 *     for the table into SQL memory represenation
 *
 * If the pSelect argument is not NULL, it means that this routine
 * was called to create a table generated from a
 * "CREATE TABLE ... AS SELECT ..." statement.  The column names of
 * the new table will match the result set of the SELECT.
 */
void
sqlite3EndTable(Parse * pParse,	/* Parse context */
		Token * pCons,	/* The ',' token after the last column defn. */
		Token * pEnd,	/* The ')' before options in the CREATE TABLE */
		u8 tabOpts,	/* Extra table options. Usually 0. */
		Select * pSelect	/* Select from a "CREATE ... AS SELECT" */
    )
{
	Table *p;		/* The new table */
	sqlite3 *db = pParse->db;	/* The database connection */
	Index *pIdx;		/* An implied index of the table */

	if (pEnd == 0 && pSelect == 0) {
		return;
	}
	assert(!db->mallocFailed);
	p = pParse->pNewTable;
	if (p == 0)
		return;

	assert(!db->init.busy || !pSelect);

	/* If db->init.busy == 1, then we're called from
	 * OP_ParseSchema2 and is about to update in-memory
	 * schema.
	 */
	if (db->init.busy)
		p->tnum = db->init.newTnum;

	if (p->pSelect) {
		tabOpts |= TF_View;
	} else {
		if ((p->tabFlags & TF_HasPrimaryKey) == 0) {
			sqlite3ErrorMsg(pParse,
					"PRIMARY KEY missing on table %s",
					p->zName);
			return;
		} else {
			convertToWithoutRowidTable(pParse, p);
		}
	}

#ifndef SQLITE_OMIT_CHECK
	/* Resolve names in all CHECK constraint expressions.
	 */
	if (p->pCheck) {
		sqlite3ResolveSelfReference(pParse, p, NC_IsCheck, 0,
					    p->pCheck);
	}
#endif				/* !defined(SQLITE_OMIT_CHECK) */

	/* Estimate the average row size for the table and for all implied indices */
	estimateTableWidth(p);
	for (pIdx = p->pIndex; pIdx; pIdx = pIdx->pNext) {
		estimateIndexWidth(pIdx);
	}

	/* If not initializing, then create new Tarantool space.
	 *
	 * If this is a TEMPORARY table, write the entry into the auxiliary
	 * file instead of into the main database file.
	 */
	if (!db->init.busy) {
		int n;
		Vdbe *v;
		char *zType;	/* "VIEW" or "TABLE" */
		char *zStmt;	/* Text of the CREATE TABLE or CREATE VIEW statement */
		Table *pSysSchema, *pSysSpace, *pSysIndex;
		int iCursor = pParse->nTab++;
		int iSpaceId;

		v = sqlite3GetVdbe(pParse);
		if (NEVER(v == 0))
			return;

		pSysSchema = sqlite3HashFind(&pParse->db->mdb.pSchema->tblHash,
					     TARANTOOL_SYS_SCHEMA_NAME);
		if (NEVER(!pSysSchema))
			return;

		pSysSpace = sqlite3HashFind(&pParse->db->mdb.pSchema->tblHash,
					    TARANTOOL_SYS_SPACE_NAME);
		if (NEVER(!pSysSpace))
			return;

		pSysIndex = sqlite3HashFind(&pParse->db->mdb.pSchema->tblHash,
					    TARANTOOL_SYS_INDEX_NAME);
		if (NEVER(!pSysIndex))
			return;

		/*
		 * Initialize zType for the new view or table.
		 */
		if (p->pSelect == 0) {
			/* A regular table */
			zType = "TABLE";
#ifndef SQLITE_OMIT_VIEW
		} else {
			/* A view */
			zType = "VIEW";
#endif
		}

		/* If this is a CREATE TABLE xx AS SELECT ..., execute the SELECT
		 * statement to populate the new table. The root-page number for the
		 * new table is in register pParse->regRoot.
		 *
		 * Once the SELECT has been coded by sqlite3Select(), it is in a
		 * suitable state to query for the column names and types to be used
		 * by the new table.
		 *
		 * A shared-cache write-lock is not required to write to the new table,
		 * as a schema-lock must have already been obtained to create it. Since
		 * a schema-lock excludes all other database users, the write-lock would
		 * be redundant.
		 */


		/* Compute the complete text of the CREATE statement */
		if (pSelect) {
			zStmt = createTableStmt(db, p);
		} else {
			Token *pEnd2 = tabOpts ? &pParse->sLastToken : pEnd;
			n = (int)(pEnd2->z - pParse->sNameToken.z);
			if (pEnd2->z[0] != ';')
				n += pEnd2->n;
			zStmt = sqlite3MPrintf(db,
					       "CREATE %s %.*s", zType, n,
					       pParse->sNameToken.z);
		}

		sqlite3OpenTable(pParse, iCursor, pSysSchema, OP_OpenRead);
		sqlite3VdbeChangeP5(v, OPFLAG_SEEKEQ);
		iSpaceId = getNewSpaceId(pParse, iCursor);
		sqlite3OpenTable(pParse, iCursor, pSysSpace, OP_OpenWrite);
		createSpace(pParse, iSpaceId, zStmt, iCursor, pSysSpace);
		sqlite3OpenTable(pParse, iCursor, pSysIndex, OP_OpenWrite);
		createImplicitIndices(pParse, iSpaceId, iCursor, pSysIndex);
		sqlite3VdbeAddOp1(v, OP_Close, iCursor);

#ifndef SQLITE_OMIT_AUTOINCREMENT
		/* Check to see if we need to create an _sequence table for
		 * keeping track of autoincrement keys.
		 */
		if ((p->tabFlags & TF_Autoincrement) != 0) {
			Table *sys_sequence, *sys_space_sequence;
			int reg_seq_id;
			int reg_seq_record, reg_space_seq_record;
			assert(iSpaceId);

			/* Do an insertion into _sequence  */
			sys_sequence = sqlite3HashFind(
				&pParse->db->mdb.pSchema->tblHash,
				TARANTOOL_SYS_SEQUENCE_NAME);
			if (NEVER(!sys_sequence))
				return;

			sqlite3OpenTable(pParse, iCursor, sys_sequence,
					 OP_OpenWrite);
			reg_seq_id = ++pParse->nMem;
			sqlite3VdbeAddOp3(v, OP_NextId, iCursor, 0, reg_seq_id);

			reg_seq_record = emitNewSysSequenceRecord(pParse,
								  reg_seq_id,
								  p->zName);
			if (reg_seq_record < 0)
				return;
			sqlite3VdbeAddOp4Int(v, OP_IdxInsert, iCursor,
					     reg_seq_record,
					     reg_seq_record + 1, 9);
			sqlite3VdbeAddOp1(v, OP_Close, iCursor);

			/* Do an insertion into _space_sequence  */
			sys_space_sequence = sqlite3HashFind(
				&pParse->db->mdb.pSchema->tblHash,
				TARANTOOL_SYS_SPACE_SEQUENCE_NAME);
			if (NEVER(!sys_space_sequence))
				return;

			sqlite3OpenTable(pParse, iCursor, sys_space_sequence,
					 OP_OpenWrite);
			
			reg_space_seq_record = emitNewSysSpaceSequenceRecord(
				pParse,
				iSpaceId,
				reg_seq_id);

			sqlite3VdbeAddOp4Int(v, OP_IdxInsert, iCursor,
					     reg_space_seq_record,
					     reg_space_seq_record + 1, 3);

			sqlite3VdbeAddOp1(v, OP_Close, iCursor);
		}
#endif

		/* Reparse everything to update our internal data structures */
		parseTableSchemaRecord(pParse, iSpaceId, zStmt);	/* consumes zStmt */
	}

	/* Add the table to the in-memory representation of the database.
	 */
	if (db->init.busy) {
		Table *pOld;
		Schema *pSchema = p->pSchema;
		pOld = sqlite3HashInsert(&pSchema->tblHash, p->zName, p);
		if (pOld) {
			assert(p == pOld);	/* Malloc must have failed inside HashInsert() */
			sqlite3OomFault(db);
			return;
		}
		pParse->pNewTable = 0;
		current_session()->sql_flags |= SQLITE_InternChanges;

#ifndef SQLITE_OMIT_ALTERTABLE
		if (!p->pSelect) {
			const char *zName = (const char *)pParse->sNameToken.z;
			int nName;
			assert(!pSelect && pCons && pEnd);
			if (pCons->z == 0) {
				pCons = pEnd;
			}
			nName = (int)((const char *)pCons->z - zName);
			p->addColOffset = 13 + sqlite3Utf8CharLen(zName, nName);
		}
#endif
	}
}

#ifndef SQLITE_OMIT_VIEW
/*
 * The parser calls this routine in order to create a new VIEW
 */
void
sqlite3CreateView(Parse * pParse,	/* The parsing context */
		  Token * pBegin,	/* The CREATE token that begins the statement */
		  Token * pName,	/* The token that holds the name of the view */
		  ExprList * pCNames,	/* Optional list of view column names */
		  Select * pSelect,	/* A SELECT statement that will become the new view */
		  int noErr	/* Suppress error messages if VIEW already exists */
    )
{
	Table *p;
	int n;
	const char *z;
	Token sEnd;
	DbFixer sFix;
	sqlite3 *db = pParse->db;

	if (pParse->nVar > 0) {
		sqlite3ErrorMsg(pParse, "parameters are not allowed in views");
		goto create_view_fail;
	}
	sqlite3StartTable(pParse, pName, noErr);
	p = pParse->pNewTable;
	if (p == 0 || pParse->nErr)
		goto create_view_fail;
	sqlite3SchemaToIndex(db, p->pSchema);
	sqlite3FixInit(&sFix, pParse, "view", pName);
	if (sqlite3FixSelect(&sFix, pSelect))
		goto create_view_fail;

	/* Make a copy of the entire SELECT statement that defines the view.
	 * This will force all the Expr.token.z values to be dynamically
	 * allocated rather than point to the input string - which means that
	 * they will persist after the current sqlite3_exec() call returns.
	 */
	p->pSelect = sqlite3SelectDup(db, pSelect, EXPRDUP_REDUCE);
	p->pCheck = sqlite3ExprListDup(db, pCNames, EXPRDUP_REDUCE);
	if (db->mallocFailed)
		goto create_view_fail;

	/* Locate the end of the CREATE VIEW statement.  Make sEnd point to
	 * the end.
	 */
	sEnd = pParse->sLastToken;
	assert(sEnd.z[0] != 0);
	if (sEnd.z[0] != ';') {
		sEnd.z += sEnd.n;
	}
	sEnd.n = 0;
	n = (int)(sEnd.z - pBegin->z);
	assert(n > 0);
	z = pBegin->z;
	while (sqlite3Isspace(z[n - 1])) {
		n--;
	}
	sEnd.z = &z[n - 1];
	sEnd.n = 1;

	/* Use sqlite3EndTable() to add the view to the Tarantool.  */
	sqlite3EndTable(pParse, 0, &sEnd, 0, 0);

 create_view_fail:
	sqlite3SelectDelete(db, pSelect);
	sqlite3ExprListDelete(db, pCNames);
	return;
}
#endif				/* SQLITE_OMIT_VIEW */

#if !defined(SQLITE_OMIT_VIEW)
/*
 * The Table structure pTable is really a VIEW.  Fill in the names of
 * the columns of the view in the pTable structure.  Return the number
 * of errors.  If an error is seen leave an error message in pParse->zErrMsg.
 */
int
sqlite3ViewGetColumnNames(Parse * pParse, Table * pTable)
{
	Table *pSelTab;		/* A fake table from which we get the result set */
	Select *pSel;		/* Copy of the SELECT that implements the view */
	int nErr = 0;		/* Number of errors encountered */
	int n;			/* Temporarily holds the number of cursors assigned */
	sqlite3 *db = pParse->db;	/* Database connection for malloc errors */
#ifndef SQLITE_OMIT_AUTHORIZATION
	sqlite3_xauth xAuth;	/* Saved xAuth pointer */
#endif

	assert(pTable);

#ifndef SQLITE_OMIT_VIEW
	/* A positive nCol means the columns names for this view are
	 * already known.
	 */
	if (pTable->nCol > 0)
		return 0;

	/* A negative nCol is a special marker meaning that we are currently
	 * trying to compute the column names.  If we enter this routine with
	 * a negative nCol, it means two or more views form a loop, like this:
	 *
	 *     CREATE VIEW one AS SELECT * FROM two;
	 *     CREATE VIEW two AS SELECT * FROM one;
	 *
	 * Actually, the error above is now caught prior to reaching this point.
	 * But the following test is still important as it does come up
	 * in the following:
	 *
	 *     CREATE TABLE main.ex1(a);
	 *     CREATE TEMP VIEW ex1 AS SELECT a FROM ex1;
	 *     SELECT * FROM temp.ex1;
	 */
	if (pTable->nCol < 0) {
		sqlite3ErrorMsg(pParse, "view %s is circularly defined",
				pTable->zName);
		return 1;
	}
	assert(pTable->nCol >= 0);

	/* If we get this far, it means we need to compute the table names.
	 * Note that the call to sqlite3ResultSetOfSelect() will expand any
	 * "*" elements in the results set of the view and will assign cursors
	 * to the elements of the FROM clause.  But we do not want these changes
	 * to be permanent.  So the computation is done on a copy of the SELECT
	 * statement that defines the view.
	 */
	assert(pTable->pSelect);
	pSel = sqlite3SelectDup(db, pTable->pSelect, 0);
	if (pSel) {
		n = pParse->nTab;
		sqlite3SrcListAssignCursors(pParse, pSel->pSrc);
		pTable->nCol = -1;
		db->lookaside.bDisable++;
#ifndef SQLITE_OMIT_AUTHORIZATION
		xAuth = db->xAuth;
		db->xAuth = 0;
		pSelTab = sqlite3ResultSetOfSelect(pParse, pSel);
		db->xAuth = xAuth;
#else
		pSelTab = sqlite3ResultSetOfSelect(pParse, pSel);
#endif
		pParse->nTab = n;
		if (pTable->pCheck) {
			/* CREATE VIEW name(arglist) AS ...
			 * The names of the columns in the table are taken from
			 * arglist which is stored in pTable->pCheck.  The pCheck field
			 * normally holds CHECK constraints on an ordinary table, but for
			 * a VIEW it holds the list of column names.
			 */
			sqlite3ColumnsFromExprList(pParse, pTable->pCheck,
						   &pTable->nCol,
						   &pTable->aCol);
			if (db->mallocFailed == 0 && pParse->nErr == 0
			    && pTable->nCol == pSel->pEList->nExpr) {
				sqlite3SelectAddColumnTypeAndCollation(pParse,
								       pTable,
								       pSel);
			}
		} else if (pSelTab) {
			/* CREATE VIEW name AS...  without an argument list.  Construct
			 * the column names from the SELECT statement that defines the view.
			 */
			assert(pTable->aCol == 0);
			pTable->nCol = pSelTab->nCol;
			pTable->aCol = pSelTab->aCol;
			pSelTab->nCol = 0;
			pSelTab->aCol = 0;
		} else {
			pTable->nCol = 0;
			nErr++;
		}
		sqlite3DeleteTable(db, pSelTab);
		sqlite3SelectDelete(db, pSel);
		db->lookaside.bDisable--;
	} else {
		nErr++;
	}
	pTable->pSchema->schemaFlags |= DB_UnresetViews;
#endif				/* SQLITE_OMIT_VIEW */
	return nErr;
}
#endif				/* !defined(SQLITE_OMIT_VIEW) */

#ifndef SQLITE_OMIT_VIEW
/*
 * Clear the column names from every VIEW in database idx.
 */
static void
sqliteViewResetAll(sqlite3 * db)
{
	HashElem *i;
	if (!DbHasProperty(db, DB_UnresetViews))
		return;
	for (i = sqliteHashFirst(&db->mdb.pSchema->tblHash); i;
	     i = sqliteHashNext(i)) {
		Table *pTab = sqliteHashData(i);
		if (pTab->pSelect) {
			sqlite3DeleteColumnNames(db, pTab);
			pTab->aCol = 0;
			pTab->nCol = 0;
		}
	}
	DbClearProperty(db, DB_UnresetViews);
}
#else
#define sqliteViewResetAll(A,B)
#endif				/* SQLITE_OMIT_VIEW */

/*
 * Remove entries from the sqlite_statN tables (for N in (1,2,3))
 * after a DROP INDEX or DROP TABLE command.
 */
static void
sqlite3ClearStatTables(Parse * pParse,	/* The parsing context */
		       const char *zType,	/* "idx" or "tbl" */
		       const char *zName	/* Name of index or table */
    )
{
	int i;
	for (i = 1; i <= 4; i++) {
		char zTab[24];
		sqlite3_snprintf(sizeof(zTab), zTab, "_sql_stat%d", i);
		if (sqlite3FindTable(pParse->db, zTab)) {
			sqlite3NestedParse(pParse,
					   "DELETE FROM \"%s\" WHERE \"%s\"=%Q",
					   zTab, zType, zName);
		}
	}
}

/*
 * Generate code to drop a table.
 */
void
sqlite3CodeDropTable(Parse * pParse, Table * pTab, int isView)
{
	Vdbe *v;
	sqlite3 *db = pParse->db;
	Trigger *pTrigger;

	v = sqlite3GetVdbe(pParse);
	assert(v != 0);
	sqlite3BeginWriteOperation(pParse, 1);

	/* Drop all triggers associated with the table being dropped. Code
	 * is generated to remove entries from _trigger space.
	 */
	pTrigger = pTab->pTrigger;
	/* Do not account triggers deletion - they will be accounted
	 * in DELETE from _space below.
	 */
	pParse->nested++;

	while (pTrigger) {
		assert(pTrigger->pSchema == pTab->pSchema);
		sqlite3DropTriggerPtr(pParse, pTrigger);
		pTrigger = pTrigger->pNext;
	}
	pParse->nested--;

	/* Remove any entries of the _sequence table associated with
	 * the table being dropped. This is done before the table is dropped
	 * at the btree level, in case the _sequence table needs to
	 * move as a result of the drop.
	 */
	if (pTab->tabFlags & TF_Autoincrement) {
		sqlite3NestedParse(pParse,
				   "DELETE FROM \"%s\" WHERE \"space_id\"=%d",
				   TARANTOOL_SYS_SPACE_SEQUENCE_NAME,
				   SQLITE_PAGENO_TO_SPACEID(pTab->tnum));
		sqlite3NestedParse(pParse,
				   "DELETE FROM \"%s\" WHERE \"name\"=%Q",
				   TARANTOOL_SYS_SEQUENCE_NAME,
				   pTab->zName);
	}

	/* Drop all _space and _index entries that refer to the
	 * table. The program loops through the _index & _space tables and deletes
	 * every row that refers to a table.
	 * Triggers are handled separately because a trigger can be
	 * created in the temp database that refers to a table in another
	 * database.
	 */
	int space_id = SQLITE_PAGENO_TO_SPACEID(pTab->tnum);
	if (!isView) {
		if (pTab->pIndex && pTab->pIndex->pNext) {
			/*  Remove all indexes, except for primary.
			   Tarantool won't allow remove primary when secondary exist. */
			sqlite3NestedParse(pParse,
					   "DELETE FROM \"%s\" WHERE \"id\"=%d AND \"iid\">0",
					   TARANTOOL_SYS_INDEX_NAME, space_id);
		}

		/*  Remove primary index. */
		sqlite3NestedParse(pParse,
				   "DELETE FROM \"%s\" WHERE \"id\"=%d AND \"iid\"=0",
				   TARANTOOL_SYS_INDEX_NAME, space_id);
	}
	/* Delete records about the space from the _truncate. */
	sqlite3NestedParse(pParse, "DELETE FROM \""
			   TARANTOOL_SYS_TRUNCATE_NAME "\" WHERE \"id\" = %d",
			   space_id);

	Expr *id_value = sqlite3ExprInteger(db, space_id);
	const char *column = "id";
	/* Execute not nested DELETE of a space to account DROP TABLE
	 * changes.
	 */
	sqlite3DeleteByKey(pParse, TARANTOOL_SYS_SPACE_NAME,
			   &column, &id_value, 1);

	/* Remove the table entry from SQLite's internal schema and modify
	 * the schema cookie.
	 */

	sqlite3VdbeAddOp4(v, OP_DropTable, 0, 0, 0, pTab->zName, 0);
	sqlite3ChangeCookie(pParse);
	sqliteViewResetAll(db);
}

/*
 * This routine is called to do the work of a DROP TABLE statement.
 * pName is the name of the table to be dropped.
 */
void
sqlite3DropTable(Parse * pParse, SrcList * pName, int isView, int noErr)
{
	Table *pTab;
	Vdbe *v = sqlite3GetVdbe(pParse);
	sqlite3 *db = pParse->db;

	if (v == NULL || db->mallocFailed) {
		goto exit_drop_table;
	}
	/* Activate changes counting here to account
	 * DROP TABLE IF NOT EXISTS, if the table really does not
	 * exist.
	 */
	if (!pParse->nested)
		sqlite3VdbeCountChanges(v);
	assert(pParse->nErr == 0);
	assert(pName->nSrc == 1);
	if (sqlite3ReadSchema(pParse))
		goto exit_drop_table;
	if (noErr)
		db->suppressErr++;
	assert(isView == 0 || isView == LOCATE_VIEW);
	pTab = sqlite3LocateTableItem(pParse, isView, &pName->a[0]);
	if (noErr)
		db->suppressErr--;

	if (pTab == 0) {
		if (noErr)
			sqlite3CodeVerifySchema(pParse);
		goto exit_drop_table;
	}
#ifndef SQLITE_OMIT_AUTHORIZATION
	{
		int code;
		const char *zTab = MASTER_NAME;
		char *zDb = db->mdb.zDbSName;
		if (sqlite3AuthCheck(pParse, SQLITE_DELETE, zTab, 0, zDb)) {
			goto exit_drop_table;
		}
		if (isView)
			code = SQLITE_DROP_VIEW;
		else
			code = SQLITE_DROP_TABLE;
		if (sqlite3AuthCheck(pParse, code, pTab->zName, NULL, zDb)) {
			goto exit_drop_table;
		}
		if (sqlite3AuthCheck
		    (pParse, SQLITE_DELETE, pTab->zName, 0, zDb)) {
			goto exit_drop_table;
		}
	}
#endif
#ifndef SQLITE_OMIT_VIEW
	/* Ensure DROP TABLE is not used on a view, and DROP VIEW is not used
	 * on a table.
	 */
	if (isView && pTab->pSelect == 0) {
		sqlite3ErrorMsg(pParse, "use DROP TABLE to delete table %s",
				pTab->zName);
		goto exit_drop_table;
	}
	if (!isView && pTab->pSelect) {
		sqlite3ErrorMsg(pParse, "use DROP VIEW to delete view %s",
				pTab->zName);
		goto exit_drop_table;
	}
#endif

	/* Generate code to remove the table from Tarantool and internal SQL
	 * tables. Basically, it consists from 3 stages:
	 * 1. Delete statistics from _stat1 and _stat4 tables (if any exist)
	 * 2. In case of presence of FK constraints, i.e. current table is child
	 *    or parent, then start new transaction and erase from table
	 *    all data row by row. On each deletion check whether any FK
	 *    violations have occurred. If ones take place, then rollback
	 *    transaction and halt VDBE. Otherwise, commit transaction.
	 * 3. Drop table by truncating (if step 2 was skipped), removing
	 *    indexes from _index table and eventually tuple with corresponding
	 *    space_id from _space.
	 */

	sqlite3BeginWriteOperation(pParse, 1);
	sqlite3ClearStatTables(pParse, "tbl", pTab->zName);
	sqlite3FkDropTable(pParse, pName, pTab);
	sqlite3CodeDropTable(pParse, pTab, isView);

 exit_drop_table:
	sqlite3SrcListDelete(db, pName);
}

/*
 * This routine is called to create a new foreign key on the table
 * currently under construction.  pFromCol determines which columns
 * in the current table point to the foreign key.  If pFromCol==0 then
 * connect the key to the last column inserted.  pTo is the name of
 * the table referred to (a.k.a the "parent" table).  pToCol is a list
 * of tables in the parent pTo table.  flags contains all
 * information about the conflict resolution algorithms specified
 * in the ON DELETE, ON UPDATE and ON INSERT clauses.
 *
 * An FKey structure is created and added to the table currently
 * under construction in the pParse->pNewTable field.
 *
 * The foreign key is set for IMMEDIATE processing.  A subsequent call
 * to sqlite3DeferForeignKey() might change this to DEFERRED.
 */
void
sqlite3CreateForeignKey(Parse * pParse,	/* Parsing context */
			ExprList * pFromCol,	/* Columns in this table that point to other table */
			Token * pTo,	/* Name of the other table */
			ExprList * pToCol,	/* Columns in the other table */
			int flags	/* Conflict resolution algorithms. */
    )
{
	sqlite3 *db = pParse->db;
#ifndef SQLITE_OMIT_FOREIGN_KEY
	FKey *pFKey = 0;
	FKey *pNextTo;
	Table *p = pParse->pNewTable;
	int nByte;
	int i;
	int nCol;
	char *z;

	assert(pTo != 0);
	if (p == 0)
		goto fk_end;
	if (pFromCol == 0) {
		int iCol = p->nCol - 1;
		if (NEVER(iCol < 0))
			goto fk_end;
		if (pToCol && pToCol->nExpr != 1) {
			sqlite3ErrorMsg(pParse, "foreign key on %s"
					" should reference only one column of table %T",
					p->aCol[iCol].zName, pTo);
			goto fk_end;
		}
		nCol = 1;
	} else if (pToCol && pToCol->nExpr != pFromCol->nExpr) {
		sqlite3ErrorMsg(pParse,
				"number of columns in foreign key does not match the number of "
				"columns in the referenced table");
		goto fk_end;
	} else {
		nCol = pFromCol->nExpr;
	}
	nByte =
	    sizeof(*pFKey) + (nCol - 1) * sizeof(pFKey->aCol[0]) + pTo->n + 1;
	if (pToCol) {
		for (i = 0; i < pToCol->nExpr; i++) {
			nByte += sqlite3Strlen30(pToCol->a[i].zName) + 1;
		}
	}
	pFKey = sqlite3DbMallocZero(db, nByte);
	if (pFKey == 0) {
		goto fk_end;
	}
	pFKey->pFrom = p;
	pFKey->pNextFrom = p->pFKey;
	z = (char *)&pFKey->aCol[nCol];
	pFKey->zTo = z;
	memcpy(z, pTo->z, pTo->n);
	z[pTo->n] = 0;
	sqlite3NormalizeName(z);
	z += pTo->n + 1;
	pFKey->nCol = nCol;
	if (pFromCol == 0) {
		pFKey->aCol[0].iFrom = p->nCol - 1;
	} else {
		for (i = 0; i < nCol; i++) {
			int j;
			for (j = 0; j < p->nCol; j++) {
				if (strcmp
				    (p->aCol[j].zName,
				     pFromCol->a[i].zName) == 0) {
					pFKey->aCol[i].iFrom = j;
					break;
				}
			}
			if (j >= p->nCol) {
				sqlite3ErrorMsg(pParse,
						"unknown column \"%s\" in foreign key definition",
						pFromCol->a[i].zName);
				goto fk_end;
			}
		}
	}
	if (pToCol) {
		for (i = 0; i < nCol; i++) {
			int n = sqlite3Strlen30(pToCol->a[i].zName);
			pFKey->aCol[i].zCol = z;
			memcpy(z, pToCol->a[i].zName, n);
			z[n] = 0;
			z += n + 1;
		}
	}
	pFKey->isDeferred = 0;
	pFKey->aAction[0] = (u8) (flags & 0xff);	/* ON DELETE action */
	pFKey->aAction[1] = (u8) ((flags >> 8) & 0xff);	/* ON UPDATE action */

	pNextTo = (FKey *) sqlite3HashInsert(&p->pSchema->fkeyHash,
					     pFKey->zTo, (void *)pFKey);
	if (pNextTo == pFKey) {
		sqlite3OomFault(db);
		goto fk_end;
	}
	if (pNextTo) {
		assert(pNextTo->pPrevTo == 0);
		pFKey->pNextTo = pNextTo;
		pNextTo->pPrevTo = pFKey;
	}

	/* Link the foreign key to the table as the last step.
	 */
	p->pFKey = pFKey;
	pFKey = 0;

 fk_end:
	sqlite3DbFree(db, pFKey);
#endif				/* !defined(SQLITE_OMIT_FOREIGN_KEY) */
	sqlite3ExprListDelete(db, pFromCol);
	sqlite3ExprListDelete(db, pToCol);
}

/*
 * This routine is called when an INITIALLY IMMEDIATE or INITIALLY DEFERRED
 * clause is seen as part of a foreign key definition.  The isDeferred
 * parameter is 1 for INITIALLY DEFERRED and 0 for INITIALLY IMMEDIATE.
 * The behavior of the most recently created foreign key is adjusted
 * accordingly.
 */
void
sqlite3DeferForeignKey(Parse * pParse, int isDeferred)
{
#ifndef SQLITE_OMIT_FOREIGN_KEY
	Table *pTab;
	FKey *pFKey;
	if ((pTab = pParse->pNewTable) == 0 || (pFKey = pTab->pFKey) == 0)
		return;
	assert(isDeferred == 0 || isDeferred == 1);	/* EV: R-30323-21917 */
	pFKey->isDeferred = (u8) isDeferred;
#endif
}

/*
 * Generate code that will erase and refill index *pIdx.  This is
 * used to initialize a newly created index or to recompute the
 * content of an index in response to a REINDEX command.
 *
 * if memRootPage is not negative, it means that the index is newly
 * created.  The register specified by memRootPage contains the
 * root page number of the index.  If memRootPage is negative, then
 * the index already exists and must be cleared before being refilled and
 * the root page number of the index is taken from pIndex->tnum.
 */
static void
sqlite3RefillIndex(Parse * pParse, Index * pIndex, int memRootPage)
{
	Table *pTab = pIndex->pTable;	/* The table that is indexed */
	int iTab = pParse->nTab++;	/* Btree cursor used for pTab */
	int iIdx = pParse->nTab++;	/* Btree cursor used for pIndex */
	int iSorter;		/* Cursor opened by OpenSorter (if in use) */
	int addr1;		/* Address of top of loop */
	int addr2;		/* Address to jump to for next iteration */
	int tnum;		/* Root page of index */
	int iPartIdxLabel;	/* Jump to this label to skip a row */
	Vdbe *v;		/* Generate code into this virtual machine */
	KeyInfo *pKey;		/* KeyInfo for index */
	int regRecord;		/* Register holding assembled index record */
	sqlite3 *db = pParse->db;	/* The database connection */
#ifndef SQLITE_OMIT_AUTHORIZATION
	if (sqlite3AuthCheck(pParse, SQLITE_REINDEX, pIndex->zName, 0,
			     db->mdb.zDbSName)) {
		return;
	}
#endif
	v = sqlite3GetVdbe(pParse);
	if (v == 0)
		return;
	if (memRootPage >= 0) {
		tnum = memRootPage;
	} else {
		tnum = pIndex->tnum;
	}
	pKey = sqlite3KeyInfoOfIndex(pParse, db, pIndex);
	assert(pKey != 0 || db->mallocFailed || pParse->nErr);

	/* Open the sorter cursor if we are to use one. */
	iSorter = pParse->nTab++;
	sqlite3VdbeAddOp4(v, OP_SorterOpen, iSorter, 0, pIndex->nKeyCol,
			  (char *)
			  sqlite3KeyInfoRef(pKey), P4_KEYINFO);

	/* Open the table. Loop through all rows of the table, inserting index
	 * records into the sorter.
	 */
	sqlite3OpenTable(pParse, iTab, pTab, OP_OpenRead);
	addr1 = sqlite3VdbeAddOp2(v, OP_Rewind, iTab, 0);
	VdbeCoverage(v);
	regRecord = sqlite3GetTempReg(pParse);

	sqlite3GenerateIndexKey(pParse, pIndex, iTab, regRecord, 0,
				&iPartIdxLabel, 0, 0);
	sqlite3VdbeAddOp2(v, OP_SorterInsert, iSorter, regRecord);
	sqlite3ResolvePartIdxLabel(pParse, iPartIdxLabel);
	sqlite3VdbeAddOp2(v, OP_Next, iTab, addr1 + 1);
	VdbeCoverage(v);
	sqlite3VdbeJumpHere(v, addr1);
	if (memRootPage < 0)
		sqlite3VdbeAddOp2(v, OP_Clear, tnum, 0);
	sqlite3VdbeAddOp4(v, OP_OpenWrite, iIdx, tnum, 0,
			  (char *)pKey, P4_KEYINFO);
	sqlite3VdbeChangeP5(v,
			    OPFLAG_BULKCSR | ((memRootPage >= 0) ?
					      OPFLAG_P2ISREG : 0));

	addr1 = sqlite3VdbeAddOp2(v, OP_SorterSort, iSorter, 0);
	VdbeCoverage(v);
	if (IsUniqueIndex(pIndex)) {
		int j2 = sqlite3VdbeCurrentAddr(v) + 3;
		sqlite3VdbeGoto(v, j2);
		addr2 = sqlite3VdbeCurrentAddr(v);
		sqlite3VdbeAddOp4Int(v, OP_SorterCompare, iSorter, j2,
				     regRecord, pIndex->nKeyCol);
		VdbeCoverage(v);
		sqlite3UniqueConstraint(pParse, ON_CONFLICT_ACTION_ABORT,
					pIndex);
	} else {
		addr2 = sqlite3VdbeCurrentAddr(v);
	}
	sqlite3VdbeAddOp3(v, OP_SorterData, iSorter, regRecord, iIdx);
	sqlite3VdbeAddOp3(v, OP_Last, iIdx, 0, -1);
	sqlite3VdbeAddOp2(v, OP_IdxInsert, iIdx, regRecord);
	sqlite3VdbeChangeP5(v, OPFLAG_USESEEKRESULT);
	sqlite3ReleaseTempReg(pParse, regRecord);
	sqlite3VdbeAddOp2(v, OP_SorterNext, iSorter, addr2);
	VdbeCoverage(v);
	sqlite3VdbeJumpHere(v, addr1);

	sqlite3VdbeAddOp1(v, OP_Close, iTab);
	sqlite3VdbeAddOp1(v, OP_Close, iIdx);
	sqlite3VdbeAddOp1(v, OP_Close, iSorter);
}

/*
 * Allocate heap space to hold an Index object with nCol columns.
 *
 * Increase the allocation size to provide an extra nExtra bytes
 * of 8-byte aligned space after the Index object and return a
 * pointer to this extra space in *ppExtra.
 */
Index *
sqlite3AllocateIndexObject(sqlite3 * db,	/* Database connection */
			   i16 nCol,	/* Total number of columns in the index */
			   int nExtra,	/* Number of bytes of extra space to alloc */
			   char **ppExtra	/* Pointer to the "extra" space */
    )
{
	Index *p;		/* Allocated index object */
	int nByte;		/* Bytes of space for Index object + arrays */

	nByte = ROUND8(sizeof(Index)) +	/* Index structure  */
	    ROUND8(sizeof(char *) * nCol) +	/* Index.azColl     */
	    ROUND8(sizeof(LogEst) * (nCol + 1) +	/* Index.aiRowLogEst   */
		   sizeof(i16) * nCol +	/* Index.aiColumn   */
		   sizeof(u8) * nCol);	/* Index.aSortOrder */
	p = sqlite3DbMallocZero(db, nByte + nExtra);
	if (p) {
		char *pExtra = ((char *)p) + ROUND8(sizeof(Index));
		p->azColl = (const char **)pExtra;
		pExtra += ROUND8(sizeof(char *) * nCol);
		p->aiRowLogEst = (LogEst *) pExtra;
		pExtra += sizeof(LogEst) * (nCol + 1);
		p->aiColumn = (i16 *) pExtra;
		pExtra += sizeof(i16) * nCol;
		p->aSortOrder = (u8 *) pExtra;
		p->nColumn = nCol;
		p->nKeyCol = nCol;
		*ppExtra = ((char *)p) + nByte;
	}
	return p;
}

/*
 * Generate code to determine next free Iid in the space identified by
 * the iSpaceId. Return register number holding the result.
 */
static int
getNewIid(Parse * pParse, int iSpaceId, int iCursor)
{
	Vdbe *v = sqlite3GetVdbe(pParse);
	int iRes = ++pParse->nMem;
	int iKey = ++pParse->nMem;
	int iSeekInst, iGotoInst;

	sqlite3VdbeAddOp2(v, OP_Integer, iSpaceId, iKey);
	iSeekInst = sqlite3VdbeAddOp4Int(v, OP_SeekLE, iCursor, 0, iKey, 1);
	sqlite3VdbeAddOp4Int(v, OP_IdxLT, iCursor, 0, iKey, 1);

	/*
	 * If SeekLE succeeds, the control falls through here, skipping
	 * IdxLt.
	 *
	 * If it fails (no entry with the given key prefix: invalid spaceId)
	 * VDBE jumps to the next code block (jump target is IMM, fixed up
	 * later with sqlite3VdbeJumpHere()).
	 */
	iGotoInst = sqlite3VdbeAddOp0(v, OP_Goto);	/* Jump over Halt */

	/* Invalid spaceId detected. Halt now. */
	sqlite3VdbeJumpHere(v, iSeekInst);
	sqlite3VdbeJumpHere(v, iSeekInst + 1);
	sqlite3VdbeAddOp4(v,
			  OP_Halt, SQLITE_ERROR, ON_CONFLICT_ACTION_FAIL, 0,
			  sqlite3MPrintf(pParse->db, "Invalid space id: %d",
					 iSpaceId), P4_DYNAMIC);

	/* Fetch iid from the row and ++it. */
	sqlite3VdbeJumpHere(v, iGotoInst);
	sqlite3VdbeAddOp3(v, OP_Column, iCursor, 1, iRes);
	sqlite3VdbeAddOp2(v, OP_AddImm, iRes, 1);
	return iRes;
}

/*
 * Add new index to pTab indexes list
 *
 * When adding an index to the list of indexes for a table, we
 * maintain special order of the indexes in the list:
 * 1. PK (go first just for simplicity)
 * 2. ON_CONFLICT_ACTION_REPLACE indexes
 * 3. ON_CONFLICT_ACTION_IGNORE indexes
 * This is necessary for the correct constraint check
 * processing (in sqlite3GenerateConstraintChecks()) as part of
 * UPDATE and INSERT statements.
 */
static void
addIndexToTable(Index * pIndex, Table * pTab)
{
	if (IsPrimaryKeyIndex(pIndex)) {
		assert(sqlite3PrimaryKeyIndex(pTab) == NULL);
		pIndex->pNext = pTab->pIndex;
		pTab->pIndex = pIndex;
		return;
	}
	if (pIndex->onError != ON_CONFLICT_ACTION_REPLACE || pTab->pIndex == 0
	    || pTab->pIndex->onError == ON_CONFLICT_ACTION_REPLACE) {
		Index *pk = sqlite3PrimaryKeyIndex(pTab);
		if (pk) {
			pIndex->pNext = pk->pNext;
			pk->pNext = pIndex;
		} else {
			pIndex->pNext = pTab->pIndex;
			pTab->pIndex = pIndex;
		}
	} else {
		Index *pOther = pTab->pIndex;
		while (pOther->pNext && pOther->pNext->onError
		       != ON_CONFLICT_ACTION_REPLACE) {
			pOther = pOther->pNext;
		}
		pIndex->pNext = pOther->pNext;
		pOther->pNext = pIndex;
	}
}

/*
 * Create a new index for an SQL table.  pName1.pName2 is the name of the index
 * and pTblList is the name of the table that is to be indexed.  Both will
 * be NULL for a primary key or an index that is created to satisfy a
 * UNIQUE constraint.  If pTable and pIndex are NULL, use pParse->pNewTable
 * as the table to be indexed.  pParse->pNewTable is a table that is
 * currently being constructed by a CREATE TABLE statement.
 *
 * pList is a list of columns to be indexed.  pList will be NULL if this
 * is a primary key or unique-constraint on the most recent column added
 * to the table currently under construction.
 */
void
sqlite3CreateIndex(Parse * pParse,	/* All information about this parse */
		   Token * pName,	/* Index name. May be NULL */
		   SrcList * pTblName,	/* Table to index. Use pParse->pNewTable if 0 */
		   ExprList * pList,	/* A list of columns to be indexed */
		   int onError,	        /* ON_CONFLICT_ACTION_ABORT, _IGNORE,
					 * _REPLACE, or _NONE.
					 */
		   Token MAYBE_UNUSED * pStart,	/* The CREATE token that begins
						 * this statement
						 */
		   Expr * pPIWhere,	/* WHERE clause for partial indices */
		   int sortOrder,	/* Sort order of primary key when pList==NULL */
		   int ifNotExist,	/* Omit error if index already exists */
		   u8 idxType	/* The index type */
    )
{
	Table *pTab = 0;	/* Table to be indexed */
	Index *pIndex = 0;	/* The index to be created */
	char *zName = 0;	/* Name of the index */
	int nName;		/* Number of characters in zName */
	int i, j;
	DbFixer sFix;		/* For assigning database names to pTable */
	sqlite3 *db = pParse->db;
	struct ExprList_item *pListItem;	/* For looping over pList */
	int nExtra = 0;		/* Space allocated for zExtra[] */
	char *zExtra = 0;	/* Extra space after the Index object */
	struct session *user_session = current_session();

	if (db->mallocFailed || pParse->nErr > 0) {
		goto exit_create_index;
	}
	/* Do not account nested operations: the count of such
	 * operations depends on Tarantool data dictionary internals,
	 * such as data layout in system spaces. Also do not account
	 * PRIMARY KEY and UNIQUE constraint - they had been accounted
	 * in CREATE TABLE already.
	 */
	if (!pParse->nested && idxType == SQLITE_IDXTYPE_APPDEF) {
		Vdbe *v = sqlite3GetVdbe(pParse);
		if (v == NULL)
			goto exit_create_index;
		sqlite3VdbeCountChanges(v);
	}
	if (SQLITE_OK != sqlite3ReadSchema(pParse)) {
		goto exit_create_index;
	}

	/*
	 * Find the table that is to be indexed.  Return early if not found.
	 */
	if (pTblName != 0) {

		/* Use the two-part index name to determine the database
		 * to search for the table. 'Fix' the table name to this db
		 * before looking up the table.
		 */
		assert(pName && pName->z);

		sqlite3FixInit(&sFix, pParse, "index", pName);
		if (sqlite3FixSrcList(&sFix, pTblName)) {
			/* Because the parser constructs pTblName from a single identifier,
			 * sqlite3FixSrcList can never fail.
			 */
			assert(0);
		}
		pTab = sqlite3LocateTableItem(pParse, 0, &pTblName->a[0]);
		assert(db->mallocFailed == 0 || pTab == 0);
		if (pTab == 0)
			goto exit_create_index;
		sqlite3PrimaryKeyIndex(pTab);
	} else {
		assert(pName == 0);
		assert(pStart == 0);
		pTab = pParse->pNewTable;
		if (!pTab)
			goto exit_create_index;
	}

	assert(pTab != 0);
	assert(pParse->nErr == 0);
#ifndef SQLITE_OMIT_VIEW
	if (pTab->pSelect) {
		sqlite3ErrorMsg(pParse, "views may not be indexed");
		goto exit_create_index;
	}
#endif
	/*
	 * Find the name of the index.  Make sure there is not already another
	 * index or table with the same name.
	 *
	 * Exception:  If we are reading the names of permanent indices from the
	 * Tarantool schema (because some other process changed the schema) and
	 * one of the index names collides with the name of a temporary table or
	 * index, then we will continue to process this index.
	 *
	 * If pName==0 it means that we are
	 * dealing with a primary key or UNIQUE constraint.  We have to invent our
	 * own name.
	 */
	if (pName) {
		zName = sqlite3NameFromToken(db, pName);
		if (zName == 0)
			goto exit_create_index;
		assert(pName->z != 0);
		if (!db->init.busy) {
			if (sqlite3FindTable(db, zName) != 0) {
				sqlite3ErrorMsg(pParse,
						"there is already a table named %s",
						zName);
				goto exit_create_index;
			}
		}
		if (sqlite3FindIndex(db, zName, pTab) != 0) {
			if (!ifNotExist) {
				sqlite3ErrorMsg(pParse,
						"index %s.%s already exists",
						pTab->zName, zName);
			} else {
				assert(!db->init.busy);
				sqlite3CodeVerifySchema(pParse);
			}
			goto exit_create_index;
		}
	} else {
		int n;
		Index *pLoop;
		for (pLoop = pTab->pIndex, n = 1; pLoop;
		     pLoop = pLoop->pNext, n++) {
		}
		zName =
		    sqlite3MPrintf(db, "sqlite_autoindex_%s_%d", pTab->zName,
				   n);
		if (zName == 0) {
			goto exit_create_index;
		}
	}

	/* Check for authorization to create an index.
	 */
#ifndef SQLITE_OMIT_AUTHORIZATION
	{
		const char *zDb = pDb->zDbSName;
		if (sqlite3AuthCheck
		    (pParse, SQLITE_INSERT, MASTER_NAME, 0, zDb)) {
			goto exit_create_index;
		}
		i = SQLITE_CREATE_INDEX;
		if (sqlite3AuthCheck(pParse, i, zName, pTab->zName, zDb)) {
			goto exit_create_index;
		}
	}
#endif

	/* If pList==0, it means this routine was called to make a primary
	 * key out of the last column added to the table under construction.
	 * So create a fake list to simulate this.
	 */
	if (pList == 0) {
		Token prevCol;
		sqlite3TokenInit(&prevCol, pTab->aCol[pTab->nCol - 1].zName);
		pList = sqlite3ExprListAppend(pParse, 0,
					      sqlite3ExprAlloc(db, TK_ID,
							       &prevCol, 0));
		if (pList == 0)
			goto exit_create_index;
		assert(pList->nExpr == 1);
		sqlite3ExprListSetSortOrder(pList, sortOrder);
	} else {
		sqlite3ExprListCheckLength(pParse, pList, "index");
	}

	/* Figure out how many bytes of space are required to store explicitly
	 * specified collation sequence names.
	 */
	for (i = 0; i < pList->nExpr; i++) {
		Expr *pExpr = pList->a[i].pExpr;
		assert(pExpr != 0);
		if (pExpr->op == TK_COLLATE) {
			nExtra += (1 + sqlite3Strlen30(pExpr->u.zToken));
		}
	}

	/*
	 * Allocate the index structure.
	 */
	nName = sqlite3Strlen30(zName);
	pIndex = sqlite3AllocateIndexObject(db, pList->nExpr,
					    nName + nExtra + 1, &zExtra);
	if (db->mallocFailed) {
		goto exit_create_index;
	}
	assert(EIGHT_BYTE_ALIGNMENT(pIndex->aiRowLogEst));
	assert(EIGHT_BYTE_ALIGNMENT(pIndex->azColl));
	pIndex->zName = zExtra;
	zExtra += nName + 1;
	memcpy(pIndex->zName, zName, nName + 1);
	pIndex->pTable = pTab;
	pIndex->onError = (u8) onError;
	pIndex->uniqNotNull = onError != ON_CONFLICT_ACTION_NONE;
	pIndex->idxType = idxType;
	pIndex->pSchema = db->mdb.pSchema;
	pIndex->nKeyCol = pList->nExpr;
	/* Tarantool have access to each column by any index */
	pIndex->isCovering = 1;
	if (pPIWhere) {
		sqlite3ResolveSelfReference(pParse, pTab, NC_PartIdx, pPIWhere,
					    0);
		pIndex->pPartIdxWhere = pPIWhere;
		pPIWhere = 0;
	}

	/* Analyze the list of expressions that form the terms of the index and
	 * report any errors.  In the common case where the expression is exactly
	 * a table column, store that column in aiColumn[].  For general expressions,
	 * populate pIndex->aColExpr and store XN_EXPR (-2) in aiColumn[].
	 *
	 * TODO: Issue a warning if two or more columns of the index are identical.
	 * TODO: Issue a warning if the table primary key is used as part of the
	 * index key.
	 */
	for (i = 0, pListItem = pList->a; i < pList->nExpr; i++, pListItem++) {
		Expr *pCExpr;	/* The i-th index expression */
		int requestedSortOrder;	/* ASC or DESC on the i-th expression */
		const char *zColl;	/* Collation sequence name */
		sqlite3ResolveSelfReference(pParse, pTab, NC_IdxExpr,
					    pListItem->pExpr, 0);
		if (pParse->nErr)
			goto exit_create_index;
		pCExpr = sqlite3ExprSkipCollate(pListItem->pExpr);
		if (pCExpr->op != TK_COLUMN) {
			sqlite3ErrorMsg(pParse,
					"expressions prohibited in PRIMARY KEY and "
					"UNIQUE constraints");
			goto exit_create_index;
		} else {
			j = pCExpr->iColumn;
			assert(j <= 0x7fff);
			if (j < 0) {
				j = pTab->iPKey;
			} else if (pTab->aCol[j].notNull == 0) {
				pIndex->uniqNotNull = 0;
			}
			pIndex->aiColumn[i] = (i16) j;
		}
		zColl = 0;
		if (pListItem->pExpr->op == TK_COLLATE) {
			int nColl;
			zColl = pListItem->pExpr->u.zToken;
			nColl = sqlite3Strlen30(zColl) + 1;
			assert(nExtra >= nColl);
			memcpy(zExtra, zColl, nColl);
			zColl = zExtra;
			zExtra += nColl;
			nExtra -= nColl;
		} else if (j >= 0) {
			zColl = column_collation_name(pTab, j);
		}
		if (!zColl)
			zColl = sqlite3StrBINARY;
		if (!db->init.busy && !sqlite3LocateCollSeq(pParse, db, zColl)) {
			goto exit_create_index;
		}
		pIndex->azColl[i] = zColl;
		/* Tarantool: DESC indexes are not supported so far.
		 * See gh-3016.
		 */
		requestedSortOrder = pListItem->sortOrder & 0;
		pIndex->aSortOrder[i] = (u8) requestedSortOrder;
	}

	sqlite3DefaultRowEst(pIndex);
	if (pParse->pNewTable == 0)
		estimateIndexWidth(pIndex);

	assert(pTab->iPKey < 0
	       || sqlite3ColumnOfIndex(pIndex, pTab->iPKey) >= 0);

	if (pTab == pParse->pNewTable) {
		/* This routine has been called to create an automatic index as a
		 * result of a PRIMARY KEY or UNIQUE clause on a column definition, or
		 * a PRIMARY KEY or UNIQUE clause following the column definitions.
		 * i.e. one of:
		 *
		 * CREATE TABLE t(x PRIMARY KEY, y);
		 * CREATE TABLE t(x, y, UNIQUE(x, y));
		 *
		 * Either way, check to see if the table already has such an index. If
		 * so, don't bother creating this one. This only applies to
		 * automatically created indices. Users can do as they wish with
		 * explicit indices.
		 *
		 * Two UNIQUE or PRIMARY KEY constraints are considered equivalent
		 * (and thus suppressing the second one) even if they have different
		 * sort orders.
		 *
		 * If there are different collating sequences or if the columns of
		 * the constraint occur in different orders, then the constraints are
		 * considered distinct and both result in separate indices.
		 */
		Index *pIdx;
		for (pIdx = pTab->pIndex; pIdx; pIdx = pIdx->pNext) {
			int k;
			assert(IsUniqueIndex(pIdx));
			assert(pIdx->idxType != SQLITE_IDXTYPE_APPDEF);
			assert(IsUniqueIndex(pIndex));

			if (pIdx->nKeyCol != pIndex->nKeyCol)
				continue;
			for (k = 0; k < pIdx->nKeyCol; k++) {
				const char *z1;
				const char *z2;
				assert(pIdx->aiColumn[k] >= 0);
				if (pIdx->aiColumn[k] != pIndex->aiColumn[k])
					break;
				z1 = index_collation_name(pIdx, k);
				z2 = index_collation_name(pIndex, k);
				if (strcmp(z1, z2))
					break;
			}
			if (k == pIdx->nKeyCol) {
				if (pIdx->onError != pIndex->onError) {
					/* This constraint creates the same index as a previous
					 * constraint specified somewhere in the CREATE TABLE statement.
					 * However the ON CONFLICT clauses are different. If both this
					 * constraint and the previous equivalent constraint have explicit
					 * ON CONFLICT clauses this is an error. Otherwise, use the
					 * explicitly specified behavior for the index.
					 */
					if (!
					    (pIdx->onError == ON_CONFLICT_ACTION_DEFAULT
					     || pIndex->onError ==
					     ON_CONFLICT_ACTION_DEFAULT)) {
						sqlite3ErrorMsg(pParse,
								"conflicting ON CONFLICT clauses specified",
								0);
					}
					if (pIdx->onError == ON_CONFLICT_ACTION_DEFAULT) {
						pIdx->onError = pIndex->onError;
					}
				}
				if (idxType == SQLITE_IDXTYPE_PRIMARYKEY)
					pIdx->idxType = idxType;
				goto exit_create_index;
			}
		}
	}

	/* Link the new Index structure to its table and to the other
	 * in-memory database structures.
	 */
	assert(pParse->nErr == 0);
	if (db->init.busy) {
		Index *p;
		p = sqlite3HashInsert(&pTab->idxHash, pIndex->zName, pIndex);
		if (p) {
			assert(p == pIndex);	/* Malloc must have failed */
			sqlite3OomFault(db);
			goto exit_create_index;
		}
		user_session->sql_flags |= SQLITE_InternChanges;
		pIndex->tnum = db->init.newTnum;
	}

	/* If this is the initial CREATE INDEX statement (or CREATE TABLE if the
	 * index is an implied index for a UNIQUE or PRIMARY KEY constraint) then
	 * emit code to to insert new index into Tarantool.
	 * But, do not do this if we are simply parsing the schema, or if this
	 * index is the PRIMARY KEY index.
	 *
	 * If pTblName==0 it means this index is generated as an implied PRIMARY KEY
	 * or UNIQUE index in a CREATE TABLE statement.  Since the table
	 * has just been created, it contains no data and the index initialization
	 * step can be skipped.
	 */
	else if (pTblName) {
		Vdbe *v;
		char *zStmt;
		Table *pSysIndex;
		int iCursor = pParse->nTab++;
		int iSpaceId, iIndexId, iFirstSchemaCol;

		v = sqlite3GetVdbe(pParse);
		if (v == 0)
			goto exit_create_index;

		sqlite3BeginWriteOperation(pParse, 1);

		pSysIndex =
		    sqlite3HashFind(&pParse->db->mdb.pSchema->tblHash,
				    TARANTOOL_SYS_INDEX_NAME);
		if (NEVER(!pSysIndex))
			return;

		sqlite3OpenTable(pParse, iCursor, pSysIndex, OP_OpenWrite);
		sqlite3VdbeChangeP5(v, OPFLAG_SEEKEQ);

		/* Gather the complete text of the CREATE INDEX statement into
		 * the zStmt variable
		 */
		assert(pStart); {
			int n =
			    (int)(pParse->sLastToken.z - pName->z) +
			    pParse->sLastToken.n;
			if (pName->z[n - 1] == ';')
				n--;
			/* A named index with an explicit CREATE INDEX statement */
			zStmt = sqlite3MPrintf(db, "CREATE%s INDEX %.*s",
					       onError ==
					       ON_CONFLICT_ACTION_NONE
					       ? "" : " UNIQUE", n,
					       pName->z);
		}

		iSpaceId = SQLITE_PAGENO_TO_SPACEID(pTab->tnum);
		iIndexId = getNewIid(pParse, iSpaceId, iCursor);
		createIndex(pParse, pIndex, iSpaceId, iIndexId, zStmt,
			    pSysIndex, iCursor);
		sqlite3VdbeAddOp1(v, OP_Close, iCursor);

		/* consumes zStmt */
		iFirstSchemaCol =
		    makeIndexSchemaRecord(pParse, pIndex, iSpaceId, iIndexId,
					  zStmt);

		/* Reparse the schema. Code an OP_Expire
		 * to invalidate all pre-compiled statements.
		 */
		sqlite3ChangeCookie(pParse);
		sqlite3VdbeAddParseSchema2Op(v, iFirstSchemaCol, 4);
		sqlite3VdbeAddOp0(v, OP_Expire);
	}

	/* When adding an index to the list of indexes for a table, we
	 * maintain special order of the indexes in the list:
	 * 1. PK (go first just for simplicity)
	 * 2. ON_CONFLICT_ACTION_REPLACE indexes
	 * 3. ON_CONFLICT_ACTION_IGNORE indexes
	 * This is necessary for the correct constraint check
	 * processing (in sqlite3GenerateConstraintChecks()) as part of
	 * UPDATE and INSERT statements.
	 */

	if (!(db->init.busy || pTblName == 0))
		goto exit_create_index;
	addIndexToTable(pIndex, pTab);
	pIndex = 0;

	/* Clean up before exiting */
 exit_create_index:
	if (pIndex)
		freeIndex(db, pIndex);
	sqlite3ExprDelete(db, pPIWhere);
	sqlite3ExprListDelete(db, pList);
	sqlite3SrcListDelete(db, pTblName);
	sqlite3DbFree(db, zName);
}

/*
 * Fill the Index.aiRowEst[] array with default information - information
 * to be used when we have not run the ANALYZE command.
 *
 * aiRowEst[0] is supposed to contain the number of elements in the index.
 * Since we do not know, guess 1 million.  aiRowEst[1] is an estimate of the
 * number of rows in the table that match any particular value of the
 * first column of the index.  aiRowEst[2] is an estimate of the number
 * of rows that match any particular combination of the first 2 columns
 * of the index.  And so forth.  It must always be the case that
*
 *           aiRowEst[N]<=aiRowEst[N-1]
 *           aiRowEst[N]>=1
 *
 * Apart from that, we have little to go on besides intuition as to
 * how aiRowEst[] should be initialized.  The numbers generated here
 * are based on typical values found in actual indices.
 */
void
sqlite3DefaultRowEst(Index * pIdx)
{
	/*                10,  9,  8,  7,  6 */
	LogEst aVal[] = { 33, 32, 30, 28, 26 };
	LogEst *a = pIdx->aiRowLogEst;
	int nCopy = MIN(ArraySize(aVal), pIdx->nKeyCol);
	int i;

	/* Set the first entry (number of rows in the index) to the estimated
	 * number of rows in the table, or half the number of rows in the table
	 * for a partial index.   But do not let the estimate drop below 10.
	 */
	a[0] = pIdx->pTable->nRowLogEst;
	if (pIdx->pPartIdxWhere != 0)
		a[0] -= 10;
	assert(10 == sqlite3LogEst(2));
	if (a[0] < 33)
		a[0] = 33;
	assert(33 == sqlite3LogEst(10));

	/* Estimate that a[1] is 10, a[2] is 9, a[3] is 8, a[4] is 7, a[5] is
	 * 6 and each subsequent value (if any) is 5.
	 */
	memcpy(&a[1], aVal, nCopy * sizeof(LogEst));
	for (i = nCopy + 1; i <= pIdx->nKeyCol; i++) {
		a[i] = 23;
		assert(23 == sqlite3LogEst(5));
	}

	assert(0 == sqlite3LogEst(1));
	if (IsUniqueIndex(pIdx))
		a[pIdx->nKeyCol] = 0;
}

/*
 * This routine will drop an existing named index.  This routine
 * implements the DROP INDEX statement.
 */
void
sqlite3DropIndex(Parse * pParse, SrcList * pName, Token * pName2, int ifExists)
{
	Index *pIndex;
	Vdbe *v = sqlite3GetVdbe(pParse);
	sqlite3 *db = pParse->db;
	char *zTableName = 0;

	assert(pParse->nErr == 0);	/* Never called with prior errors */
	assert(pName2 != 0);

	if (db->mallocFailed) {
		goto exit_drop_index;
	}
	assert(v != NULL);
	/* Do not account nested operations: the count of such
	 * operations depends on Tarantool data dictionary internals,
	 * such as data layout in system spaces.
	 */
	if (!pParse->nested)
		sqlite3VdbeCountChanges(v);
	assert(pName->nSrc == 1);
	if (SQLITE_OK != sqlite3ReadSchema(pParse)) {
		goto exit_drop_index;
	}

	assert(pName2->n > 0);
	zTableName = sqlite3NameFromToken(db, pName2);

	pIndex = sqlite3LocateIndex(db, pName->a[0].zName, zTableName);
	if (pIndex == 0) {
		if (!ifExists) {
			sqlite3ErrorMsg(pParse, "no such index: %s.%S",
					zTableName, pName, 0);
		} else {
			sqlite3CodeVerifySchema(pParse);
		}
		pParse->checkSchema = 1;
		goto exit_drop_index;
	}
	if (pIndex->idxType != SQLITE_IDXTYPE_APPDEF) {
		sqlite3ErrorMsg(pParse, "index associated with UNIQUE "
				"or PRIMARY KEY constraint cannot be dropped",
				0);
		goto exit_drop_index;
	}
#ifndef SQLITE_OMIT_AUTHORIZATION
	{
		int code = SQLITE_DROP_INDEX;
		const char *zDb = db->mdb.zDbSName;
		const char *zTab = MASTER_NAME;
		if (sqlite3AuthCheck(pParse, SQLITE_DELETE, zTab, 0, zDb)) {
			goto exit_drop_index;
		}
		if (sqlite3AuthCheck(pParse, code, pIndex->zName,
				     pIndex->pTable->zName, zDb)) {
			goto exit_drop_index;
		}
	}
#endif

	/* Generate code to remove the index and from the master table */
	sqlite3BeginWriteOperation(pParse, 1);
	const char *columns[2] = { "id", "iid" };
	Expr *values[2];
	values[0] =
	    sqlite3ExprInteger(db, SQLITE_PAGENO_TO_SPACEID(pIndex->tnum));
	values[1] =
	    sqlite3ExprInteger(db, SQLITE_PAGENO_TO_INDEXID(pIndex->tnum));
	sqlite3DeleteByKey(pParse, TARANTOOL_SYS_INDEX_NAME,
			   columns, values, 2);
	sqlite3ClearStatTables(pParse, "idx", pIndex->zName);
	sqlite3ChangeCookie(pParse);

	sqlite3VdbeAddOp3(v, OP_DropIndex, 0, 0, 0);
	sqlite3VdbeAppendP4(v, pIndex, P4_INDEX);

 exit_drop_index:
	sqlite3SrcListDelete(db, pName);
	sqlite3DbFree(db, zTableName);
}

/*
 * pArray is a pointer to an array of objects. Each object in the
 * array is szEntry bytes in size. This routine uses sqlite3DbRealloc()
 * to extend the array so that there is space for a new object at the end.
 *
 * When this function is called, *pnEntry contains the current size of
 * the array (in entries - so the allocation is ((*pnEntry) * szEntry) bytes
 * in total).
 *
 * If the realloc() is successful (i.e. if no OOM condition occurs), the
 * space allocated for the new object is zeroed, *pnEntry updated to
 * reflect the new size of the array and a pointer to the new allocation
 * returned. *pIdx is set to the index of the new array entry in this case.
 *
 * Otherwise, if the realloc() fails, *pIdx is set to -1, *pnEntry remains
 * unchanged and a copy of pArray returned.
 */
void *
sqlite3ArrayAllocate(sqlite3 * db,	/* Connection to notify of malloc failures */
		     void *pArray,	/* Array of objects.  Might be reallocated */
		     int szEntry,	/* Size of each object in the array */
		     int *pnEntry,	/* Number of objects currently in use */
		     int *pIdx	/* Write the index of a new slot here */
    )
{
	char *z;
	int n = *pnEntry;
	if ((n & (n - 1)) == 0) {
		int sz = (n == 0) ? 1 : 2 * n;
		void *pNew = sqlite3DbRealloc(db, pArray, sz * szEntry);
		if (pNew == 0) {
			*pIdx = -1;
			return pArray;
		}
		pArray = pNew;
	}
	z = (char *)pArray;
	memset(&z[n * szEntry], 0, szEntry);
	*pIdx = n;
	++*pnEntry;
	return pArray;
}

/*
 * Append a new element to the given IdList.  Create a new IdList if
 * need be.
 *
 * A new IdList is returned, or NULL if malloc() fails.
 */
IdList *
sqlite3IdListAppend(sqlite3 * db, IdList * pList, Token * pToken)
{
	int i;
	if (pList == 0) {
		pList = sqlite3DbMallocZero(db, sizeof(IdList));
		if (pList == 0)
			return 0;
	}
	pList->a = sqlite3ArrayAllocate(db,
					pList->a,
					sizeof(pList->a[0]), &pList->nId, &i);
	if (i < 0) {
		sqlite3IdListDelete(db, pList);
		return 0;
	}
	pList->a[i].zName = sqlite3NameFromToken(db, pToken);
	return pList;
}

/*
 * Delete an IdList.
 */
void
sqlite3IdListDelete(sqlite3 * db, IdList * pList)
{
	int i;
	if (pList == 0)
		return;
	for (i = 0; i < pList->nId; i++) {
		sqlite3DbFree(db, pList->a[i].zName);
	}
	sqlite3DbFree(db, pList->a);
	sqlite3DbFree(db, pList);
}

/*
 * Return the index in pList of the identifier named zId.  Return -1
 * if not found.
 */
int
sqlite3IdListIndex(IdList * pList, const char *zName)
{
	int i;
	if (pList == 0)
		return -1;
	for (i = 0; i < pList->nId; i++) {
		if (strcmp(pList->a[i].zName, zName) == 0)
			return i;
	}
	return -1;
}

/*
 * Expand the space allocated for the given SrcList object by
 * creating nExtra new slots beginning at iStart.  iStart is zero based.
 * New slots are zeroed.
 *
 * For example, suppose a SrcList initially contains two entries: A,B.
 * To append 3 new entries onto the end, do this:
 *
 *    sqlite3SrcListEnlarge(db, pSrclist, 3, 2);
 *
 * After the call above it would contain:  A, B, nil, nil, nil.
 * If the iStart argument had been 1 instead of 2, then the result
 * would have been:  A, nil, nil, nil, B.  To prepend the new slots,
 * the iStart value would be 0.  The result then would
 * be: nil, nil, nil, A, B.
 *
 * If a memory allocation fails the SrcList is unchanged.  The
 * db->mallocFailed flag will be set to true.
 */
SrcList *
sqlite3SrcListEnlarge(sqlite3 * db,	/* Database connection to notify of OOM errors */
		      SrcList * pSrc,	/* The SrcList to be enlarged */
		      int nExtra,	/* Number of new slots to add to pSrc->a[] */
		      int iStart	/* Index in pSrc->a[] of first new slot */
    )
{
	int i;

	/* Sanity checking on calling parameters */
	assert(iStart >= 0);
	assert(nExtra >= 1);
	assert(pSrc != 0);
	assert(iStart <= pSrc->nSrc);

	/* Allocate additional space if needed */
	if ((u32) pSrc->nSrc + nExtra > pSrc->nAlloc) {
		SrcList *pNew;
		int nAlloc = pSrc->nSrc * 2 + nExtra;
		int nGot;
		pNew = sqlite3DbRealloc(db, pSrc,
					sizeof(*pSrc) + (nAlloc -
							 1) *
					sizeof(pSrc->a[0]));
		if (pNew == 0) {
			assert(db->mallocFailed);
			return pSrc;
		}
		pSrc = pNew;
		nGot =
		    (sqlite3DbMallocSize(db, pNew) -
		     sizeof(*pSrc)) / sizeof(pSrc->a[0]) + 1;
		pSrc->nAlloc = nGot;
	}

	/* Move existing slots that come after the newly inserted slots
	 * out of the way
	 */
	for (i = pSrc->nSrc - 1; i >= iStart; i--) {
		pSrc->a[i + nExtra] = pSrc->a[i];
	}
	pSrc->nSrc += nExtra;

	/* Zero the newly allocated slots */
	memset(&pSrc->a[iStart], 0, sizeof(pSrc->a[0]) * nExtra);
	for (i = iStart; i < iStart + nExtra; i++) {
		pSrc->a[i].iCursor = -1;
	}

	/* Return a pointer to the enlarged SrcList */
	return pSrc;
}

SrcList *
sql_alloc_src_list(sqlite3 *db)
{
	SrcList *pList;

	pList = sqlite3DbMallocRawNN(db, sizeof(SrcList));
	if (pList == 0)
		return NULL;
	pList->nAlloc = 1;
	pList->nSrc = 1;
	memset(&pList->a[0], 0, sizeof(pList->a[0]));
	pList->a[0].iCursor = -1;
	return pList;
}

/*
 * Append a new table name to the given SrcList.  Create a new SrcList if
 * need be.  A new entry is created in the SrcList even if pTable is NULL.
 *
 * A SrcList is returned, or NULL if there is an OOM error.  The returned
 * SrcList might be the same as the SrcList that was input or it might be
 * a new one.  If an OOM error does occurs, then the prior value of pList
 * that is input to this routine is automatically freed.
 *
 * If pDatabase is not null, it means that the table has an optional
 * database name prefix.  Like this:  "database.table".  The pDatabase
 * points to the table name and the pTable points to the database name.
 * The SrcList.a[].zName field is filled with the table name which might
 * come from pTable (if pDatabase is NULL) or from pDatabase.
 * SrcList.a[].zDatabase is filled with the database name from pTable,
 * or with NULL if no database is specified.
 *
 * In other words, if call like this:
 *
 *         sqlite3SrcListAppend(D,A,B,0);
 *
 * Then B is a table name and the database name is unspecified.  If called
 * like this:
 *
 *         sqlite3SrcListAppend(D,A,B,C);
 *
 * Then C is the table name and B is the database name.  If C is defined
 * then so is B.  In other words, we never have a case where:
 *
 *         sqlite3SrcListAppend(D,A,0,C);
 *
 * Both pTable and pDatabase are assumed to be quoted.  They are dequoted
 * before being added to the SrcList.
 */
SrcList *
sqlite3SrcListAppend(sqlite3 * db,	/* Connection to notify of malloc failures */
		     SrcList * pList,	/* Append to this SrcList. NULL creates a new SrcList */
		     Token * pTable	/* Table to append */
    )
{
	struct SrcList_item *pItem;
	assert(db != 0);
	if (pList == 0) {
		pList = sql_alloc_src_list(db);
		if (pList == 0)
			return 0;
	} else {
		pList = sqlite3SrcListEnlarge(db, pList, 1, pList->nSrc);
	}
	if (db->mallocFailed) {
		sqlite3SrcListDelete(db, pList);
		return 0;
	}
	pItem = &pList->a[pList->nSrc - 1];
	pItem->zName = sqlite3NameFromToken(db, pTable);
	return pList;
}

/*
 * Assign VdbeCursor index numbers to all tables in a SrcList
 */
void
sqlite3SrcListAssignCursors(Parse * pParse, SrcList * pList)
{
	int i;
	struct SrcList_item *pItem;
	assert(pList || pParse->db->mallocFailed);
	if (pList) {
		for (i = 0, pItem = pList->a; i < pList->nSrc; i++, pItem++) {
			if (pItem->iCursor >= 0)
				break;
			pItem->iCursor = pParse->nTab++;
			if (pItem->pSelect) {
				sqlite3SrcListAssignCursors(pParse,
							    pItem->pSelect->
							    pSrc);
			}
		}
	}
}

/*
 * Delete an entire SrcList including all its substructure.
 */
void
sqlite3SrcListDelete(sqlite3 * db, SrcList * pList)
{
	int i;
	struct SrcList_item *pItem;
	if (pList == 0)
		return;
	for (pItem = pList->a, i = 0; i < pList->nSrc; i++, pItem++) {
		sqlite3DbFree(db, pItem->zName);
		sqlite3DbFree(db, pItem->zAlias);
		if (pItem->fg.isIndexedBy)
			sqlite3DbFree(db, pItem->u1.zIndexedBy);
		if (pItem->fg.isTabFunc)
			sqlite3ExprListDelete(db, pItem->u1.pFuncArg);
		sqlite3DeleteTable(db, pItem->pTab);
		sqlite3SelectDelete(db, pItem->pSelect);
		sqlite3ExprDelete(db, pItem->pOn);
		sqlite3IdListDelete(db, pItem->pUsing);
	}
	sqlite3DbFree(db, pList);
}

/*
 * This routine is called by the parser to add a new term to the
 * end of a growing FROM clause.  The "p" parameter is the part of
 * the FROM clause that has already been constructed.  "p" is NULL
 * if this is the first term of the FROM clause.  pTable and pDatabase
 * are the name of the table and database named in the FROM clause term.
 * pDatabase is NULL if the database name qualifier is missing - the
 * usual case.  If the term has an alias, then pAlias points to the
 * alias token.  If the term is a subquery, then pSubquery is the
 * SELECT statement that the subquery encodes.  The pTable and
 * pDatabase parameters are NULL for subqueries.  The pOn and pUsing
 * parameters are the content of the ON and USING clauses.
 *
 * Return a new SrcList which encodes is the FROM with the new
 * term added.
 */
SrcList *
sqlite3SrcListAppendFromTerm(Parse * pParse,	/* Parsing context */
			     SrcList * p,	/* The left part of the FROM clause already seen */
			     Token * pTable,	/* Name of the table to add to the FROM clause */
			     Token * pAlias,	/* The right-hand side of the AS subexpression */
			     Select * pSubquery,	/* A subquery used in place of a table name */
			     Expr * pOn,	/* The ON clause of a join */
			     IdList * pUsing	/* The USING clause of a join */
    )
{
	struct SrcList_item *pItem;
	sqlite3 *db = pParse->db;
	if (!p && (pOn || pUsing)) {
		sqlite3ErrorMsg(pParse, "a JOIN clause is required before %s",
				(pOn ? "ON" : "USING")
		    );
		goto append_from_error;
	}
	p = sqlite3SrcListAppend(db, p, pTable);
	if (p == 0 || NEVER(p->nSrc == 0)) {
		goto append_from_error;
	}
	pItem = &p->a[p->nSrc - 1];
	assert(pAlias != 0);
	if (pAlias->n) {
		pItem->zAlias = sqlite3NameFromToken(db, pAlias);
	}
	pItem->pSelect = pSubquery;
	pItem->pOn = pOn;
	pItem->pUsing = pUsing;
	return p;

 append_from_error:
	assert(p == 0);
	sqlite3ExprDelete(db, pOn);
	sqlite3IdListDelete(db, pUsing);
	sqlite3SelectDelete(db, pSubquery);
	return 0;
}

/*
 * Add an INDEXED BY or NOT INDEXED clause to the most recently added
 * element of the source-list passed as the second argument.
 */
void
sqlite3SrcListIndexedBy(Parse * pParse, SrcList * p, Token * pIndexedBy)
{
	assert(pIndexedBy != 0);
	if (p && ALWAYS(p->nSrc > 0)) {
		struct SrcList_item *pItem = &p->a[p->nSrc - 1];
		assert(pItem->fg.notIndexed == 0);
		assert(pItem->fg.isIndexedBy == 0);
		assert(pItem->fg.isTabFunc == 0);
		if (pIndexedBy->n == 1 && !pIndexedBy->z) {
			/* A "NOT INDEXED" clause was supplied. See parse.y
			 * construct "indexed_opt" for details.
			 */
			pItem->fg.notIndexed = 1;
		} else {
			pItem->u1.zIndexedBy =
			    sqlite3NameFromToken(pParse->db, pIndexedBy);
			pItem->fg.isIndexedBy = (pItem->u1.zIndexedBy != 0);
		}
	}
}

/*
 * Add the list of function arguments to the SrcList entry for a
 * table-valued-function.
 */
void
sqlite3SrcListFuncArgs(Parse * pParse, SrcList * p, ExprList * pList)
{
	if (p) {
		struct SrcList_item *pItem = &p->a[p->nSrc - 1];
		assert(pItem->fg.notIndexed == 0);
		assert(pItem->fg.isIndexedBy == 0);
		assert(pItem->fg.isTabFunc == 0);
		pItem->u1.pFuncArg = pList;
		pItem->fg.isTabFunc = 1;
	} else {
		sqlite3ExprListDelete(pParse->db, pList);
	}
}

/*
 * When building up a FROM clause in the parser, the join operator
 * is initially attached to the left operand.  But the code generator
 * expects the join operator to be on the right operand.  This routine
 * Shifts all join operators from left to right for an entire FROM
 * clause.
 *
 * Example: Suppose the join is like this:
 *
 *           A natural cross join B
 *
 * The operator is "natural cross join".  The A and B operands are stored
 * in p->a[0] and p->a[1], respectively.  The parser initially stores the
 * operator with A.  This routine shifts that operator over to B.
 */
void
sqlite3SrcListShiftJoinType(SrcList * p)
{
	if (p) {
		int i;
		for (i = p->nSrc - 1; i > 0; i--) {
			p->a[i].fg.jointype = p->a[i - 1].fg.jointype;
		}
		p->a[0].fg.jointype = 0;
	}
}

/*
 * Generate VDBE code for a BEGIN statement.
 */
void
sqlite3BeginTransaction(Parse * pParse, int MAYBE_UNUSED type)
{
	sqlite3 MAYBE_UNUSED *db;
	Vdbe *v;

	assert(pParse != 0);
	db = pParse->db;
	assert(db != 0);
	if (sqlite3AuthCheck(pParse, SQLITE_TRANSACTION, "BEGIN", 0, 0)) {
		return;
	}
	v = sqlite3GetVdbe(pParse);
	if (!v)
		return;
	sqlite3VdbeAddOp0(v, OP_AutoCommit);
}

/*
 * Generate VDBE code for a COMMIT statement.
 */
void
sqlite3CommitTransaction(Parse * pParse)
{
	Vdbe *v;

	assert(pParse != 0);
	assert(pParse->db != 0);
	if (sqlite3AuthCheck(pParse, SQLITE_TRANSACTION, "COMMIT", 0, 0)) {
		return;
	}
	v = sqlite3GetVdbe(pParse);
	if (v) {
		sqlite3VdbeAddOp1(v, OP_AutoCommit, 1);
	}
}

/*
 * Generate VDBE code for a ROLLBACK statement.
 */
void
sqlite3RollbackTransaction(Parse * pParse)
{
	Vdbe *v;

	assert(pParse != 0);
	assert(pParse->db != 0);
	if (sqlite3AuthCheck(pParse, SQLITE_TRANSACTION, "ROLLBACK", 0, 0)) {
		return;
	}
	v = sqlite3GetVdbe(pParse);
	if (v) {
		sqlite3VdbeAddOp2(v, OP_AutoCommit, 1, 1);
	}
}

/*
 * This function is called by the parser when it parses a command to create,
 * release or rollback an SQL savepoint.
 */
void
sqlite3Savepoint(Parse * pParse, int op, Token * pName)
{
	char *zName = sqlite3NameFromToken(pParse->db, pName);
	if (zName) {
		Vdbe *v = sqlite3GetVdbe(pParse);
		static const char *const az[] =
		    { "BEGIN", "RELEASE", "ROLLBACK" };
#ifndef SQLITE_OMIT_AUTHORIZATION
		assert(!SAVEPOINT_BEGIN && SAVEPOINT_RELEASE == 1
		       && SAVEPOINT_ROLLBACK == 2);
#endif
		if (!v
		    || sqlite3AuthCheck(pParse, SQLITE_SAVEPOINT, az[op], zName,
					0)) {
			sqlite3DbFree(pParse->db, zName);
			return;
		}
		if (op == SAVEPOINT_BEGIN &&
			sqlite3CheckIdentifierName(pParse, zName)
				!= SQLITE_OK) {
			sqlite3ErrorMsg(pParse, "bad savepoint name");
			return;
		}
		sqlite3VdbeAddOp4(v, OP_Savepoint, op, 0, 0, zName, P4_DYNAMIC);
	}
}

/*
 * Record the fact that the schema cookie will need to be verified
 * for database.  The code to actually verify the schema cookie
 * will occur at the end of the top-level VDBE and will be generated
 * later, by sqlite3FinishCoding().
 */
void
sqlite3CodeVerifySchema(Parse * pParse)
{
	Parse *pToplevel = sqlite3ParseToplevel(pParse);

	if (DbMaskTest(pToplevel->cookieMask, 0) == 0) {
		DbMaskSet(pToplevel->cookieMask, 0);
	}
}

/*
 * Generate VDBE code that prepares for doing an operation that
 * might change the database.
 *
 * This routine starts a new transaction if we are not already within
 * a transaction.  If we are already within a transaction, then a checkpoint
 * is set if the setStatement parameter is true.  A checkpoint should
 * be set for operations that might fail (due to a constraint) part of
 * the way through and which will need to undo some writes without having to
 * rollback the whole transaction.  For operations where all constraints
 * can be checked before any changes are made to the database, it is never
 * necessary to undo a write and the checkpoint should not be set.
 */
void
sqlite3BeginWriteOperation(Parse * pParse, int setStatement)
{
	Parse *pToplevel = sqlite3ParseToplevel(pParse);
	sqlite3CodeVerifySchema(pParse);
	DbMaskSet(pToplevel->writeMask, 0);
	pToplevel->isMultiWrite |= setStatement;
}

/*
 * Indicate that the statement currently under construction might write
 * more than one entry (example: deleting one row then inserting another,
 * inserting multiple rows in a table, or inserting a row and index entries.)
 * If an abort occurs after some of these writes have completed, then it will
 * be necessary to undo the completed writes.
 */
void
sqlite3MultiWrite(Parse * pParse)
{
	Parse *pToplevel = sqlite3ParseToplevel(pParse);
	pToplevel->isMultiWrite = 1;
}

/*
 * The code generator calls this routine if is discovers that it is
 * possible to abort a statement prior to completion.  In order to
 * perform this abort without corrupting the database, we need to make
 * sure that the statement is protected by a statement transaction.
 *
 * Technically, we only need to set the mayAbort flag if the
 * isMultiWrite flag was previously set.  There is a time dependency
 * such that the abort must occur after the multiwrite.  This makes
 * some statements involving the REPLACE conflict resolution algorithm
 * go a little faster.  But taking advantage of this time dependency
 * makes it more difficult to prove that the code is correct (in
 * particular, it prevents us from writing an effective
 * implementation of sqlite3AssertMayAbort()) and so we have chosen
 * to take the safe route and skip the optimization.
 */
void
sqlite3MayAbort(Parse * pParse)
{
	Parse *pToplevel = sqlite3ParseToplevel(pParse);
	pToplevel->mayAbort = 1;
}

/*
 * Code an OP_Halt that causes the vdbe to return an SQLITE_CONSTRAINT
 * error. The onError parameter determines which (if any) of the statement
 * and/or current transaction is rolled back.
 */
void
sqlite3HaltConstraint(Parse * pParse,	/* Parsing context */
		      int errCode,	/* extended error code */
		      int onError,	/* Constraint type */
		      char *p4,	/* Error message */
		      i8 p4type,	/* P4_STATIC or P4_TRANSIENT */
		      u8 p5Errmsg	/* P5_ErrMsg type */
    )
{
	Vdbe *v = sqlite3GetVdbe(pParse);
	assert((errCode & 0xff) == SQLITE_CONSTRAINT);
	if (onError == ON_CONFLICT_ACTION_ABORT) {
		sqlite3MayAbort(pParse);
	}
	sqlite3VdbeAddOp4(v, OP_Halt, errCode, onError, 0, p4, p4type);
	sqlite3VdbeChangeP5(v, p5Errmsg);
}

/*
 * Code an OP_Halt due to UNIQUE or PRIMARY KEY constraint violation.
 */
void
sqlite3UniqueConstraint(Parse * pParse,	/* Parsing context */
			int onError,	/* Constraint type */
			Index * pIdx	/* The index that triggers the constraint */
    )
{
	char *zErr;
	int j;
	StrAccum errMsg;
	Table *pTab = pIdx->pTable;

	sqlite3StrAccumInit(&errMsg, pParse->db, 0, 0, 200);
	if (pIdx->aColExpr) {
		sqlite3XPrintf(&errMsg, "index '%q'", pIdx->zName);
	} else {
		for (j = 0; j < pIdx->nKeyCol; j++) {
			char *zCol;
			assert(pIdx->aiColumn[j] >= 0);
			zCol = pTab->aCol[pIdx->aiColumn[j]].zName;
			if (j)
				sqlite3StrAccumAppend(&errMsg, ", ", 2);
			sqlite3XPrintf(&errMsg, "%s.%s", pTab->zName, zCol);
		}
	}
	zErr = sqlite3StrAccumFinish(&errMsg);
	sqlite3HaltConstraint(pParse,
			      IsPrimaryKeyIndex(pIdx) ?
			      SQLITE_CONSTRAINT_PRIMARYKEY :
			      SQLITE_CONSTRAINT_UNIQUE, onError, zErr,
			      P4_DYNAMIC, P5_ConstraintUnique);
}

/*
 * Check to see if pIndex uses the collating sequence pColl.  Return
 * true if it does and false if it does not.
 */
#ifndef SQLITE_OMIT_REINDEX
static int
collationMatch(const char *zColl, Index * pIndex)
{
	int i;
	assert(zColl != 0);
	for (i = 0; i < pIndex->nColumn; i++) {
		const char *z = index_collation_name(pIndex, i);
		assert(z != 0 || pIndex->aiColumn[i] < 0);
		if (pIndex->aiColumn[i] >= 0 && 0 == sqlite3StrICmp(z, zColl)) {
			return 1;
		}
	}
	return 0;
}
#endif

/*
 * Recompute all indices of pTab that use the collating sequence pColl.
 * If pColl==0 then recompute all indices of pTab.
 */
#ifndef SQLITE_OMIT_REINDEX
static void
reindexTable(Parse * pParse, Table * pTab, char const *zColl)
{
	Index *pIndex;		/* An index associated with pTab */

	for (pIndex = pTab->pIndex; pIndex; pIndex = pIndex->pNext) {
		if (zColl == 0 || collationMatch(zColl, pIndex)) {
			sqlite3BeginWriteOperation(pParse, 0);
			sqlite3RefillIndex(pParse, pIndex, -1);
		}
	}
}
#endif

/*
 * Recompute all indices of all tables in all databases where the
 * indices use the collating sequence pColl.  If pColl==0 then recompute
 * all indices everywhere.
 */
#ifndef SQLITE_OMIT_REINDEX
static void
reindexDatabases(Parse * pParse, char const *zColl)
{
	Db *pDb;		/* A single database */
	sqlite3 *db = pParse->db;	/* The database connection */
	HashElem *k;		/* For looping over tables in pDb */
	Table *pTab;		/* A table in the database */

	pDb = &db->mdb;
	assert(pDb != 0);
	for (k = sqliteHashFirst(&pDb->pSchema->tblHash); k;
	     k = sqliteHashNext(k)) {
		pTab = (Table *) sqliteHashData(k);
		reindexTable(pParse, pTab, zColl);
	}
}
#endif

/*
 * Generate code for the REINDEX command.
 *
 *        REINDEX                             -- 1
 *        REINDEX  <collation>                -- 2
 *        REINDEX  <tablename>                -- 3
 *        REINDEX  <indexname> ON <tablename> -- 4
 *
 * Form 1 causes all indices in all attached databases to be rebuilt.
 * Form 2 rebuilds all indices in all databases that use the named
 * collating function.  Forms 3 and 4 rebuild the named index or all
 * indices associated with the named table.
 */
#ifndef SQLITE_OMIT_REINDEX
void
sqlite3Reindex(Parse * pParse, Token * pName1, Token * pName2)
{
	struct coll *pColl;		/* Collating sequence to be reindexed, or NULL */
	char *z = 0;		/* Name of index */
	char *zTable = 0;	/* Name of indexed table */
	Table *pTab;		/* A table in the database */
	Index *pIndex;		/* An index associated with pTab */
	sqlite3 *db = pParse->db;	/* The database connection */

	/* Read the database schema. If an error occurs, leave an error message
	 * and code in pParse and return NULL.
	 */
	if (SQLITE_OK != sqlite3ReadSchema(pParse)) {
		return;
	}

	if (pName1 == 0) {
		reindexDatabases(pParse, 0);
		return;
	} else if (NEVER(pName2 == 0) || pName2->z == 0) {
		char *zColl;
		assert(pName1->z);
		zColl = sqlite3NameFromToken(pParse->db, pName1);
		if (!zColl)
			return;
		pColl = sqlite3FindCollSeq(zColl);
		if (pColl) {
			reindexDatabases(pParse, zColl);
			sqlite3DbFree(db, zColl);
			return;
		}
		sqlite3DbFree(db, zColl);
	}
	z = sqlite3NameFromToken(db, pName1);
	if (z == 0)
		return;
	pTab = sqlite3FindTable(db, z);
	if (pTab) {
		reindexTable(pParse, pTab, 0);
		sqlite3DbFree(db, z);
		return;
	}
	if (pName2->n > 0) {
		zTable = sqlite3NameFromToken(db, pName2);
	}

	pTab = sqlite3FindTable(db, zTable);
	if (pTab == 0) {
		sqlite3ErrorMsg(pParse, "no such table: %s", zTable);
		goto exit_reindex;
	}

	pIndex = sqlite3FindIndex(db, z, pTab);

	if (pIndex) {
		sqlite3BeginWriteOperation(pParse, 0);
		sqlite3RefillIndex(pParse, pIndex, -1);
		return;
	}

	sqlite3ErrorMsg(pParse,
			"unable to identify the object to be reindexed");

 exit_reindex:
	sqlite3DbFree(db, z);
	sqlite3DbFree(db, zTable);
}
#endif

/*
 * Return a KeyInfo structure that is appropriate for the given Index.
 *
 * The caller should invoke sqlite3KeyInfoUnref() on the returned object
 * when it has finished using it.
 */
KeyInfo *
sqlite3KeyInfoOfIndex(Parse * pParse, sqlite3 * db, Index * pIdx)
{
	int i;
	int nCol = pIdx->nColumn;
	int nTableCol = pIdx->pTable->nCol;
	int nKey = pIdx->nKeyCol;
	KeyInfo *pKey;

	if (pParse && pParse->nErr)
		return 0;

	/*
	 * KeyInfo describes the index (i.e. the number of key columns,
	 * comparator options, and the number of columns beyond the key).
	 * Since Tarantool iterator yields the full tuple, we need a KeyInfo
	 * as wide as the table itself.  Otherwize, not enough slots
	 * for row parser cache are allocated in VdbeCursor object.
	 */
	if (pIdx->uniqNotNull) {
		pKey = sqlite3KeyInfoAlloc(db, nKey, nTableCol - nKey);
	} else {
		pKey = sqlite3KeyInfoAlloc(db, nCol, nTableCol - nCol);
	}
	if (pKey) {
		assert(sqlite3KeyInfoIsWriteable(pKey));
		for (i = 0; i < nCol; i++) {
			const char *zColl = index_collation_name(pIdx, i);
			pKey->aColl[i] = zColl == sqlite3StrBINARY ? 0 :
			    sqlite3LocateCollSeq(pParse, db, zColl);
			pKey->aSortOrder[i] = pIdx->aSortOrder[i];
		}
		if (pParse && pParse->nErr) {
			sqlite3KeyInfoUnref(pKey);
			pKey = 0;
		}
	}
	return pKey;
}

#ifndef SQLITE_OMIT_CTE
/*
 * This routine is invoked once per CTE by the parser while parsing a
 * WITH clause.
 */
With *
sqlite3WithAdd(Parse * pParse,	/* Parsing context */
	       With * pWith,	/* Existing WITH clause, or NULL */
	       Token * pName,	/* Name of the common-table */
	       ExprList * pArglist,	/* Optional column name list for the table */
	       Select * pQuery	/* Query used to initialize the table */
    )
{
	sqlite3 *db = pParse->db;
	With *pNew;
	char *zName;

	/* Check that the CTE name is unique within this WITH clause. If
	 * not, store an error in the Parse structure.
	 */
	zName = sqlite3NameFromToken(pParse->db, pName);
	if (zName && pWith) {
		int i;
		for (i = 0; i < pWith->nCte; i++) {
			if (strcmp(zName, pWith->a[i].zName) == 0) {
				sqlite3ErrorMsg(pParse,
						"duplicate WITH table name: %s",
						zName);
			}
		}
	}

	if (pWith) {
		int nByte =
		    sizeof(*pWith) + (sizeof(pWith->a[1]) * pWith->nCte);
		pNew = sqlite3DbRealloc(db, pWith, nByte);
	} else {
		pNew = sqlite3DbMallocZero(db, sizeof(*pWith));
	}
	assert((pNew != 0 && zName != 0) || db->mallocFailed);

	if (db->mallocFailed) {
		sqlite3ExprListDelete(db, pArglist);
		sqlite3SelectDelete(db, pQuery);
		sqlite3DbFree(db, zName);
		pNew = pWith;
	} else {
		pNew->a[pNew->nCte].pSelect = pQuery;
		pNew->a[pNew->nCte].pCols = pArglist;
		pNew->a[pNew->nCte].zName = zName;
		pNew->a[pNew->nCte].zCteErr = 0;
		pNew->nCte++;
	}

	return pNew;
}

/*
 * Free the contents of the With object passed as the second argument.
 */
void
sqlite3WithDelete(sqlite3 * db, With * pWith)
{
	if (pWith) {
		int i;
		for (i = 0; i < pWith->nCte; i++) {
			struct Cte *pCte = &pWith->a[i];
			sqlite3ExprListDelete(db, pCte->pCols);
			sqlite3SelectDelete(db, pCte->pSelect);
			sqlite3DbFree(db, pCte->zName);
		}
		sqlite3DbFree(db, pWith);
	}
}
#endif				/* !defined(SQLITE_OMIT_CTE) */

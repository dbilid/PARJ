#ifndef SQLITE_OMIT_VIRTUALTABLE
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#define _GNU_SOURCE
#include <inttypes.h>
#include <raptor2/raptor2.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include <pthread.h>
//#include <gperftools/tcmalloc.h>
#include <unistd.h>
#include <sched.h>
#include <stdint.h>
#include <glib.h>
#include <errno.h>
//#include <nmmintrin.h>
//#include <papi.h>
#define BILLION 1E9
#define MAXPROPERTIES 1024
//#define WINDOW 100
#define USEBITMAP 1
#define LOADDICTIONARY 0
#define STATLIMIT 500E8


//#define SetBit(A,k)     ( A[(k/64)] |= (1 << (k%64)) )
//#define ClearBit(A,k)   ( A[(k/64)] &= ~(1 << (k%64)) )            
//#define TestBit(A,k)    ( A[(k/64)] & (1 << (k%64)) )
//PAPI

/*
 //float real_time, proc_time, mflops;
 // long long flpins;
 int retval;
 long binaryCounter=0L;
 long long l3_access=0L;
 long long l3_misses=0L;
 long long l2_access=0L;
 long long l2_misses=0L;
 long long tlb_data_misses=0L;
 int numEvents = 4;
 long long values[4];
 int events[4] = {PAPI_L3_TCA,PAPI_L3_TCM, PAPI_L2_TCA,PAPI_L2_TCM};
 int papiCounter=0;
*/


/*typedef struct subjectVector {
	uint32_t start;
	uint32_t end;
} subjectVector_t;
*/
struct tableInfo {
	int partitions, inverse;
	long propNo;
	const char* dbdir;

};




static uint16_t wordbits[65536];
int popcount32e_init(void)
{
printf("start filling wordbits...\n");
    uint32_t i;
    uint16_t x;
    int count;
    for (i=0; i <= 0xFFFF; i++)
    {
        x = i;
        for (count=0; x; count++) 
            x &= x - 1;
        wordbits[i] = count;
    }
printf("finished filling wordbits...\n");
}
static inline int popcount32e(const uint32_t x)
{
    return wordbits[x & 0xFFFF] + wordbits[x >> 16];
}

static const char ** dictionary;
//static const struct subjectVector ***vectorGlobal;
static int * __restrict__ noOfSubjectsGlobal;// __attribute__((aligned(0x1000000)));
static int * __restrict__ windowSizeGlobal;// __attribute__((aligned(0x1000000)));
static const uint32_t ** __restrict__ subjectsGlobal;
static const uint32_t ** __restrict__ objectsGlobal;
static const uint32_t ** __restrict__ sizesGlobal;
//static uint32_t *summariesGlobal; // __attribute__((aligned(0x100000)));
static uint32_t maxId=0;
//static unsigned long binaryCounter1=0L;
//static unsigned long scanCounter=0L;
static const uint32_t ** __restrict__ bitmapsGlobal;
//static uint32_t **bitmapsValuesGlobal;
pthread_mutex_t mutexload;

int loadMemoryData(sqlite3 *, int, int);
int stick_this_thread_to_core(int);
static inline  int summaryBinarySearchSubject(
		int, const uint32_t, const int, int*);
static inline  int scanSubject(
                int, uint32_t, int*);
static inline  int bitmapSearch(
                int, uint32_t, int*);
static inline  int binarySearchSubject(
                int, uint32_t, int, int*);
static inline int scanObject(uint32_t, const uint32_t*, const int, int*);
static uint64_t getTypeCardinality(int, int, int);

static inline void  SetBit( uint32_t A[],  int k )
   {
	A[k/32] |= 1 << (k%32);
	//if(k<64){
	//	printf("k:%i shift: %lld  A[0]: %lld \n", k, 1LL << (k%64), A[0]);
	//}
     // A[k/64] |= 1LL << (k%64);  // Set the bit at the k-th position in A[i]
	
   }



static inline void  ClearBit( uint32_t A[],  int k )                
   {
      A[k/32] &= ~(1 << (k%32));
   }

static inline int TestBit(const int position,  int k )
   {
	//printf("i:%" PRIu64 " k: %i \n", A[k/64], k);
      return ( (bitmapsGlobal[position][k/32] & (1 << (k%32) )) != 0 ) ;     
   }



struct timeval oldtp = { 0 };

long getTime(void)
{
        struct timeval tp;
        gettimeofday(&tp, 0);
        if (oldtp.tv_sec == 0 && oldtp.tv_usec == 0) {
                oldtp = tp;
        }
        return (long)( (long)(tp.tv_sec  - oldtp.tv_sec ) * (long)1000000 + 
                        (long)(tp.tv_usec - oldtp.tv_usec)       );
}


int propId;
uint32_t id;

GHashTable * hashtable;
GHashTable * prophash;
sqlite3_stmt *inserts[MAXPROPERTIES];
static const char* insertPrefix = "insert or ignore into ext.prop";
static const char* insertPostfix = " values(?, ?) ;";
static const char* createPrefix = "create table ext.prop";
static const char* createPostfix =
		" (s INTEGER, o INTEGER, primary key(s, o)) without rowid;";
sqlite3 *dbExt;

void addBatch(char* key, int value, sqlite3_stmt *res) {
	//printf("key:%s value:%d \n", key, value);
	sqlite3_bind_int(res, 1, value);
	sqlite3_bind_text(res, 2, key, -1, SQLITE_STATIC);
	if (sqlite3_step(res) != SQLITE_DONE) {
		printf("Commit Failed!\n");
	}
	sqlite3_reset(res);
}

void saveDictionaryToDisk(GHashTable *hash_table, sqlite3 *db, int dictionary) {
	printf("saving dictionary \n");
	char *sql = malloc(1000);
	if (dictionary) {
		strcpy(sql,
				"create table ext.dictionary(id INTEGER PRIMARY KEY, uri TEXT)");
	} else {
		strcpy(sql,
				"create table ext.properties(id INTEGER PRIMARY KEY, uri TEXT)");
	}
	int rc;
	rc = sqlite3_exec(db, sql, 0, 0, NULL);
	if (rc != SQLITE_OK) {

		fprintf(stderr, "could not create dictionary : %s\n",
		sqlite3_errmsg(db));
		return;
	}
	if (dictionary) {
		strcpy(sql, "insert into ext.dictionary values(?, ?)");
	} else {
		strcpy(sql, "insert into ext.properties values(?, ?)");
	}

	sqlite3_stmt *res;
	rc = sqlite3_prepare_v2(db, sql, -1, &res, 0);
	if (rc != SQLITE_OK) {

		fprintf(stderr, "Cannot prepare statement: %s\n", sqlite3_errmsg(db));
		return;
	}

	GHashTableIter iter;
	gpointer key, value;

	g_hash_table_iter_init(&iter, hash_table);

	while (g_hash_table_iter_next(&iter, &key, &value)) {
		//printf("key:%p values:%p \n", key, value);
		char* k = (char*) key;
		int v = GPOINTER_TO_INT(value);
		addBatch(k, v, res);
	}

	rc = sqlite3_finalize(res);
	free(sql);

}

/*
 void addBatch(char* key, int* value, sqlite3_stmt *res) {
 sqlite3_bind_int(res, 1, value);
 sqlite3_bind_text(res, 2, key, -1, SQLITE_STATIC);
 if (sqlite3_step(res) != SQLITE_DONE) {
 printf("Commit Failed!\n");
 }
 sqlite3_reset(res);
 }
 */

static void finalizeInserts() {
	int p;
	int rc;
	for (p = 0; p < propId + 1; p++) {
		rc = sqlite3_finalize(inserts[p]);
		if (rc != SQLITE_OK) {

			fprintf(stderr,
					"Could not finalize dictionary/properties statement %s",
					sqlite3_errmsg(dbExt));
			//sqlite3_close(dbExt);

			return;
		}
	}
}

static void createInverseTables() {
	int pId;
	int rc;

	char create[100];
	char insert[100];
	for (pId = 0; pId < propId; pId++) {
		snprintf(create, sizeof create,
				"create table ext.invprop%i (o INTEGER, s INTEGER, primary key(o, s)) without rowid;",
				pId);
		rc = sqlite3_exec(dbExt, create, 0, 0, NULL);

		if (rc != SQLITE_OK) {

			fprintf(stderr, "Could not create inv property table: %s\n",
			sqlite3_errmsg(dbExt));
			//sqlite3_close(dbExt);

			return;
		}

		snprintf(insert, sizeof insert,
				"insert into invprop%i select o, s from prop%i; ", pId, pId);
		rc = sqlite3_exec(dbExt, insert, 0, 0, NULL);

		if (rc != SQLITE_OK) {

			fprintf(stderr, "Could not insert into inv property table: %s\n",
			sqlite3_errmsg(dbExt));
			//sqlite3_close(dbExt);

			return;
		}
	}
	rc = sqlite3_exec(dbExt,
			"CREATE UNIQUE INDEX ext.uriindex on dictionary(uri);", 0, 0, NULL);

	if (rc != SQLITE_OK) {

		fprintf(stderr, "Could not create  index on dictionary %s",
		sqlite3_errmsg(dbExt));
		//sqlite3_close(dbExt);

		return;
	}
	rc = sqlite3_exec(dbExt,
			"CREATE UNIQUE INDEX ext.uriindex2 on properties(uri);", 0, 0,
			NULL);

	if (rc != SQLITE_OK) {

		fprintf(stderr, "Could not create  index on properties %s",
		sqlite3_errmsg(dbExt));
		//sqlite3_close(dbExt);

		return;
	}
}

static char* getUri(raptor_term* term) {
	//size_t len;
	if (term->type == RAPTOR_TERM_TYPE_BLANK) {
		printf("error, blank nodes currently not supported");
		return NULL;
	} else if (term->type == RAPTOR_TERM_TYPE_UNKNOWN) {
		printf("error, unknown term");
		return NULL;
	}

	else if (term->type == RAPTOR_TERM_TYPE_URI) {
		unsigned char* uri = NULL;

		uri = raptor_uri_as_string(term->value.uri);
		if (!uri)
			printf("not uri! %s\n", raptor_term_to_string(term));
		char* result = malloc(strlen(uri) + 1);
		//strcpy(result, "<");
		strcpy(result, uri);
		//strcat(result, ">");
		return result;
	} else if (term->type == RAPTOR_TERM_TYPE_LITERAL) {
		char* datatype = NULL;
		uint32_t length = term->value.literal.string_len + 1;
		if (term->value.literal.language) {
			length += (term->value.literal.language_len);
		}
		if (term->value.literal.datatype) {
			datatype = raptor_uri_as_string(term->value.literal.datatype);
			length += strlen(datatype) + 2;
		}
		char* uri = malloc(length);
		//strcpy(uri, """);
		strcpy(uri, term->value.literal.string);
		//strcat(uri, """);
		if (term->value.literal.language) {
			strcat(uri, "@");
			strcat(uri, term->value.literal.language);

		}
		if (term->value.literal.datatype) {

			strcat(uri, "^^");

			strcat(uri, datatype);

		}

		return uri;
	}
	return NULL;
}

uint64_t getId(char* uri) {
	uint64_t result;
	gpointer res;

	res = g_hash_table_lookup(hashtable, uri);

	if (!res) {

		result = id;
		g_hash_table_insert(hashtable, uri, GUINT_TO_POINTER(id++));
	} else {
		result = GPOINTER_TO_UINT(res);
		free(uri);
	}
	return result;
}

static void createNextPropertyTable() {

	char create[100];
	snprintf(create, sizeof create, "%s%i%s", createPrefix, propId,
			createPostfix);
	int rc;
	rc = sqlite3_exec(dbExt, create, 0, 0, NULL);

	if (rc != SQLITE_OK) {

		fprintf(stderr, "Could not create property table: %s\n",
		sqlite3_errmsg(dbExt));
		//sqlite3_close(dbExt);

		return;
	}
	char insert[100];
	snprintf(insert, sizeof insert, "%s%i%s", insertPrefix, propId,
			insertPostfix);

	rc = sqlite3_prepare_v2(dbExt, insert, -1, &inserts[propId], 0);
	if (rc != SQLITE_OK) {

		fprintf(stderr, "Cannot prepare statement: %s\n",
		sqlite3_errmsg(dbExt));
		//sqlite3_close(dbExt);

		return;
	}
}

int getPropId(char* uri) {
	int result;
	gpointer res;
	res = g_hash_table_lookup(prophash, uri);

	if (!res) {
		result = propId;
		createNextPropertyTable();
		printf("next prop!\n");
		char *toCopy = malloc(strlen(uri) + 1);
		strcpy(toCopy, uri);
		g_hash_table_insert(prophash, toCopy, GINT_TO_POINTER(propId++));
	} else {
		result = GPOINTER_TO_INT(res);
	}

	return result;
}

static void import_triples(void* user_data, raptor_statement* triple) {

	unsigned int* count_p = (unsigned int*) user_data;
	raptor_term *term = triple->subject;
	char* uri = getUri(term);
	uint64_t subject;
	uint64_t object;
	int propNo;

	if (uri) {
		subject = getId(uri);
	}

	raptor_term *term2 = triple->object;
	char* uri2 = getUri(term2);
	if (uri2) {
		object = getId(uri2);

	}

	char* uri3 = raptor_uri_as_string(triple->predicate->value.uri);
	if (!uri3)
		printf("not uri3 %s", raptor_term_to_string(term));

	propNo = getPropId(uri3);

	sqlite3_bind_int64(inserts[propNo], 1, subject);
	sqlite3_bind_int64(inserts[propNo], 2, object);
	int rc = sqlite3_step(inserts[propNo]);
	if (rc != SQLITE_DONE) {
		printf("Commit Failed! error:%i\n", rc);
	}
	sqlite3_reset(inserts[propNo]);
	//triple->usage=1;
	//(*count_p)++;
}


#ifndef SQLITE_OMIT_VIRTUALTABLE
/**************** start stat.c ***********/

typedef struct stat_vtab stat_vtab;
struct stat_vtab {
	sqlite3_vtab base; /* Base class - must be first */
	int maxPropID;
	int typePropID;
	
};

/* A stat cursor object */
typedef struct stat_cursor stat_cursor;
struct stat_cursor {
	//sqlite3_vtab_cursor base; /* Base class - must be first */
	stat_vtab* pVtab;
	int eof;
    uint64_t value;
    int counter;
    int size;
    uint64_t* result;


};

//float real_time, proc_time, mflops;
//  long long flpins;
//  int retval;

static int statConnect(sqlite3 *db, void *pAux, int argc,
		const char * const *argv, sqlite3_vtab **ppVtab, char **pzErr) {


	sqlite3_vtab *pNew ;
	pNew = *ppVtab = sqlite3_malloc(sizeof(*pNew));
	if (pNew == 0)
		return SQLITE_NOMEM;
	sqlite3_declare_vtab(db,
			"CREATE TABLE x(result INTEGER, mode INTEGER, option1 INTEGER, option2 INTEGER, option3 INTEGER)");
	int maxPropID;
	sscanf(argv[3], "%d", &maxPropID);
	printf("maxPropId:%i\n", maxPropID);

	int typePropID;
        sscanf(argv[4], "%d", &typePropID);
        printf("maxPropId:%i\n", typePropID);

	memset(pNew, 0, sizeof(*pNew));
	stat_vtab *pVt = 0;
	pVt = sqlite3_malloc(sizeof(*pVt));
	if (pVt == 0)
		return SQLITE_NOMEM;
	memset(pVt, 0, sizeof(*pVt));
	//pVt->db = db;
	*ppVtab = &pVt->base;
	pVt->maxPropID=maxPropID;
	pVt->typePropID=typePropID;

	return SQLITE_OK;
}

static int statDisconnect(sqlite3_vtab *pVtab) {
	stat_vtab *pVt = (stat_vtab*) pVtab;
	
	sqlite3_free(pVt);
	return SQLITE_OK;
}

static int statOpen(sqlite3_vtab *pVTab,
		sqlite3_vtab_cursor **ppCursor) {

	stat_vtab *pVt = (stat_vtab*) pVTab;
	stat_cursor *pCur;
	

	pCur = sqlite3_malloc(sizeof(*pCur));
	if (pCur == 0)
		return SQLITE_NOMEM;
	memset(pCur, 0, sizeof(*pCur));
	pCur->pVtab = pVt;
	// *ppCursor = &pCur->base;
	*ppCursor = (sqlite3_vtab_cursor*) &pCur->pVtab;
	//pCur->count = 0;

	return SQLITE_OK;
}

static int statClose(sqlite3_vtab_cursor *cur) {
	
	stat_cursor *pCur = (stat_cursor*) cur;
	sqlite3_free(pCur->result);
	sqlite3_free(cur);
	return SQLITE_OK;
}

static int statNext(sqlite3_vtab_cursor *cur) {
        stat_cursor *pCur = (stat_cursor*) cur;
	//printf("counter: %i\n", pCur->counter);
        if(pCur->counter<pCur->size){
            pCur->value=pCur->result[pCur->counter++];
        }
        else{
            pCur->eof=1;
        }
        
        
	    return SQLITE_OK;
}

static int statColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx,
		int i) {
	//printf("next %i  \n", pCur->value);
	stat_cursor *pCur = (stat_cursor*) cur;
	//printf("next %i  \n", pCur->value);
	if (i == 0)
		sqlite3_result_int64(ctx, pCur->value);
	else
		sqlite3_result_int(ctx, -1);

	
	return SQLITE_OK;
}

static int statRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid) {
	stat_cursor *pCur = (stat_cursor*) cur;
	*pRowid = pCur->counter;
	return SQLITE_OK;
}

static int statEof(sqlite3_vtab_cursor *cur) {
	stat_cursor *pCur = (stat_cursor*) cur;
	return pCur->eof;
}



static int statFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
		const char *idxStr, int argc, sqlite3_value **argv) {

	stat_cursor *pCur = (stat_cursor *) pVtabCursor;
	stat_vtab *pVtab = (stat_vtab *) pCur->pVtab;
	int mode = sqlite3_value_int(argv[0]);
	pCur->eof=0;
	pCur->counter=1;
	//0->tablestats, 1->typestats, 2->estimate
	if(mode==0){
		int size=pVtab->maxPropID * 6 *2;
		pCur->result= sqlite3_malloc(sizeof(uint64_t) * size);
		pCur->size=size;
		int j;
		pCur->value=0;
		for(j=0;j<pVtab->maxPropID;j++){
			int index=j*2;
			int i=j*12;
			printf("i: %i\n", i);
			pCur->result[i]=j;
			pCur->result[i+1]=0;
			pCur->result[i+2]=subjectsGlobal[index][0]; //min
			pCur->result[i+3]=subjectsGlobal[index][noOfSubjectsGlobal[index]-1]; //max
			pCur->result[i+4]=noOfSubjectsGlobal[index]; //distinct
			pCur->result[i+5]=sizesGlobal[index][noOfSubjectsGlobal[index]-1]; //all
			index++; //inverse
			pCur->result[i+6]=j;
			pCur->result[i+7]=1;
			pCur->result[i+8]=subjectsGlobal[index][0]; //min
			pCur->result[i+9]=subjectsGlobal[index][noOfSubjectsGlobal[index]-1]; //max
			pCur->result[i+10]=noOfSubjectsGlobal[index]; //distinct
			pCur->result[i+11]=sizesGlobal[index][noOfSubjectsGlobal[index]-1]; //all
			
		}		
		
	}
	else if(mode==1){
		int index = (sqlite3_value_int(argv[1])*2)+1;
		//printf("index: %i  \n", index);
		pCur->result= sqlite3_malloc(sizeof(uint64_t) * noOfSubjectsGlobal[index] * 4);
		pCur->size=noOfSubjectsGlobal[index] * 4;
		//printf("noOfSubj: %i \n", noOfSubjectsGlobal[index]);
		int previous=0;
		pCur->counter=1;
		pCur->value=subjectsGlobal[index][0];
		int i;
		for(i=0;i<noOfSubjectsGlobal[index];i++){
			int j=i*4;
			pCur->result[j]=subjectsGlobal[index][i];
			//printf("next o: %i \n", pCur->result[j]);
			if(i==0){
				pCur->result[j+1]=objectsGlobal[index][0];
			}else{
				pCur->result[j+1]=objectsGlobal[index][sizesGlobal[index][i-1]];
			}
			pCur->result[j+2]=objectsGlobal[index][sizesGlobal[index][i]-1];
			pCur->result[j+3]=sizesGlobal[index][i]-previous;
			//printf("value: %i \n", objectsGlobal[index][sizesGlobal[index][i]-1]);
			//printf("min: %i, max: %i,diff: %i \n", pCur->result[j+1], pCur->result[j+2], pCur->result[j+3]); 
			previous=sizesGlobal[index][i];
		}
		
	}
	else if(mode==2){
		//printf("0 \n");
		int joinType=sqlite3_value_int(argv[1]);
		int prop1=sqlite3_value_int(argv[2]);
		int prop2=sqlite3_value_int(argv[3]);
		//printf("1\n");
		pCur->size=1;
		pCur->result= sqlite3_malloc(sizeof(uint64_t));
                pCur->result[0]=0;
		if(prop1<0 && prop2<0){
                        //SS Join only
			
			int typeIndex=(2*pVtab->typePropID)+1;
			int start=0;
			int start2=0;
			int f1=summaryBinarySearchSubject(typeIndex, -prop1, 0, &start);
			int f2=summaryBinarySearchSubject(typeIndex, -prop2, 0, &start2);
			if(f1 && f2){
				int startPos=0;
                                if(start>0){
	                                startPos=sizesGlobal[typeIndex][start-1];
                                }
                                int endPos=sizesGlobal[typeIndex][start];
				int startPos2=0;
                                if(start2>0){
                                        startPos2=sizesGlobal[typeIndex][start2-1];
                                }
                                int endPos2=sizesGlobal[typeIndex][start2];
				int k=startPos;
				int j=startPos2;
				while(k<endPos && j<endPos2 && pCur->result[0]<STATLIMIT){
					if(objectsGlobal[typeIndex][k]==objectsGlobal[typeIndex][j]){
						pCur->result[0]++;
						k++;
						j++;
					}
					else if (objectsGlobal[typeIndex][k]<objectsGlobal[typeIndex][j]){
						k++;
					}
					else{
						j++;
					}
				}
			}
			pCur->value=pCur->result[0];
			return SQLITE_OK;
                }

		else if(prop1<0){
			int typeIndex=(2*pVtab->typePropID)+1;
			int propindex=0;
			if(joinType==1){
                        	//SO
                        	propindex=(prop2*2)+1;
                	}
                	if(joinType==0){
                        	//SS
                        	propindex=prop2*2;
                	}
			pCur->result[0]=getTypeCardinality(typeIndex, propindex, prop1);
			pCur->value=pCur->result[0];
			return SQLITE_OK;
		}
		else if(prop2<0){
                        int typeIndex=(2*pVtab->typePropID)+1;
                        int propindex=0;
                        if(joinType==1){
                                //SO
                                propindex=(prop1*2)+1;
                        }
                        if(joinType==0){
                                //SS
                                propindex=prop1*2;
                        }
                        pCur->result[0]=getTypeCardinality(typeIndex, propindex, prop2);
			pCur->value=pCur->result[0];
                        return SQLITE_OK;
                }


		int index1=0;
		int index2=0;
		if(joinType==0){
			//SS
			index1=prop1*2;
			index2=prop2*2;
		}
		if(joinType==1){
			//SO
			index1=prop1*2;
			index2=(prop2*2)+1;
		}
		if(joinType==2){
			//OS
			index1=(prop1*2)+1;
			index2=prop2*2;
		}
		if(joinType==3){
			//OO
			index1=(prop1*2)+1;
			index2=(prop2*2)+1;
		}
		//pCur->result= sqlite3_malloc(sizeof(uint64_t));
		//pCur->result[0]=0;
		int step=1;
                //printf("2\n");
		//if(noOfSubjectsGlobal[index1]>10000){
		//	step=noOfSubjectsGlobal[index1]/10000;
		//}
		//printf("step:%i \n", step);
			int counter=1;
			int pos=0;
			int i;
			for(i=0;i<noOfSubjectsGlobal[index1] && pCur->result[0]<STATLIMIT;i+=step){
				//printf("i: %i to find: %i index1 : %i index2 :%i\n", i, subjectsGlobal[index1][i], index1, index2);
				counter++;
				int f=summaryBinarySearchSubject(index2, subjectsGlobal[index1][i], 0, &pos);
				if(f){
					//printf("pos: %i \n", pos);
					int startPos2=0;
					if(pos>0){
						startPos2=sizesGlobal[index2][pos-1];
					}
					int endPos2=sizesGlobal[index2][pos];
					int startPos1=0;
					if(i>0){
						startPos1=sizesGlobal[index1][i-1];
					}
					int endPos1=sizesGlobal[index1][i];
					pCur->result[0]+=(endPos1-startPos1)*(endPos2-startPos2);
				}
			}
			//printf("result:%i \n", pCur->result[0]);
			pCur->value=pCur->result[0]*step;
		
		
	}
	else{
		pCur->eof=1;
	}
        //printf("exit\n");

	return SQLITE_OK;
}

static uint64_t getTypeCardinality(int typeIndex, int propIndex, int type){
	int start=0;
                        int f1=summaryBinarySearchSubject(typeIndex, -type, 0, &start);
                        uint64_t result=0;
			
                        if(f1){
                                int startPos=0;
				int pos=0;
				
                                if(start>0){
                                        startPos=sizesGlobal[typeIndex][start-1];
                                }
                                int endPos=sizesGlobal[typeIndex][start];
                                int k=startPos;
				
                                if(start>0){
                                        startPos=sizesGlobal[typeIndex][start-1];
                                }
				
				int temp=summaryBinarySearchSubject(propIndex, objectsGlobal[typeIndex][k], 0, &pos);
				
                                while(k<endPos && result<STATLIMIT){
                                       	k++;
					int f2=summaryBinarySearchSubject(propIndex, objectsGlobal[typeIndex][k], 0, &pos);
                                	if(f2){
                                        //printf("pos: %i \n", pos);
                                        	int startPos2=0;
                                        	if(pos>0){
                                                	startPos2=sizesGlobal[propIndex][pos-1];
                                        	}
                                        	int endPos2=sizesGlobal[propIndex][pos];
                                                result+=(endPos2-startPos2);
                                }
 
				}
                        }
	return result;
}

static int statBestIndex(sqlite3_vtab *tab,
		sqlite3_index_info *pIdxInfo) {

	int i;
	//binaryCounter1=0;
	//scanCounter=0;
	int col = 0;
	//int inequality = 1;
	pIdxInfo->idxNum = 0;

	pIdxInfo->estimatedCost = 1;
	pIdxInfo->estimatedRows = 1;
	const struct sqlite3_index_constraint *pConstraint;
	pConstraint = pIdxInfo->aConstraint;

	for (i = 0; i < pIdxInfo->nConstraint; i++, pConstraint++) {
		if (pConstraint->usable == 0)
			continue;
		
		
		pIdxInfo->idxNum = SQLITE_INDEX_SCAN_UNIQUE;
		
        pIdxInfo->aConstraintUsage[i].argvIndex = pConstraint->iColumn;
		pIdxInfo->aConstraintUsage[i].omit = 1;
	}
	//0 scan table specific partition
	//1 only s
	//2 both s and o
	//-1 only s specific partition
	//-2 both s and o specific partition
	//pIdxInfo->idxNum = col * inequality;

	return SQLITE_OK;

}

/*
 ** A virtual table module that provides read-only access to a
 ** Tcl global variable namespace.
 */
static sqlite3_module statModule = {  0, /* iVersion */

statConnect, statConnect, statBestIndex,
		statDisconnect, statDisconnect, statOpen, /* xOpen - open a cursor */
		statClose, /* xClose - close a cursor */
		statFilter, /* xFilter - configure scan constraints */
		statNext, /* xNext - advance a cursor */
		statEof, /* xEof - check for end of scan */
		statColumn, /* xColumn - read data */
		statRowid, /* xRowid - read data */
		0, /* xUpdate */
		0, /* xBegin */
		0, /* xSync */
		0, /* xCommit */
		0, /* xRollback */
		0, /* xFindMethod */
		0, /* xRename */
};

#endif /* SQLITE_OMIT_VIRTUALTABLE */

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_stat_init(sqlite3 *db, char **pzErrMsg,
		const sqlite3_api_routines *pApi) {
	int rc = SQLITE_OK;
	SQLITE_EXTENSION_INIT2(pApi);
#ifndef SQLITE_OMIT_VIRTUALTABLE
	rc = sqlite3_create_module(db, "stat", &statModule, 0);
#endif
	return rc;
}

/************** End of stat.c ************************************************/




typedef struct parj_vtab parj_vtab;
struct parj_vtab {
	sqlite3_vtab base; /* Base class - must be first */
	//char* zSql[0];
	sqlite3 *db; /* The database connection */

};

/* A parj cursor object */
typedef struct parj_cursor parj_cursor;
struct parj_cursor {
	sqlite3_vtab_cursor base; /* Base class - must be first */

	parj_vtab* pVtab;

};

static int parjConnect(sqlite3 *db, void *pAux, int argc,
		const char * const *argv, sqlite3_vtab **ppVtab, char **pzErr) {

	sqlite3_vtab *pNew;
	pNew = *ppVtab = sqlite3_malloc(sizeof(*pNew));
	if (pNew == 0)
		return SQLITE_NOMEM;
	sqlite3_declare_vtab(db, "CREATE TABLE x(uri TEXT, id INTEGER)");
	memset(pNew, 0, sizeof(*pNew));

	parj_vtab *pVt = 0;
	pVt = sqlite3_malloc(sizeof(*pVt));
	if (pVt == 0)
		return SQLITE_NOMEM;
	memset(pVt, 0, sizeof(*pVt));
	pVt->db = db;
	//dbExt = db;
	*ppVtab = &pVt->base;

	struct timespec requestStart, requestEnd;

	clock_gettime(CLOCK_MONOTONIC, &requestStart);

	const char *directory;
	directory = argv[3];

	int rc;
	char *sql = malloc(1000);
	rc = sqlite3_open_v2("file::memory:", &dbExt,
	SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI | SQLITE_OPEN_CREATE, NULL);
	if (rc != SQLITE_OK) {

		fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(dbExt));
		//sqlite3_close(db);

		return 1;
	}
	strcpy(sql, "attach database '");
	strcat(sql, directory);
	strcat(sql, "' as ext;");
	//strcpy(sql, "select * from  p0all;");
	rc = sqlite3_exec(dbExt, sql, 0, 0, NULL);

	if (rc != SQLITE_OK) {

		fprintf(stderr, "Cannot attach database: %s\n", sqlite3_errmsg(dbExt));
		//sqlite3_close(db);

		return 1;
	}

	strcpy(sql, "PRAGMA page_size = 4096;");

	sqlite3_exec(dbExt, sql, 0, 0, NULL);

	strcpy(sql, "PRAGMA cache_size = 1048576;");

	sqlite3_exec(dbExt, sql, 0, 0, NULL);

	strcpy(sql, "PRAGMA synchronous = OFF;");

	sqlite3_exec(dbExt, sql, 0, 0, NULL);
	strcpy(sql, "PRAGMA locking_mode = EXCLUSIVE;");

	sqlite3_exec(dbExt, sql, 0, 0, NULL);
	strcpy(sql, "PRAGMA journal_mode = OFF;");

	sqlite3_exec(dbExt, sql, 0, 0, NULL);
	strcpy(sql, "PRAGMA temp_store=2;");
	sqlite3_exec(dbExt, sql, 0, 0, NULL);
	//createMemoryDB(dbt);
	//strcpy(sql, "detach database aa;");
	//sqlite3_exec(dbt, sql, 0, 0, NULL);
	//strcpy(sql, "PRAGMA query_only = 1;");
	sqlite3_exec(dbExt, "BEGIN", 0, 0, 0);

	propId = 1;
	id = 1;

	hashtable = g_hash_table_new(g_str_hash, g_str_equal);
	prophash = g_hash_table_new(g_str_hash, g_str_equal);
	raptor_world *world = NULL;

	raptor_parser* rdf_parser = NULL;
	unsigned int i;
	unsigned int count;
	unsigned int files_count = 0;
	unsigned int total_count = 0;

	world = raptor_new_world();

	/* just one parser is used and reused here */
	rdf_parser = raptor_new_parser(world, "guess");

	raptor_parser_set_statement_handler(rdf_parser, &count, import_triples);

	for (i = 4; i < argc; i++) {
		const char* filename = argv[i];
		unsigned char *uri_string;
		raptor_uri *uri, *base_uri;

		uri_string = raptor_uri_filename_to_uri_string(filename);
		uri = raptor_new_uri(world, uri_string);
		base_uri = raptor_uri_copy(uri);
		FILE *fp = fopen(filename, "r"); //our test file, with the contents "123abc"
		char buf[4096];
		memset(buf, '\0', sizeof(buf));
		setvbuf(fp, buf, _IOFBF, 4096);
		count = 0;
		if (!raptor_parser_parse_file_stream(rdf_parser, fp, NULL, base_uri)) {
			fprintf(stderr, "%s : %d triples\n", filename, count);
			total_count += count;
			files_count++;
		} else {
			fprintf(stderr, "%s : failed to parse\n", filename);
		}

		clock_gettime(CLOCK_MONOTONIC, &requestEnd);

		// Calculate time it took
		double accum = (requestEnd.tv_sec - requestStart.tv_sec)
				+ (requestEnd.tv_nsec - requestStart.tv_nsec) / BILLION;
		printf("%lf with %i objects and %i properties\n", accum, id, propId);

		raptor_free_uri(base_uri);
		raptor_free_uri(uri);
		raptor_free_memory(uri_string);

	}

	finalizeInserts();
	saveDictionaryToDisk(hashtable, dbExt, 1);
	saveDictionaryToDisk(prophash, dbExt, 0);
	createInverseTables();
	rc = sqlite3_exec(dbExt, "END", 0, 0, 0);
	if (rc != SQLITE_OK) {

		fprintf(stderr, "Cannot commit: %s\n", sqlite3_errmsg(dbExt));
		sqlite3_close(dbExt);

		return 1;
	}

	rc = sqlite3_close(dbExt);
	if (rc != SQLITE_OK) {

		fprintf(stderr, "Cannot close ext connection: %s\n",
				sqlite3_errmsg(dbExt));
		sqlite3_close(dbExt);

		return 1;
	}

	raptor_free_parser(rdf_parser);

	raptor_free_world(world);

	fprintf(stderr, "Total count: %d files  %d triples imported\n", files_count,
			total_count);

	return SQLITE_OK;
}

static int parjDisconnect(sqlite3_vtab *pVtab) {
	parj_vtab *pVt = (parj_vtab*) pVtab;
	sqlite3_free(pVt);
	return SQLITE_OK;
}

static int parjOpen(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor) {

	return SQLITE_OK;
}

static int parjClose(sqlite3_vtab_cursor *cur) {

	sqlite3_free(cur);
	return SQLITE_OK;
}

static int parjNext(sqlite3_vtab_cursor *cur) {

	return SQLITE_OK;
}

static int parjColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int i) {

	return SQLITE_OK;
}

static int parjRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid) {
	*pRowid = 0;
	return SQLITE_OK;
}

static int parjEof(sqlite3_vtab_cursor *cur) {
	return 1;
}

static int parjFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
		const char *idxStr, int argc, sqlite3_value **argv) {

	return SQLITE_OK;
}

static int parjBestIndex(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo) {

	return SQLITE_OK;

}

/*
 ** A virtual table module that provides read-only access to a
 ** Tcl global variable namespace.
 */
static sqlite3_module parjModule = { 0, /* iVersion */

parjConnect, parjConnect, parjBestIndex, parjDisconnect,
		parjDisconnect, parjOpen, /* xOpen - open a cursor */
		parjClose, /* xClose - close a cursor */
		parjFilter, /* xFilter - configure scan constraints */
		parjNext, /* xNext - advance a cursor */
		parjEof, /* xEof - check for end of scan */
		parjColumn, /* xColumn - read data */
		parjRowid, /* xRowid - read data */
		0, /* xUpdate */
		0, /* xBegin */
		0, /* xSync */
		0, /* xCommit */
		0, /* xRollback */
		0, /* xFindMethod */
		0, /* xRename */
};

#endif /* SQLITE_OMIT_VIRTUALTABLE */

#ifdef _WIN32
__declspec(dllexport)
#endif

#ifndef SQLITE_OMIT_VIRTUALTABLE

/**************** start memorywrapper.c ***********/

typedef struct memorywrapper_vtab memorywrapper_vtab;
struct memorywrapper_vtab {
	sqlite3_vtab base; /* Base class - must be first */
	//char* zSql[0];
	uint16_t partitions;
	uint16_t inverse;
	int position;
	//int *iter;

	//int currentPosition;
	int maxSize;
//first outer, then inner
//int *iterOuter;
//sqlite3 *db; /* The database connection */
	int iter0;
	int iter1;
	int pad[9];
	//int *iters  __attribute__((aligned(0x100000)));
};

/* A memorywrapper cursor object */
typedef struct memorywrapper_cursor memorywrapper_cursor;
struct memorywrapper_cursor {
	//sqlite3_vtab_cursor base; /* Base class - must be first */
	memorywrapper_vtab* pVtab;
	const uint32_t * objects;
    
	uint32_t o;
	uint32_t s;
    int end;
	int both;
	//0->only s, 1->both s, o, 2-> scan
	int eof;
//memorywrapper_vtab* pVtab;
//struct subjectVector *sv;

};

//float real_time, proc_time, mflops;
//  long long flpins;
//  int retval;

static int memorywrapperConnect(sqlite3 *db, void *pAux, int argc,
		const char * const *argv, sqlite3_vtab **ppVtab, char **pzErr) {
	int parts;

	sscanf(argv[3], "%d", &parts);
	int propNo;
	sscanf(argv[4], "%d", &propNo);
	if (propNo < 0) {
		loadMemoryData(db, 1, propNo);
		return SQLITE_OK;
	}

	sqlite3_vtab *pNew __attribute__((aligned(0x100000)));
	pNew = *ppVtab = sqlite3_malloc(sizeof(*pNew));
	if (pNew == 0)
		return SQLITE_NOMEM;
	sqlite3_declare_vtab(db,
			"CREATE TABLE x(first INTEGER, second INTEGER, partition INTEGER HIDDEN, secondShard INTEGER HIDDEN)");
	memset(pNew, 0, sizeof(*pNew));
	memorywrapper_vtab *pVt __attribute__((aligned(0x100000))) = 0;
	pVt = sqlite3_malloc(sizeof(*pVt));
	if (pVt == 0)
		return SQLITE_NOMEM;
	memset(pVt, 0, sizeof(*pVt));
	//pVt->db = db;
	*ppVtab = &pVt->base;
	int i;
	sscanf(argv[5], "%d", &(pVt->inverse));

	pVt->partitions = parts;
    
	pVt->position = (propNo  * 2)
			+ (pVt->inverse);

	
	pVt->iter0=0;
	pVt->iter1=0;

	return SQLITE_OK;
}

static int memorywrapperDisconnect(sqlite3_vtab *pVtab) {
	memorywrapper_vtab *pVt = (memorywrapper_vtab*) pVtab;
	//printf("Real_time:\t%f\nProc_time:\t%f\nTotal flpins:\t%lld\nMFLOPS:\t\t%f\n",
	//real_time, proc_time, flpins, mflops);
	int i;
	//rb_iter_object_dealloc(pVt->iter);
	//rb_iter_subject_dealloc(pVt->iterOuter);

	//free(pVt->iters);
	sqlite3_free(pVt);
	return SQLITE_OK;
}

static int memorywrapperOpen(sqlite3_vtab *pVTab,
		sqlite3_vtab_cursor **ppCursor) {
/*
char errstring[PAPI_MAX_STR_LEN];
	if((retval = PAPI_library_init(PAPI_VER_CURRENT)) != PAPI_VER_CURRENT )
   {
      fprintf(stderr, "Error: %d %s\n",retval, errstring);
      exit(1);
   }*/
	memorywrapper_vtab *pVt = (memorywrapper_vtab*) pVTab;
	memorywrapper_cursor *pCur __attribute__((aligned(0x100000)));
	

	pCur = sqlite3_malloc(sizeof(*pCur));
	if (pCur == 0)
		return SQLITE_NOMEM;
	memset(pCur, 0, sizeof(*pCur));
	pCur->pVtab = pVt;
	// *ppCursor = &pCur->base;
	*ppCursor = (sqlite3_vtab_cursor*) &pCur->pVtab;
	//pCur->count = 0;

	return SQLITE_OK;
}

static int memorywrapperClose(sqlite3_vtab_cursor *cur) {

	memorywrapper_cursor *pCur = (memorywrapper_cursor*) cur;
	pCur->o = 0;
//printf("binary.:%ld  scan: %ld\n ", binaryCounter1, scanCounter); 
/*	 retval = PAPI_query_event(PAPI_TLB_DM);
	 if (retval != PAPI_OK) {
	 printf("PAPI_TLB_TL not available\n");
	 //test_skip(test_string);
	 }
	 printf("l2 acc.:%ld l2 miss: %ld l3 acc.:%ld l3 mis.:%ld tlb misses:%ld counter:%i binarySearches:%i \n", l2_access, l2_misses, l3_access, l3_misses, tlb_data_misses, papiCounter, binaryCounter);
	
tlb_data_misses=0L;
l2_access=0L;
l2_misses=0L;
l3_access=0L;
l3_misses=0L;*/
	sqlite3_free(cur);
	return SQLITE_OK;
}

static int memorywrapperNext(sqlite3_vtab_cursor *cur) {

	memorywrapper_cursor *pCur = (memorywrapper_cursor*) cur;
	memorywrapper_vtab *pVtab = (memorywrapper_vtab *) pCur->pVtab;

	//pCur->count++;

	if (pCur->both == 2) {

		if (pCur->end
				> pVtab->iter1) {
			pCur->o = pCur->objects[pVtab->iter1++];
		} else {
			//printf("next s");

			//pVtab->iter1 = 0;
			
                        if (noOfSubjectsGlobal[pVtab->position]
                                        > pVtab->iter0) {

                pVtab->iter1 = sizesGlobal[pVtab->position][pVtab->iter0-1];
                pCur->end = sizesGlobal[pVtab->position][pVtab->iter0];
                //pCur->objects=&objectsGlobal[pVtab->position][0];
				//pCur->sv = vectorGlobal[pVtab->position][pVtab->iter0];
				pCur->s = subjectsGlobal[pVtab->position][pVtab->iter0];
				//pVtab->iters[0]+=pVtab->partitions;
				pCur->o = pCur->objects[pVtab->iter1++];
				pVtab->iter0+=pVtab->partitions;

			} else {
				pVtab->iter0 = 0;
				pCur->eof = 1;
			}

		}
	}

	else if (pCur->both) {
		pCur->eof = 1;
	} else {
		if (pVtab->maxSize
				> pVtab->iter1) {
			//printf("next single");

			pCur->o = pCur->objects[pVtab->iter1++];
		}

		else {
			pVtab->iter1 = 0;
			pCur->eof = 1;

		}
	}

	return SQLITE_OK;
}

static int memorywrapperColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx,
		int i) {

	memorywrapper_cursor *pCur = (memorywrapper_cursor*) cur;
	if (i == 0)
		sqlite3_result_int(ctx, pCur->s);

	else
		sqlite3_result_int(ctx, pCur->o);
	return SQLITE_OK;
}

static int memorywrapperRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid) {
	//memorywrapper_cursor *pCur = (memorywrapper_cursor*) cur;
	// *pRowid = pCur->count;
	return SQLITE_OK;
}

static int memorywrapperEof(sqlite3_vtab_cursor *cur) {
	memorywrapper_cursor *pCur = (memorywrapper_cursor*) cur;
	return pCur->eof;
}

const unsigned char first = 1 << 0;
const unsigned char second = 1 << 1;
const unsigned char partition = 1 << 2;
const unsigned char secondShard = 1 << 3;

static int memorywrapperFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
		const char *idxStr, int argc, sqlite3_value **argv) {

	memorywrapper_cursor *pCur = (memorywrapper_cursor *) pVtabCursor;


	memorywrapper_vtab *pVtab = (memorywrapper_vtab *) pCur->pVtab;

	

	if (idxNum == partition) {
		//only partition column is constrained
		pCur->both = 2;
		int partNo;
		partNo = sqlite3_value_int(argv[0]);
		stick_this_thread_to_core(partNo);
       
		pCur->eof = 1;

pVtab->iter0=partNo;

			if (pVtab->iter0 >noOfSubjectsGlobal[pVtab->position] - 1) {
				pCur->eof = 1;
				pVtab->iter1 = 0;
			}
    


		else {
			//const struct subjectVector *v2 =
				//	(struct subjectVector*) subjects[pVtab->iters[0]];
			//const uint32_t subjectId = subjectIds[pVtab->iters[0]];
			//; v2 = rb_iter_next(iter)) {
			if(pVtab->iter0){
            pVtab->iter1=sizesGlobal[pVtab->position][pVtab->iter0-1];
        }
        else{
            pVtab->iter1=0;
        }
            pCur->end=sizesGlobal[pVtab->position][pVtab->iter0];
			pCur->objects = &objectsGlobal[pVtab->position][0];
			pCur->s = subjectsGlobal[pVtab->position][pVtab->iter0];
			pCur->o = pCur->objects[ pVtab->iter1++];
			pCur->eof = 0;
			pVtab->iter0+=pVtab->partitions;

		}

		return SQLITE_OK;
	}

	uint32_t subject = sqlite3_value_int(argv[0]);
	//int partI = subject % (pVtab->partitions);
	//pCur->i = subject % (pVtab->partitions);
	//pCur->i = subject & 0x00000001	;
//if (idxNum & 0x80000000) {
	int shard;
	shard = -1;
	if (idxNum & partition) {
		//printf("in\n");
		//if(idxNum<0){
		int partition;
		if (idxNum & secondShard) {
			//printf("shard\n");
			partition = sqlite3_value_int(argv[argc - 1]);

			shard = sqlite3_value_int(argv[argc - 2]);
			stick_this_thread_to_core(shard);
			//printf("sticked\n");
		} else {
//printf("not shard\n");
			partition = sqlite3_value_int(argv[argc - 1]);
			stick_this_thread_to_core(partition);
		}
		
	}
	
	pCur->eof = 0;
	 int f = summaryBinarySearchSubject(pVtab->position, subject,
//		 start, 0, noOfSubjectsGlobal[pVtab->position],
 0,
			&(pVtab->iter0));

	if (f) {
//printf("found...\n");
        if(pVtab->iter0){
            pVtab->iter1=sizesGlobal[pVtab->position][pVtab->iter0-1];
        }
        else{
            pVtab->iter1=0;
        }
        pCur->end=sizesGlobal[pVtab->position][pVtab->iter0];
        pCur->objects=&objectsGlobal[pVtab->position][0];
		//pCur->sv = f;
		pCur->s = subject;
		//if (idxNum == 2 || idxNum == -2) {
		if (idxNum & second) {
			//both s and o are bound
			pCur->o = sqlite3_value_int(argv[1]);
			pCur->both = 1;

			//if (pVtab->iter1 >= f->noOfObjects) {
			//	pVtab->iter1 = 0;
				//printf("equal! %i\n", pVtab->iters[pVtab->partitions+partI]);
			//}
			/*int distance = pCur->o
					- pCur->objects[pVtab->iter1];

			if (distance == 10000000 ) {
				//printf("current out: %i  subsrc:%i \n", f->objects[pVtab->iters[pVtab->partitions+partI]], pVtab->iters[pVtab->partitions+partI]);
				if (scanObject(pCur->o, pCur->objects[pVtab->iter1], f->noOfObjects,
						&(pVtab->iter1))) {
					return SQLITE_OK;
				}
			} else {*/

				int left, right, mid;
				/* Initializations */
				left = pVtab->iter1;
				right = pCur->end - 1;

				while (left <= right) {
					//binary seacrh on objects
					mid = (left + right) / 2;

					if (pCur->o == pCur->objects[mid]) {
						//element found

						pVtab->iter1 = mid;
						return SQLITE_OK;
					} else if (pCur->o > pCur->objects[mid]) {
						left = mid + 1;
					} else {
						right = mid - 1;
					}
				}
//printf("mid:: %d\n", mid);
//pVtab->iters[pVtab->partitions+partI]=mid;
			//}
			//printf("elemnt %i not found, objects size: %i, obj 0:%i \n", pCur->o, f->noOfObjects, f->objects[0]);
			pCur->eof = 1;

		} else if (shard > -1) {
            int noOfObjects=pCur->end - pVtab->iter1 + 1;
			//printf("shard>-1\n");
			//only s is bound, iterate over shard of objects
			int shardSize;
			shardSize = (noOfObjects + pVtab->partitions - 1)
					/ pVtab->partitions;
			pVtab->iter1 += shardSize * shard;
			pVtab->maxSize = pVtab->iter1
					+ shardSize;
			
			//printf("max: %i iter1: %i shardsize: %i \n", pVtab->maxSize, pVtab->iter1, shardSize);
			if (pVtab->maxSize > pCur->end) {
				pVtab->maxSize = pCur->end;
			}
			if (pCur->end > pVtab->iter1 ) {
				pCur->o = pCur->objects[pVtab->iter1++];
			} else{
				pCur->eof = 1;
				pVtab->iter1 = 0;
			}
			//printf("in position: %i its %i \n", pVtab->iter1, pCur->objects[pVtab->iter1]);
			//pCur->o = pCur->objects[pVtab->iter1++];
			//printf("in position: %i its %i \n", pVtab->iter1, pCur->objects[pVtab->iter1]);
		} else {
			//printf("shard<0\n");
			//only s is bound, iterate result set over all objects
			pCur->both = 0;
			//pVtab->iter1 = 0;
			pCur->o = pCur->objects[pVtab->iter1++];
			//prefetch
			/*if(pVtab->position==160){
			 int mod=pCur->o%32;
			 uint32_t *next;
			 next=subjectsGlobal[32+mod];
			 __builtin_prefetch(&next[noOfSubjectsGlobal[32+mod]/2]);
			 }*/
			pVtab->maxSize = pCur->end ;
		}
	} else {
		//printf("not found\n");
		pCur->eof = 1;

	}

	return SQLITE_OK;
}

static int memorywrapperBestIndex(sqlite3_vtab *tab,
		sqlite3_index_info *pIdxInfo) {

	int i;
	//binaryCounter1=0;
	//scanCounter=0;
	int col = 0;
	//int inequality = 1;
	pIdxInfo->idxNum = 0;

	pIdxInfo->estimatedCost = 1000;
	pIdxInfo->estimatedRows = 1000;
	const struct sqlite3_index_constraint *pConstraint;
	pConstraint = pIdxInfo->aConstraint;

	for (i = 0; i < pIdxInfo->nConstraint; i++, pConstraint++) {
		if (pConstraint->usable == 0)
			continue;
		pIdxInfo->idxNum |= 1 << pConstraint->iColumn;
		// printf("contrained iCol:%i\n", pConstraint->iColumn);
		if (pConstraint->iColumn == 2) {
			//partiton
			//scan whole table
			//inequality = -1;
			int j;
			int notUsedConstraints;
			notUsedConstraints = 0;
			int shard = -1;
			const struct sqlite3_index_constraint *pConstraint2;
			pConstraint2 = pIdxInfo->aConstraint;
			for (j = 0; j < pIdxInfo->nConstraint; j++, pConstraint2++) {
				//count not used contraints
				if (pConstraint2->usable == 0) {
					notUsedConstraints++;
					//printf("not usable:%i \n", j);
				}

				if (pConstraint->iColumn == 3) {
					//scan only specific shard
					shard = j;
					//notUsedConstraints--;
				}

			}

			pIdxInfo->aConstraintUsage[i].argvIndex = pIdxInfo->nConstraint
					- notUsedConstraints;
			pIdxInfo->aConstraintUsage[i].omit = 1;

			if (shard > -1) {
				//printf("shard:%i \n", shard);
				pIdxInfo->aConstraintUsage[shard].argvIndex =
						pIdxInfo->nConstraint - notUsedConstraints - 1;
				pIdxInfo->aConstraintUsage[shard].omit = 1;
			}

			continue;
			//pIdxInfo->idxNum =4;
			//return SQLITE_OK;
		}

		//col+=pConstraint->iColumn+1;
		col++;
		pIdxInfo->estimatedCost -=30;
		if (pConstraint->iColumn == 0) {
			pIdxInfo->aConstraintUsage[i].argvIndex = 1;
		} else {
			pIdxInfo->aConstraintUsage[i].argvIndex = 2;
		}
		pIdxInfo->aConstraintUsage[i].omit = 1;
	}
	//0 scan table specific partition
	//1 only s
	//2 both s and o
	//-1 only s specific partition
	//-2 both s and o specific partition
	//pIdxInfo->idxNum = col * inequality;

	return SQLITE_OK;

}

/*
 ** A virtual table module that provides read-only access to a
 ** Tcl global variable namespace.
 */
static sqlite3_module memorywrapperModule = {  0, /* iVersion */

memorywrapperConnect, memorywrapperConnect, memorywrapperBestIndex,
		memorywrapperDisconnect, memorywrapperDisconnect, memorywrapperOpen, /* xOpen - open a cursor */
		memorywrapperClose, /* xClose - close a cursor */
		memorywrapperFilter, /* xFilter - configure scan constraints */
		memorywrapperNext, /* xNext - advance a cursor */
		memorywrapperEof, /* xEof - check for end of scan */
		memorywrapperColumn, /* xColumn - read data */
		memorywrapperRowid, /* xRowid - read data */
		0, /* xUpdate */
		0, /* xBegin */
		0, /* xSync */
		0, /* xCommit */
		0, /* xRollback */
		0, /* xFindMethod */
		0, /* xRename */
};

#endif /* SQLITE_OMIT_VIRTUALTABLE */

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_memorywrapper_init(sqlite3 *db, char **pzErrMsg,
		const sqlite3_api_routines *pApi) {
	int rc = SQLITE_OK;
	SQLITE_EXTENSION_INIT2(pApi);
#ifndef SQLITE_OMIT_VIRTUALTABLE
	rc = sqlite3_create_module(db, "memorywrapper", &memorywrapperModule, 0);
#endif
	return rc;
}

/************** End of memorywrapper.c ************************************************/



#ifndef SQLITE_OMIT_VIRTUALTABLE
/**************** start dictionary.c ***********/

typedef struct dictionary_vtab dictionary_vtab;
struct dictionary_vtab {
	sqlite3_vtab base; /* Base class - must be first */

	
};

/* A dictionary cursor object */
typedef struct dictionary_cursor dictionary_cursor;
struct dictionary_cursor {
	//sqlite3_vtab_cursor base; /* Base class - must be first */
	dictionary_vtab* pVtab;
	int eof;
    int id;


};

//float real_time, proc_time, mflops;
//  long long flpins;
//  int retval;

static int dictionaryConnect(sqlite3 *db, void *pAux, int argc,
		const char * const *argv, sqlite3_vtab **ppVtab, char **pzErr) {
	int parts;


	sqlite3_vtab *pNew __attribute__((aligned(0x100000)));
	pNew = *ppVtab = sqlite3_malloc(sizeof(*pNew));
	if (pNew == 0)
		return SQLITE_NOMEM;
	sqlite3_declare_vtab(db,
			"CREATE TABLE x(id INTEGER, uri TEXT)");
	memset(pNew, 0, sizeof(*pNew));
	dictionary_vtab *pVt __attribute__((aligned(0x100000))) = 0;
	pVt = sqlite3_malloc(sizeof(*pVt));
	if (pVt == 0)
		return SQLITE_NOMEM;
	memset(pVt, 0, sizeof(*pVt));
	//pVt->db = db;
	*ppVtab = &pVt->base;
	

	return SQLITE_OK;
}

static int dictionaryDisconnect(sqlite3_vtab *pVtab) {
	dictionary_vtab *pVt = (dictionary_vtab*) pVtab;
	
	sqlite3_free(pVt);
	return SQLITE_OK;
}

static int dictionaryOpen(sqlite3_vtab *pVTab,
		sqlite3_vtab_cursor **ppCursor) {
/*
char errstring[PAPI_MAX_STR_LEN];
	if((retval = PAPI_library_init(PAPI_VER_CURRENT)) != PAPI_VER_CURRENT )
   {
      fprintf(stderr, "Error: %d %s\n",retval, errstring);
      exit(1);
   }*/
	dictionary_vtab *pVt = (dictionary_vtab*) pVTab;
	dictionary_cursor *pCur __attribute__((aligned(0x100000)));
	

	pCur = sqlite3_malloc(sizeof(*pCur));
	if (pCur == 0)
		return SQLITE_NOMEM;
	memset(pCur, 0, sizeof(*pCur));
	pCur->pVtab = pVt;
	// *ppCursor = &pCur->base;
	*ppCursor = (sqlite3_vtab_cursor*) &pCur->pVtab;
	//pCur->count = 0;

	return SQLITE_OK;
}

static int dictionaryClose(sqlite3_vtab_cursor *cur) {

	//dictionary_cursor *pCur = (dictionary_cursor*) cur;
	sqlite3_free(cur);
	return SQLITE_OK;
}

static int dictionaryNext(sqlite3_vtab_cursor *cur) {
        dictionary_cursor *pCur = (dictionary_cursor*) cur;
        pCur->eof=1;
	return SQLITE_OK;
}

static int dictionaryColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx,
		int i) {

	dictionary_cursor *pCur = (dictionary_cursor*) cur;
	if (i == 0)
		sqlite3_result_int(ctx, pCur->id);

	else
		sqlite3_result_text(ctx, dictionary[pCur->id], -1, SQLITE_STATIC);
	return SQLITE_OK;
}

static int dictionaryRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid) {
	//dictionary_cursor *pCur = (dictionary_cursor*) cur;
	// *pRowid = pCur->count;
	return SQLITE_OK;
}

static int dictionaryEof(sqlite3_vtab_cursor *cur) {
	dictionary_cursor *pCur = (dictionary_cursor*) cur;
	return pCur->eof;
}



static int dictionaryFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
		const char *idxStr, int argc, sqlite3_value **argv) {

	dictionary_cursor *pCur = (dictionary_cursor *) pVtabCursor;
	dictionary_vtab *pVtab = (dictionary_vtab *) pCur->pVtab;
	pCur-> id = sqlite3_value_int(argv[0]);
        pCur->eof=0;

	return SQLITE_OK;
}

static int dictionaryBestIndex(sqlite3_vtab *tab,
		sqlite3_index_info *pIdxInfo) {

	int i;
	//binaryCounter1=0;
	//scanCounter=0;
	int col = 0;
	//int inequality = 1;
	pIdxInfo->idxNum = 0;

	pIdxInfo->estimatedCost = 1;
	pIdxInfo->estimatedRows = 1;
	const struct sqlite3_index_constraint *pConstraint;
	pConstraint = pIdxInfo->aConstraint;

	for (i = 0; i < pIdxInfo->nConstraint; i++, pConstraint++) {
		if (pConstraint->usable == 0)
			continue;
		pIdxInfo->idxNum = SQLITE_INDEX_SCAN_UNIQUE;
		
        pIdxInfo->aConstraintUsage[i].argvIndex = 1;
      pIdxInfo->aConstraintUsage[i].omit = 1;
	}
	//0 scan table specific partition
	//1 only s
	//2 both s and o
	//-1 only s specific partition
	//-2 both s and o specific partition
	//pIdxInfo->idxNum = col * inequality;

	return SQLITE_OK;

}

/*
 ** A virtual table module that provides read-only access to a
 ** Tcl global variable namespace.
 */
static sqlite3_module dictionaryModule = {  0, /* iVersion */

dictionaryConnect, dictionaryConnect, dictionaryBestIndex,
		dictionaryDisconnect, dictionaryDisconnect, dictionaryOpen, /* xOpen - open a cursor */
		dictionaryClose, /* xClose - close a cursor */
		dictionaryFilter, /* xFilter - configure scan constraints */
		dictionaryNext, /* xNext - advance a cursor */
		dictionaryEof, /* xEof - check for end of scan */
		dictionaryColumn, /* xColumn - read data */
		dictionaryRowid, /* xRowid - read data */
		0, /* xUpdate */
		0, /* xBegin */
		0, /* xSync */
		0, /* xCommit */
		0, /* xRollback */
		0, /* xFindMethod */
		0, /* xRename */
};

#endif /* SQLITE_OMIT_VIRTUALTABLE */

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_dictionary_init(sqlite3 *db, char **pzErrMsg,
		const sqlite3_api_routines *pApi) {
	int rc = SQLITE_OK;
	SQLITE_EXTENSION_INIT2(pApi);
#ifndef SQLITE_OMIT_VIRTUALTABLE
	rc = sqlite3_create_module(db, "dictionary", &dictionaryModule, 0);
#endif
	return rc;
}

/************** End of dictionary.c ************************************************/


int stick_this_thread_to_core(int core_id) {
	int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
	core_id = core_id % num_cores;
	if (core_id < 0 || core_id >= num_cores) {
		printf("error, core no:%i\n", core_id);
		return EINVAL;
	}

	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(core_id, &cpuset);

	pthread_t current_thread = pthread_self();
//printf("setting core no:%i\n", core_id);    
	return pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
	//return 1;
}



static inline  int bitmapSearch(const int position,
                const uint32_t subject, int* iterOuter) {

// const uint32_t  *  bitmap=bitmapsGlobal[position];
//printf("position: %i subject:%i\n", position, subject);
int valuepos=(subject/480)<<4;
int subjmod=subject%480;
int subjpos=subjmod+(valuepos<<5)+32;

    if(TestBit(position, subjpos)){

*iterOuter=bitmapsGlobal[position][valuepos];
  int startPos=(valuepos<<5)+32;
    int k;

        int end=startPos+subjmod-31;
        for(k=startPos;k<end;k+=32){
  //    printf("k:%i \n", k);
        *iterOuter+=//__builtin_popcount(bitmap[++valuepos]);
popcount32e(bitmapsGlobal[position][++valuepos]);
//__builtin_popcount(bitmap[++valuepos]);
//      printf("value: %i \n", valueAtStart);
        }
        //__builtin_prefetch(&subjects[*iterOuter]);
        *iterOuter+=//__builtin_popcount(bitmap[(++valuepos)]<<(31-(subjmod%32)));
popcount32e(bitmapsGlobal[position][(++valuepos)]<<(31-(subjmod%32)));
			__builtin_prefetch(&subjectsGlobal[position][*iterOuter]);
                        return 1;
    

}
    else{ 
    return 0;
        }



}

static inline  int scanSubject( int position,
                uint32_t subject, 
                int* iterOuter) {
//printf("current:%i\n", *iterOuter);
//if(*iterOuter >= noOfSubjectsGlobal){
//      printf(">>>>>>!!!!!!!!: %i  %i  \n", *iterOuter, noOfSubjectsGlobal);
//}
//struct subjectVector* result=NULL;
        if (subject == subjectsGlobal[position][*iterOuter]) {
                return 1;
        } else if (subject > subjectsGlobal[position][*iterOuter]) {
                while (*iterOuter < noOfSubjectsGlobal[position] - 1) {
                        (*iterOuter)++;
                        if (subject == subjectsGlobal[position][*iterOuter]) {
                                return 1;
                        } else if (subject < subjectsGlobal[position][*iterOuter]) {
                                return 0;
                        }
                }
        } else {
                while (*iterOuter > 0) {
                        (*iterOuter)--;
                        if (subject == subjectsGlobal[position][*iterOuter]) {
                                return 1;
                        } else if (subject > subjectsGlobal[position][*iterOuter]) {
                                return 0;
                        }
                }

        }
        return 0;
}

static inline int summaryBinarySearchSubject(
		const int position, const uint32_t subject, int start, int* iterOuter) {
//printf("current:%i \n", *iterOuter);
const int distance = subject - subjectsGlobal[position][*iterOuter];
//printf("distance: %i \n", distance);
        if (distance < windowSizeGlobal[position] && distance > -windowSizeGlobal[position]) {
  //      scanCounter++;
           //printf("current:%i window:%i\n", *iterOuter, windowSize);
                return scanSubject(position, subject,
                                iterOuter);
       }
//binaryCounter1++;
if(USEBITMAP){
return bitmapSearch(position, subject, iterOuter);

    }

else{
	return binarySearchSubject(position, subject, 0, iterOuter);
}


}


static inline int binarySearchSubject(
                int position, uint32_t subject, int start, int* iterOuter) {
        int left, right, mid;
        /* Initializations */
        left = start;
        right = noOfSubjectsGlobal[position] - 1;

        while (left <= right) {
                mid = (left + right) / 2;
                ///printf("left%i right:5 mid:%i \n", left, right,  mid);
                //binaryCounter++;

                if (subject == subjectsGlobal[position][mid]) {
                        //element found
                        //printf("mid:%i\n", mid);
                        (*iterOuter) = mid;
                        return 1;
                        //return result;
                } else if (subject > subjectsGlobal[position][mid]) {
                        left = mid + 1;
                } else {
                        right = mid - 1;
                }
        }
        (*iterOuter) = mid;
        return 0;
}



int scanObject(uint32_t object,const uint32_t* objects, const int noOfObjects, int* iter) {
//0=failure
//printf("to find:%i,noOFObj:% iter:%i current: %i \n", object, noOfObjects, *iter, objects[*iter]);

	if (object == objects[*iter]) {
		return object;
	} else if (object > objects[*iter]) {
		while (*iter < noOfObjects - 1) {
			(*iter)++;
			if (object == objects[*iter]) {
				return object;
			} else if (object < objects[*iter]) {

				return 0;
			}

		}
	} else {
		while (*iter > 0) {
			(*iter)--;
			if (object == objects[*iter]) {
				return object;
			} else if (object > objects[*iter]) {
				return 0;
			}
		}

	}

	return 0;
}

void loadDict(const char * dir){

sqlite3 *dbt;
	int rc;

	rc = sqlite3_open_v2("file::memory:", &dbt,
	SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL);

	//sqlite3_exec(dbt, "BEGIN", 0, 0, 0);
	if (rc != SQLITE_OK) {

		fprintf(stderr, "Failed to fetch data: %s\n", sqlite3_errmsg(dbt));
		//sqlite3_close(db);

		return;
	}

	char *sql = malloc(1000);

	//strcpy(sql, "attach database '");
	//	strcat(sql, directory);
	//	strcat(sql, "' as aa;");
	strcpy(sql, "attach database '");
	strcat(sql, dir);
	strcat(sql, "' as aa;");
	//"attach database '/media/dimitris/T/templubm/rdf.db' as aa;");
	sqlite3_exec(dbt, sql, 0, 0, NULL);
	strcpy(sql, "PRAGMA synchronous = OFF;");

	sqlite3_exec(dbt, sql, 0, 0, NULL);
	strcpy(sql, "PRAGMA locking_mode = EXCLUSIVE;");

	sqlite3_exec(dbt, sql, 0, 0, NULL);
	strcpy(sql, "PRAGMA journal_mode = OFF;");

	sqlite3_exec(dbt, sql, 0, 0, NULL);
	strcpy(sql, "PRAGMA temp_store=2;");
	sqlite3_exec(dbt, sql, 0, 0, NULL);

	rc = sqlite3_exec(dbt, "BEGIN", 0, 0, 0);
	if (rc != SQLITE_OK) {

		fprintf(stderr, "Failed to fetch data: %s\n", sqlite3_errmsg(dbt));
		//sqlite3_close(db);

		pthread_exit(NULL);
	}

	
	sqlite3_stmt *pStmt;

	char* zSql;
	char select[50];
	//char buf[3];
	//sprintf(buf, "%ld", propNo);

	
	strcpy(select, "select id, uri from dictionary");
	

	//strncat(select, buf, 4);

	zSql = sqlite3_mprintf(select);

	if (zSql == 0) {
		printf("error out of memory\n");
		pthread_exit(NULL);
		//return SQLITE_NOMEM;
	}
	rc = sqlite3_prepare_v2(dbt, zSql, -1, &pStmt, NULL);

	free(sql);

	sqlite3_free(zSql);

	if (rc != SQLITE_OK) {
		printf("error no : %i ", rc);
	}

	dictionary= malloc(sizeof(char*) * maxId);
	while (sqlite3_step(pStmt) == SQLITE_ROW) {
		int s;
		s = sqlite3_column_int(pStmt, 0);
                const char * uri=(const char*)sqlite3_column_text(pStmt, 1);
		//if(s==1000) printf("test 1000:%s \n", uri);
		char* uri2=malloc(strlen(uri) + 1);
		strcpy(uri2, uri);
		dictionary[s]=uri2;
		//if(s==1000) printf("test 1000:%s \n", dictionary[1000]);
		


	}

      //printf("test 1000:%s \n", dictionary[1000]);
	rc = sqlite3_exec(dbt, "END", 0, 0, 0);
        if (rc != SQLITE_OK) {

                fprintf(stderr, "Failed to fetch data: %s\n", sqlite3_errmsg(dbt));
                //sqlite3_close(db);

                pthread_exit(NULL);
        }
        rc = sqlite3_finalize(pStmt);
        if (rc != SQLITE_OK) {

                fprintf(stderr, "Failed to fetch data: %s\n", sqlite3_errmsg(dbt));
                //sqlite3_close(db);

                pthread_exit(NULL);
        }
        rc = sqlite3_close(dbt);
	if (rc != SQLITE_OK) {

                fprintf(stderr, "Failed to fetch data: %s\n", sqlite3_errmsg(dbt));
                //sqlite3_close(db);

                pthread_exit(NULL);
        }
}

void *loadTable(void *table_info) {
//printf("startreal\n");
	struct tableInfo *table = (struct tableInfo*) table_info;
	sqlite3 *dbt;
	int rc;

	rc = sqlite3_open_v2("file::memory:", &dbt,
	SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL);

	//sqlite3_exec(dbt, "BEGIN", 0, 0, 0);
	if (rc != SQLITE_OK) {

		fprintf(stderr, "Failed to fetch data: %s\n", sqlite3_errmsg(dbt));
		//sqlite3_close(db);

		pthread_exit(NULL);
	}

	char *sql = malloc(1000);

	//strcpy(sql, "attach database '");
	//	strcat(sql, directory);
	//	strcat(sql, "' as aa;");
	strcpy(sql, "attach database '");
	strcat(sql, table->dbdir);
	strcat(sql, "' as aa;");
	//"attach database '/media/dimitris/T/templubm/rdf.db' as aa;");
	sqlite3_exec(dbt, sql, 0, 0, NULL);
	strcpy(sql, "PRAGMA synchronous = OFF;");

	sqlite3_exec(dbt, sql, 0, 0, NULL);
	strcpy(sql, "PRAGMA locking_mode = EXCLUSIVE;");

	sqlite3_exec(dbt, sql, 0, 0, NULL);
	strcpy(sql, "PRAGMA journal_mode = OFF;");

	sqlite3_exec(dbt, sql, 0, 0, NULL);
	strcpy(sql, "PRAGMA temp_store=2;");
	sqlite3_exec(dbt, sql, 0, 0, NULL);

	rc = sqlite3_exec(dbt, "BEGIN", 0, 0, 0);
	if (rc != SQLITE_OK) {

		fprintf(stderr, "Failed to fetch data: %s\n", sqlite3_errmsg(dbt));
		//sqlite3_close(db);

		pthread_exit(NULL);
	}

	long propNo = table->propNo;
	//int partitions = table->partitions;
	int index;
	if (table->inverse) {
		index = (propNo * 2) + 1;
	} else {
		index = propNo * 2;
	}
	//printf("index: %i propNo: %ld, partitions: %i \n", index, propNo, partitions);

	sqlite3_stmt *pStmt;

	char* zSql;
	char select[70];
	char buf[4];
    char par[2];
	sprintf(buf, "%ld", propNo);
    sprintf(par, ")");
    int countDistinct=0;
    int countAll=0;
    

    if (table->inverse) {
		strcpy(select, "select count(*) from invprop");
	} else {
		strcpy(select, "select count(*) from prop");
	}

	strncat(select, buf, 4);
	//strncat(select, par, 1);
	zSql = sqlite3_mprintf(select);
	
	//printf("select: %s buf: %s  propno: %ld \n", select, buf, propNo);
	
	if (zSql == 0) {
		printf("error out of memory\n");
		pthread_exit(NULL);
		//return SQLITE_NOMEM;
	}
	//sqlite3_extended_result_codes(dbt, 1);

	rc = sqlite3_prepare_v2(dbt, zSql, -1, &pStmt, NULL);
	if (rc != SQLITE_OK) {
                printf("error... no : %s %i ", sqlite3_errmsg(dbt), rc);
        }

    
    rc = sqlite3_step(pStmt);
    
    if (rc == SQLITE_ROW) {
        countAll=sqlite3_column_int(pStmt, 0);
    }

    sqlite3_free(zSql);
    rc = sqlite3_finalize(pStmt);
    sqlite3_stmt *pStmt2;
	if (rc != SQLITE_OK) {

		fprintf(stderr, "Failed to fetch data: %s\n", sqlite3_errmsg(dbt));
		//sqlite3_close(db);

		pthread_exit(NULL);
	}
    

    if (table->inverse) {
		strcpy(select, "select count(*) from (select distinct o from invprop");
	} else {
		strcpy(select, "select count(*) from (select distinct s from prop");
	}

	strncat(select, buf, 4);
        strncat(select, par, 1);

	zSql = sqlite3_mprintf(select);

	if (zSql == 0) {
		printf("error out of memory\n");
		pthread_exit(NULL);
		//return SQLITE_NOMEM;
	}
	rc = sqlite3_prepare_v2(dbt, zSql, -1, &pStmt2, NULL);
if (rc != SQLITE_OK) {
                printf("error no:::: %i ", rc);
        }
    

    rc = sqlite3_step(pStmt2);
    
    if (rc == SQLITE_ROW) {
        countDistinct=sqlite3_column_int(pStmt2, 0);
    }

    sqlite3_free(zSql);
    rc = sqlite3_finalize(pStmt2);
	if (rc != SQLITE_OK) {

		fprintf(stderr, "Failed to fetch data: %s\n", sqlite3_errmsg(dbt));
		//sqlite3_close(db);

		pthread_exit(NULL);
	}

	if (table->inverse) {
		strcpy(select, "select o, s from invprop");
	} else {
		strcpy(select, "select s, o from prop");
	}

	strncat(select, buf, 4);

	zSql = sqlite3_mprintf(select);

	if (zSql == 0) {
		printf("error out of memory\n");
		pthread_exit(NULL);
		//return SQLITE_NOMEM;
	}
	sqlite3_stmt *pStmt3;
	rc = sqlite3_prepare_v2(dbt, zSql, -1, &pStmt3, NULL);

	free(sql);

	sqlite3_free(zSql);

	if (rc != SQLITE_OK) {
		printf("error! no : %i ", rc);
	}
	int lastSubj;
	lastSubj = -1;
	//countDistinct=0;
    //countAll=0;
   //sprintf("all: %i distinct: %i \n", countAll, countDistinct); 

		uint32_t *subjectIds;// __attribute__((aligned(64)));
		subjectIds = malloc(sizeof(uint32_t) * countDistinct);
uint32_t *counts;// __attribute__((aligned(64)));
		counts = malloc(sizeof(uint32_t) * countDistinct);
uint32_t *objectIds;// __attribute__((aligned(64)));
		objectIds = malloc(sizeof(uint32_t) * countAll);


countDistinct=0;
countAll=0;
int s;
int o;
	while (sqlite3_step(pStmt3) == SQLITE_ROW) {
		
		//int s;
		s = sqlite3_column_int(pStmt3, 0);
		//printf("s : %i last:%i \n", s, lastSubj);
		if (lastSubj != s) {
		
               if (lastSubj>-1){
                    
                        counts[countDistinct-1]=countAll;
                    }
            
            subjectIds[countDistinct++]=s;
		
            lastSubj = s;
			
		}

		//int o;
		o = sqlite3_column_int(pStmt3, 1);
        objectIds[countAll++]=o;
		/*if(table->inverse){
		if(propNo==42 && countAll==3796 ){
			printf("its %i \n", o);
		}
		}*/

	}
    counts[countDistinct-1]=countAll;



		noOfSubjectsGlobal[index] = countDistinct;
		subjectsGlobal[index] = subjectIds;
        objectsGlobal[index] = objectIds;
        sizesGlobal[index] = counts;
//printf("no of subjs: %i \n", noOfSubjectsGlobal[index]);

if(USEBITMAP)
{
        uint32_t *bitmap ;//__attribute__((aligned(0x10000000)));
        
        int bitmapSize=(((maxId+1)/480)<<4)+64;

		bitmap = malloc(sizeof(uint32_t) * bitmapSize);
        
        int j;
        //int l;
        //l=0;
        int position=0;
	int index2=0;
	int lll=0;
        for(j=0;j<bitmapSize;j+=16){

            bitmap[j]=position-1;
		lll+=32;

            int k;
	
            for(k=0;k<480&&position<countDistinct;k++,lll++,index2++){
		
                if(subjectIds[position]==index2){

                    SetBit(bitmap, lll);
                    position++;

                    }
                else{
			
                    ClearBit(bitmap, lll);
                }
          }

            
        }

        bitmapsGlobal[index]=bitmap;


}

    

	

	
	rc = sqlite3_exec(dbt, "END", 0, 0, 0);
	if (rc != SQLITE_OK) {

		fprintf(stderr, "Failed to fetch data: %s\n", sqlite3_errmsg(dbt));
		//sqlite3_close(db);

		pthread_exit(NULL);
	}
	rc = sqlite3_finalize(pStmt3);
	if (rc != SQLITE_OK) {

		fprintf(stderr, "Failed to fetch data: %s\n", sqlite3_errmsg(dbt));
		//sqlite3_close(db);

		pthread_exit(NULL);
	}
	rc = sqlite3_close(dbt);
	if (rc != SQLITE_OK) {

		fprintf(stderr, "Failed to fetch data: %s\n", sqlite3_errmsg(dbt));
		//sqlite3_close(db);

		pthread_exit(NULL);
	}
	//printf("created\n");

	pthread_exit(NULL);
}

int loadMemoryData(sqlite3 *db, int partitions, int loadDictionary) {
	const char *dbdir = sqlite3_db_filename(db, "m");
	int maxPropId = 0;
	char *sql = (char*) malloc(50);
	strcpy(sql, "select max(id) from properties");
	sqlite3_stmt *res;
	int rc;
	rc = sqlite3_prepare_v2(db, sql, -1, &res, 0);
	if (rc != SQLITE_OK) {

		fprintf(stderr, "Failed to read properties table: %s\n",
		sqlite3_errmsg(db));
		//sqlite3_close(db);

		return rc;
	}
	while (sqlite3_step(res) == SQLITE_ROW) {
		maxPropId = sqlite3_column_int(res, 0);
	}

	sqlite3_finalize(res);
	
	noOfSubjectsGlobal = malloc(
			partitions * 2 * (maxPropId + 1) * sizeof(int));
	windowSizeGlobal = malloc(
			partitions * 2 * (maxPropId + 1) * sizeof(int));
	subjectsGlobal = malloc(partitions * 2 * (maxPropId + 1) * sizeof(uint32_t*));
    objectsGlobal = malloc(partitions * 2 * (maxPropId + 1) * sizeof(uint32_t*));
    sizesGlobal = malloc(partitions * 2 * (maxPropId + 1) * sizeof(uint32_t*));
    //if(USEBITMAP){

	strcpy(sql, "select max(id) from dictionary");
        sqlite3_stmt *res2;
          
        rc = sqlite3_prepare_v2(db, sql, -1, &res2, 0);
        if (rc != SQLITE_OK) {

                fprintf(stderr, "Failed to read dictionary table: %s\n",
                sqlite3_errmsg(db));
                //sqlite3_close(db);

                return rc;
        }
        while (sqlite3_step(res2) == SQLITE_ROW) {
                maxId = sqlite3_column_int(res2, 0)+1;
        }
	sqlite3_finalize(res2);
	if(USEBITMAP){
    		bitmapsGlobal = malloc( 2 * (maxPropId + 1) * sizeof(uint32_t*));
	}
    //bitmapsValuesGlobal = malloc( 2 * (maxPropId + 1) * sizeof(int*));
	int i;

	//for (i = 0; i < (maxPropId+1) * 2 * partitions; i++) {
	//	struct rb_subject_tree *t = rb_subject_tree_create();
	//	treeGlobal[i] = t;
	//}
	long propNo;
	pthread_t threads[2 * (maxPropId + 1)];
	struct tableInfo tables[2 * (maxPropId + 1)];
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	void *status;
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	pthread_mutex_init(&mutexload, NULL);

	
	if(loadDictionary==-2){
		loadDict(dbdir);
	}

	for (propNo = 0; propNo < maxPropId + 1; propNo++) {

		//tables[propNo]=malloc(sizeof(struct tableInfo));
		tables[propNo].propNo = propNo;
		tables[propNo].partitions = partitions;
		tables[propNo].inverse = 0;
		tables[propNo].dbdir = dbdir;
		//tables[propNo].db=db;

		int code;
		code = pthread_create(&(threads[propNo]), &attr, loadTable,
				&tables[propNo]);
//pthread_join(threads[propNo], NULL);
		if (code != 0)
			printf("\ncan't create thread :[%s]", strerror(code));

	}
	for (propNo = 0; propNo < maxPropId + 1; propNo++) {

		//tables[propNo+maxPropId+1]=malloc(sizeof(struct tableInfo));
		tables[propNo + maxPropId + 1].propNo = propNo;
		tables[propNo + maxPropId + 1].partitions = partitions;
		tables[propNo + maxPropId + 1].inverse = 1;
		tables[propNo + maxPropId + 1].dbdir = dbdir;
		//tables[propNo+maxPropId+1].db=db;

		int code;
		code = pthread_create(&(threads[maxPropId + 1 + propNo]), &attr,
				loadTable, &tables[propNo + maxPropId + 1]);
//pthread_join(threads[maxPropId+1+propNo], NULL);
		if (code != 0)
			printf("\ncan't create thread :[%s]", strerror(code));

	}
	long t;
	pthread_attr_destroy(&attr);
	for (t = 0; t < 2 * (maxPropId + 1); t++) {

		pthread_join(threads[t], &status);
	}
	if(USEBITMAP){
		popcount32e_init();
	}
	long maxNoOfSubjs=0;
	long indexOfMaxNoOfSubjs=0;
	printf("finished loading tables	\n");

	for (t = 0; t < 2 * (maxPropId + 1); t++) {
	if(noOfSubjectsGlobal[t]>maxNoOfSubjs){
		maxNoOfSubjs=noOfSubjectsGlobal[t];
		indexOfMaxNoOfSubjs=t;
	}
	}
	//printf("1\n");
	int window;

	//int range=((subjectsGlobal[indexOfMaxNoOfSubjs][maxNoOfSubjs-1]-subjectsGlobal[indexOfMaxNoOfSubjs][0])/maxNoOfSubjs)*window;
	int k;
	uint32_t toFind=subjectsGlobal[indexOfMaxNoOfSubjs][0];
	int iter;
	int *iterPtr=&iter;
	int limit=0;
	float timeDivided;
	float timeDiff;
	int nextWindow=200;
	if(USEBITMAP){
		nextWindow=20;
	}
	//printf("2\n");
	//printf("index: %i max: %i last:%i first:%i \n", indexOfMaxNoOfSubjs, maxNoOfSubjs, subjectsGlobal[indexOfMaxNoOfSubjs][maxNoOfSubjs-1], subjectsGlobal[indexOfMaxNoOfSubjs][0]);
	do{
	window=nextWindow;
	int range=((subjectsGlobal[indexOfMaxNoOfSubjs][maxNoOfSubjs-1]-subjectsGlobal[indexOfMaxNoOfSubjs][0])/maxNoOfSubjs)*window;
	iter=0;
	long timeBinary=getTime();
	int results=0;
	 toFind=subjectsGlobal[indexOfMaxNoOfSubjs][0];
	if(USEBITMAP){
		for(k=0;k<10000&&toFind<subjectsGlobal[indexOfMaxNoOfSubjs][maxNoOfSubjs-1];k++, toFind+=range){
                //printf("tofind: %i \n", toFind);
                if(bitmapSearch(indexOfMaxNoOfSubjs, toFind, iterPtr)) results++; 
        }

	}
	else{
		for(k=0;k<10000&&toFind<subjectsGlobal[indexOfMaxNoOfSubjs][maxNoOfSubjs-1];k++, toFind+=range){
                //printf("tofind: %i \n", toFind);
                if(binarySearchSubject(indexOfMaxNoOfSubjs, toFind, 0, iterPtr)) results++; 
        }
	}
	
	timeBinary=getTime()-timeBinary;
	
	//printf("binary: %ld k:%i results: %i \n", getTime()-time, k, results);
	toFind=subjectsGlobal[indexOfMaxNoOfSubjs][0];
        iter=0;
	int results2=0;
        long timeScan = getTime();
	      
        for(k=0;k<10000&&toFind<subjectsGlobal[indexOfMaxNoOfSubjs][maxNoOfSubjs-1];k++, toFind+=range){
                if(scanSubject(indexOfMaxNoOfSubjs, toFind, iterPtr)) results2++; 
        }
	if(results!=results2) printf("different results \n");
	timeScan=getTime()-timeScan;
	
	if(timeBinary>timeScan){
		timeDiff=timeBinary-timeScan;
		timeDivided=timeDiff/(timeScan/10);
		float scanFloat=timeScan;
		float fraction=timeBinary/scanFloat;

		nextWindow=window*fraction;
	}
	else{
		timeDiff=timeScan-timeBinary;
                timeDivided=timeDiff/(timeBinary/10);
		float binaryFloat=timeBinary;
		float fraction=timeScan/binaryFloat;
                nextWindow=window/fraction;

	}
	
	limit++;
	//printf("time scan: %ld, time binary: %ld, window: %i k: %i \n", timeScan, timeBinary, window, k);

	}
	while(timeDivided>1 && limit<20);
//printf("3\n");
for (t = 0; t < 2 * (maxPropId + 1); t++) {
	 int range = subjectsGlobal[t][noOfSubjectsGlobal[t] - 1] -  subjectsGlobal[t][0];

                int estimatedWindow = 0;
                if (noOfSubjectsGlobal[t] < window) {
                        windowSizeGlobal[t] = window;

                } else {
                        estimatedWindow = range / noOfSubjectsGlobal[t];
                        estimatedWindow *= window;
                        windowSizeGlobal[t] = estimatedWindow;
                }
}
//printf("4\n");
	free(sql);
	pthread_mutex_destroy(&mutexload);


}











struct NodeBoth {
	int first;
	int second;
	int pos; // index of the array from which the element is taken
	int nextfirst; // index of the next element to be picked from array
	int nextsecond;
};

struct NodeSecond {
	int second;
	int pos; // index of the array from which the element is taken
	int nextsecond;
};

// Prototype of a utility function to swap two min heap nodes
static inline void swap(void **x, void **y);

int heap_size; // size of min heap

// Constructor: creates a min heap of given size
static inline void MinHeapBoth(int, struct NodeBoth **);

// to heapify a subtree with root at given index
static inline void MinHeapifyBoth(int, struct NodeBoth **, int);

// Constructor: creates a min heap of given size
static inline void MinHeapSecond(int, struct NodeSecond **);

// to heapify a subtree with root at given index
static inline void MinHeapifySecond(int, struct NodeSecond **, int);

// to get index of left child of node at index i
int left(int i) {
	return (2 * i + 1);
}

// to get index of right child of node at index i
int right(int i) {
	return (2 * i + 2);
}

static inline void MinHeapBoth(int size, struct NodeBoth **heap) {

	//harr = a;  // store address of array
	int i = (size - 1) / 2;
//printf("size i: %i\n", i	);
	while (i >= 0) {
		MinHeapifyBoth(i, heap, size);
		i--;
	}
}

static inline void MinHeapSecond(int size, struct NodeSecond **heap) {
	//heap_size = size;
//printf("size: %i\n", heap_size	);
	//harr = a;  // store address of array
	int i = (size - 1) / 2;
//printf("size %i size i: %i\n", size, i);
	while (i >= 0) {
		MinHeapifySecond(i, heap, size);
		i--;
	}
}

// A recursive method to heapify a subtree with root at given index
// This method assumes that the subtrees are already heapified
static inline void MinHeapifyBoth(int i, struct NodeBoth **heap, int heap_size) {

	int l = left(i);
	int r = right(i);
	int smallest = i;

	if (l < heap_size) {
		if (heap[l]->first < heap[i]->first)
			smallest = l;
		else if (heap[l]->first == heap[i]->first
				&& heap[l]->second < heap[i]->second)
			smallest = l;

	}

	if (r < heap_size) {
		if (heap[r]->first < heap[smallest]->first)
			smallest = r;
		else if (heap[r]->first == heap[smallest]->first
				&& heap[r]->second < heap[smallest]->second)
			smallest = r;
	}

	if (smallest != i) {
		swap((void**) &heap[i], (void**) &heap[smallest]);
		MinHeapifyBoth(smallest, heap, heap_size);
	}
}

static inline void MinHeapifySecond(int i, struct NodeSecond **heap, int heap_size) {

	int l = left(i);
	int r = right(i);
	int smallest = i;

	if (l < heap_size) {
		if (heap[l]->second < heap[i]->second)
			smallest = l;
	}

	if (r < heap_size) {
		if (heap[r]->second < heap[smallest]->second)
			smallest = r;
	}

	if (smallest != i) {
		swap((void**) &heap[i], (void**) &heap[smallest]);
		MinHeapifySecond(smallest, heap, heap_size);
	}
}

// A utility function to swap two elements
static inline void swap(void **x, void **y) {
	//printf("value x: %i pointer x:%p value y:%i pointer y:%p \n", x->element, x, y->element, y);
	void *temp = *x;
	*x = *y;
	*y = temp;
	//printf("out value x: %i pointer x:%p value y:%i pointer y:%p \n", x->element, x, y->element, y);
}

#ifndef SQLITE_OMIT_VIRTUALTABLE

typedef struct unionwrapper_vtab unionwrapper_vtab;
struct unionwrapper_vtab {
	sqlite3_vtab base; /* Base class - must be first */
	//char* zSql[0];
	uint16_t partitions;
	uint16_t cardinality;
	uint16_t finished;
	uint16_t existsFilter;
	int *position;
	int *filters;

	//int *currentPosition;
	int *maxSize;

	int *iters;
    int *end;
	struct NodeSecond **heap;
	struct NodeBoth **heap2;
    const uint32_t **filterVectors;
};

/* A unionwrapper cursor object */
typedef struct unionwrapper_cursor unionwrapper_cursor;
struct unionwrapper_cursor {
	//sqlite3_vtab_cursor base; /* Base class - must be first */
	unionwrapper_vtab* pVtab;

	uint32_t o;
	uint32_t s;
	int shard;
	int both;
	//0->only s, 1->both s, o, 2-> scan
	int eof;
	int heapsize;
	const uint32_t *sv[0];
//unionwrapper_vtab* pVtab;
//struct subjectVector *sv;

};

//float real_time, proc_time, mflops;
//  long long flpins;
//  int retval;

static int unionwrapperConnect(sqlite3 *db, void *pAux, int argc,
		const char * const *argv, sqlite3_vtab **ppVtab, char **pzErr) {
	int parts;

	sscanf(argv[3], "%d", &parts);
	int cardinality;
	sscanf(argv[4], "%d", &cardinality);
	if (cardinality < 0) {
		loadMemoryData(db, parts, cardinality);
		return SQLITE_OK;
	}

	sqlite3_vtab *pNew;
	pNew = *ppVtab = sqlite3_malloc(sizeof(*pNew));
	if (pNew == 0)
		return SQLITE_NOMEM;
	sqlite3_declare_vtab(db,
			"CREATE TABLE x(first INTEGER, second INTEGER, partition INTEGER HIDDEN, secondShard INTEGER HIDDEN)");
	memset(pNew, 0, sizeof(*pNew));
	unionwrapper_vtab *pVt = 0;
	pVt = sqlite3_malloc(sizeof(*pVt));
	if (pVt == 0)
		return SQLITE_NOMEM;
	memset(pVt, 0, sizeof(*pVt));
	//pVt->db = db;
	*ppVtab = &pVt->base;
	pVt->cardinality = cardinality;
	pVt->partitions = parts;
	pVt->existsFilter = 0;
	int i;

	pVt->iters = malloc(cardinality * 2 * sizeof(int) );
	for (i = 0; i < 2 * cardinality; i++) {
		pVt->iters[i] = 0;
	}

    pVt->heap = malloc(sizeof(struct NodeSecond*) * pVt->cardinality);
	pVt->heap2 = malloc(sizeof(struct NodeBoth*) * pVt->cardinality);
    
    

    pVt->filters = malloc(cardinality * sizeof(int));
    pVt->position = malloc(cardinality * sizeof(int));
    pVt->maxSize = malloc(cardinality * sizeof(int));
    pVt->end = malloc(cardinality * sizeof(int));
	for (i = 0; i < cardinality; i++) {
        pVt->heap2[i] = malloc(sizeof(struct NodeBoth));
        //pVt->heap2[i]->first=INT_MAX;
        //pVt->heap2[i]->second=INT_MAX;
        pVt->heap[i] = malloc(sizeof(struct NodeSecond));
        //pVt->heap[i]->second=INT_MAX;
        pVt->filters[i] = -1;
        pVt->position[i] = 0;
        pVt->maxSize[i] = 0;
	}
	
	
	
	
	
	int propNo = 0;
	int inverse = 0;
	int filter = 0;
	int position = 5;
    
    pVt->filterVectors = malloc(sizeof(struct uint32_t*) * pVt->cardinality);
	for (i = 0; i < cardinality; i++) {
		sscanf(argv[position++], "%d", &propNo);
		sscanf(argv[position++], "%d", &inverse);
        
		if (inverse > 1) {
			sscanf(argv[position++], "%d", &filter);
			inverse = inverse - 2;
            pVt->position[i] = (propNo * 2) + inverse ;
			pVt->filters[i] = filter;
			pVt->existsFilter = 1;
            int iterPosition =  2 * i;
           // int partI2 = filter % (pVt->partitions);

			//pVt->currentPosition[i] = pVt->position[i] + partI2;
			int f = summaryBinarySearchSubject(pVt->position[i], filter,
					0,
					&(pVt->iters[iterPosition]));   
        

 if (f) {
//printf("found...\n");
        /*if(pVt->iters[iterPosition]){
            pVt->iters[iterPosition+1]=sizesGlobal[pVt->position[i]][pVt->iters[iterPosition]-1];
        }
        else{
            pVt->iters[iterPosition+1]=0;
        }*/
        pVt->end[i]=sizesGlobal[pVt->position[i]][pVt->iters[iterPosition]];
        //pCur->objects=&objectsGlobal[pVt->position][0];


        pVt->filterVectors[i]=objectsGlobal[pVt->position[i]];
            
		}
else{
pVt->filterVectors[i]=NULL;
}
}else{
            pVt->position[i] = (propNo * 2) + inverse ;
            pVt->filterVectors[i]=NULL;
        }

		
	}

	

	return SQLITE_OK;
}

static int unionwrapperDisconnect(sqlite3_vtab *pVtab) {
	unionwrapper_vtab *pVt = (unionwrapper_vtab*) pVtab;
	//printf("Real_time:\t%f\nProc_time:\t%f\nTotal flpins:\t%lld\nMFLOPS:\t\t%f\n",
	//real_time, proc_time, flpins, mflops);
	int i;
	//rb_iter_object_dealloc(pVt->iter);
	//rb_iter_subject_dealloc(pVt->iterOuter);

	free(pVt->iters);
	free(pVt->position);
	//free(pVt->currentPosition);
	free(pVt->maxSize);
	free(pVt->filters);
    for (i = 0; i < pVt->cardinality; i++) {
        free(pVt->heap[i]);
        free(pVt->heap2[i]);
	}
    free(pVt->heap);
	free(pVt->heap2);
	sqlite3_free(pVt);
	return SQLITE_OK;
}

static int unionwrapperOpen(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor) {

	unionwrapper_vtab *pVt = (unionwrapper_vtab*) pVTab;
	unionwrapper_cursor *pCur __attribute__((aligned(0x100000)));

    int i;
    for (i = 0; i < pVt->cardinality; i++) {
		int iterPos=i*2;
        if(pVt->iters[iterPos] && pVt->filters[i]>-1){
            pVt->iters[iterPos+1]=sizesGlobal[pVt->position[i]][pVt->iters[iterPos]-1];
        }
        else{
            pVt->iters[iterPos+1]=0;
        }

        pVt->heap2[i]->first=INT_MAX;
        pVt->heap2[i]->second=INT_MAX;
        pVt->heap[i]->second=INT_MAX;
}
	

	pCur = malloc(
			sizeof(*pCur) + pVt->cardinality * sizeof(struct uint32_t**));
	if (pCur == 0)
		return SQLITE_NOMEM;
	memset(pCur, 0, sizeof(*pCur));
	pCur->pVtab = pVt;
	// *ppCursor = &pCur->base;
	*ppCursor = (sqlite3_vtab_cursor*) &pCur->pVtab;
	//pCur->count = 0;

	return SQLITE_OK;
}

static int unionwrapperClose(sqlite3_vtab_cursor *cur) {

	unionwrapper_cursor *pCur = (unionwrapper_cursor*) cur;

    
    

	//pCur->o = 0;
    //int i;
    //for (i = 0; i < pVtab->cardinality; i++) {
   //     pVtab->heap2[i]->first=INT_MAX;
    //    pVtab->heap2[i]->second=INT_MAX;
   //     pVtab->heap[i]->second=INT_MAX;
	//}
	/* retval = PAPI_query_event(PAPI_TLB_DM);
	 if (retval != PAPI_OK) {
	 printf("PAPI_TLB_TL not available\n");
	 //test_skip(test_string);
	 }
	 printf("l2 acc.:%ld l2 miss: %ld l3 acc.:%ld l3 mis.:%ld tlb misses:%ld counter:%i binarySearches:%i \n", l2_access, l2_misses, l3_access, l3_misses, tlb_data_misses, papiCounter, binaryCounter);
	 */
//	printf("binary.:%ld  scan: %ld\n ", binaryCounter, scanCounter);	
	free(cur);
	return SQLITE_OK;
}

static int unionwrapperNext(sqlite3_vtab_cursor *cur) {

	unionwrapper_cursor *pCur = (unionwrapper_cursor*) cur;
	unionwrapper_vtab *pVtab = (unionwrapper_vtab *) pCur->pVtab;

	//pCur->count++;

	if (pCur->both == 2) {

		if (pVtab->existsFilter) {

			if (pVtab->finished == pVtab->cardinality) {
				pCur->eof = 1;
				return SQLITE_OK;
			}

			struct NodeSecond *root = pVtab->heap[0];

			while (root->second == pCur->s
					|| root->second % pVtab->partitions != pCur->shard) {
				if (root->nextsecond < pVtab->maxSize[root->pos]) {
					if (pCur->sv[root->pos]) {
						root->second =
								pCur->sv[root->pos][root->nextsecond];
					} else {

						root->second =
								subjectsGlobal[pVtab->position[root->pos]][root->nextsecond];
					}
					root->nextsecond++;

				}

				else {
					root->second = INT_MAX; //INT_MAX is for infinite
					pVtab->finished++;
					if (pVtab->finished == pVtab->cardinality) {
						pCur->eof = 1;
						return SQLITE_OK;
					}
				}
				MinHeapifySecond(0, pVtab->heap, pCur->heapsize);
				root = pVtab->heap[0];
			}

			pCur->s = root->second;
			if (root->nextsecond < pVtab->maxSize[root->pos]) {
				if (pCur->sv[root->pos]) {
					root->second =
							pCur->sv[root->pos][root->nextsecond];
				} else {
					root->second =
							subjectsGlobal[pVtab->position[root->pos]][root->nextsecond];
				}
				root->nextsecond++;

			}

			else {
				root->second = INT_MAX; //INT_MAX is for infinite
				pVtab->finished++;

			}

			// Replace root with next element of array
//        pVtab->heap[0] = root;
			MinHeapifySecond(0, pVtab->heap, pCur->heapsize);

		}

		else {
			//printf("finished: %i \n",  pVtab->finished);
			if (pVtab->finished == pVtab->cardinality) {
				pCur->eof = 1;
				return SQLITE_OK;
			}
			struct NodeBoth *root = pVtab->heap2[0];

			while (root->second == pCur->o && root->first == pCur->s) {
				//printf("same\n");
				if (root->nextsecond < pVtab->maxSize[root->pos]) {
					root->second =
							pCur->sv[root->pos][root->nextsecond];
					root->nextsecond++;

				} else if (noOfSubjectsGlobal[pVtab->position[root->pos]]
						> root->nextfirst) {
					root->nextsecond = 1;
					pCur->sv[root->pos] =
							&objectsGlobal[pVtab->position[root->pos]][root->nextfirst];


					root->second = pCur->sv[root->pos][0];
					root->first =
							subjectsGlobal[pVtab->position[root->pos]][root->nextfirst];
					root->nextfirst++;
				} else {
					pVtab->finished++;
					root->first = INT_MAX;
					if (pVtab->finished == pVtab->cardinality) {
						pCur->eof = 1;
						return SQLITE_OK;
					}
				}

				// Replace root with next element of array
				//pVtab->heap[0] = root;

				MinHeapifyBoth(0, pVtab->heap2, pCur->heapsize);
				root = pVtab->heap2[0];

			}

			pCur->o = root->second;
			pCur->s = root->first;

			if (root->nextsecond < pVtab->maxSize[root->pos]) {
				root->second = pCur->sv[root->pos][root->nextsecond];
				root->nextsecond++;

			} else if (noOfSubjectsGlobal[pVtab->position[root->pos]]
					> root->nextfirst) {
				root->nextsecond = 1;
				//printf("next first: %i\n", root->nextfirst);
				pCur->sv[root->pos] =
						&objectsGlobal[pVtab->position[root->pos]][root->nextfirst];


				root->second = pCur->sv[root->pos][0];
				//printf("second: %i\n", root->second);
				root->first =
						subjectsGlobal[pVtab->position[root->pos]][root->nextfirst];
				root->nextfirst++;
				//printf("new fiirts: %i \n", root->first);

			} else {
				//printf("current:%i \n", pVtab->currentPosition[root->pos]);
				pVtab->finished++;
				root->first = INT_MAX;
			}
			MinHeapifyBoth(0, pVtab->heap2, pCur->heapsize);

		}
	}

	else if (pCur->both) {
		pCur->eof = 1;
	} else {

		// Get the minimum element and store it in output

		//output[count] = root.element;

		// Find the next elelement that will replace current
		// root of heap. The next element belongs to same
		// array as the current root.

		if (pVtab->finished == pVtab->cardinality) {
			pCur->eof = 1;
			return SQLITE_OK;
		}

		struct NodeSecond *root = pVtab->heap[0];

		while (root->second == pCur->o
				|| (pCur->shard > -1
						&& root->second % pVtab->partitions != pCur->shard)) {
			if (root->nextsecond < pVtab->maxSize[root->pos]) {
				root->second = pCur->sv[root->pos][root->nextsecond];
				root->nextsecond++;

			}

			else {
				root->second = INT_MAX; //INT_MAX is for infinite
				pVtab->finished++;
				if (pVtab->finished == pVtab->cardinality) {
					pCur->eof = 1;
					return SQLITE_OK;
				}
			}
			MinHeapifySecond(0, pVtab->heap, pCur->heapsize);
			root = pVtab->heap[0];
		}

		pCur->o = root->second;
		if (root->nextsecond < pVtab->maxSize[root->pos]) {
			root->second = pCur->sv[root->pos][root->nextsecond];
			root->nextsecond++;

		}

		else {
			root->second = INT_MAX; //INT_MAX is for infinite
			pVtab->finished++;

		}

		// Replace root with next element of array
//        pVtab->heap[0] = root;
		MinHeapifySecond(0, pVtab->heap, pCur->heapsize);

	}

	return SQLITE_OK;
}

static int unionwrapperColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx,
		int i) {

	unionwrapper_cursor *pCur = (unionwrapper_cursor*) cur;
	if (i == 0)
		sqlite3_result_int(ctx, pCur->s);

	else
		sqlite3_result_int(ctx, pCur->o);
	return SQLITE_OK;
}

static int unionwrapperRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid) {
	//unionwrapper_cursor *pCur = (unionwrapper_cursor*) cur;
	// *pRowid = pCur->count;
	return SQLITE_OK;
}

static int unionwrapperEof(sqlite3_vtab_cursor *cur) {
	unionwrapper_cursor *pCur = (unionwrapper_cursor*) cur;
	return pCur->eof;
}

static int unionwrapperFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
		const char *idxStr, int argc, sqlite3_value **argv) {

	unionwrapper_cursor *pCur = (unionwrapper_cursor *) pVtabCursor;

	unionwrapper_vtab *pVtab = (unionwrapper_vtab *) pCur->pVtab;

	const uint32_t **subjects;
	const uint32_t  *subjectIds;

	if (idxNum == partition) {
		//only partition column is constrained
		pCur->both = 2;
		int partNo;
		partNo = sqlite3_value_int(argv[0]);
		pCur->shard = partNo;
		stick_this_thread_to_core(partNo);

		int input;
		int heapPos = 0;

		pVtab->finished = 0;

		if (pVtab->existsFilter) {
			for (input = 0; input < pVtab->cardinality; input++) {
				//pVtab->currentPosition[input] = pVtab->position[input] + partNo;
				int iterpos=input * 2;
				if (pVtab->filters[input] < 0) {
					if (noOfSubjectsGlobal[pVtab->position[input]]
							== 0) {
						//finished++;
						pVtab->finished++;
						continue;
					}
					subjectIds = subjectsGlobal[pVtab->position[input]];
					pCur->sv[input] = NULL;
					pVtab->maxSize[input] =
							noOfSubjectsGlobal[pVtab->position[input]];
					pVtab->iters[iterpos] = 1;

					uint32_t subjectId = subjectIds[0];
//printf("pos: %i value: %i \n", pVtab->currentPosition[input], subjectId);
					//; v2 = rb_iter_next(iter)) {
					//pVtab->iters[iterpos
					//		+ pVtab->partitions + partNo] = 1;

					//pCur->sv[input] = v2;

					//pVtab->heap[heapPos] = malloc(sizeof(struct NodeSecond));
					pVtab->heap[heapPos]->second = subjectId;

					pVtab->heap[heapPos]->pos = input;  // index of array

					pVtab->heap[heapPos]->nextsecond = 1;

					heapPos++;

				} else {

//printf("5\n");

					int partI2 = pVtab->filters[input] % (pVtab->partitions);
					//pVtab->currentPosition[input] = pVtab->position[input]
					//		+ partI2;
					
					//int iterPosition = pVtab->partitions * input * 2;
					int f = summaryBinarySearchSubject(
							pVtab->position[input], pVtab->filters[input], 0, &(pVtab->iters[iterpos]));
					
                            if (f) {
pCur->sv[input] = &objectsGlobal[pVtab->position[input]][0];
//printf("found...\n");
        if(pVtab->iters[iterpos]){
            pVtab->heap[heapPos]->nextsecond =sizesGlobal[pVtab->position[input]][pVtab->iters[iterpos]-1];
        }
        else{
           pVtab->heap[heapPos]->nextsecond =0;
        }
        pVtab->maxSize[input]=sizesGlobal[pVtab->position[input]][pVtab->iters[iterpos]];


					
						//pVtab->maxSize[input] = f->noOfObjects;
						//pVtab->heap[heapPos] = malloc(
						//		sizeof(struct NodeSecond));

						//pVtab->heap[heapPos]->nextsecond = 0;

						while (pCur->sv[input][pVtab->heap[heapPos]->nextsecond]
								% pVtab->partitions != partNo) {
							pVtab->heap[heapPos]->nextsecond++;
							if (pVtab->heap[heapPos]->nextsecond
									== pVtab->maxSize[input]) {
								pVtab->finished++;
								pCur->sv[input] = NULL;
								break;
							}

						}
						if (pVtab->heap[heapPos]->nextsecond < pVtab->maxSize[input]) {
							pVtab->heap[heapPos]->second =
									pCur->sv[input][pVtab->heap[heapPos]->nextsecond];
							pVtab->heap[heapPos]->pos = input; // index of array
							// printf("next value: %i next pos: %i \n", pVtab->heap[heapPos]->second, pVtab->heap[heapPos]->nextsecond);
							pVtab->heap[heapPos]->nextsecond++;

							heapPos++;
						}

					} else {
                        pCur->sv[input] = NULL;
						pVtab->finished++;
					}

				}

//for input
			}

			pCur->heapsize = pVtab->cardinality - pVtab->finished;
			//MinHeapSecond(pCur->heapsize, pVtab->heap); // Create the heap

			// Now one by one get the minimum element from min
			// heap and replace it with next element of its array

			if (pVtab->finished < pVtab->cardinality) {
				MinHeapSecond(pCur->heapsize, pVtab->heap);
				// Get the minimum element and store it in output
				struct NodeSecond *root = pVtab->heap[0];
				//output[count] = root.element;
				//printf("next element: %i \n", root->second);

				pCur->o = 0;
				pCur->s = root->second;
				if (root->nextsecond < pVtab->maxSize[root->pos]) {
					if (pCur->sv[root->pos]) {
						root->second =
								pCur->sv[root->pos][root->nextsecond];
					} else {
						root->second =
								subjectsGlobal[pVtab->position[root->pos]][root->nextsecond];
					}
					root->nextsecond++;

				} else {
					pVtab->finished++;
					root->second = INT_MAX;
				}

				// Replace root with next element of array
				//pVtab->heap[0] = root;
				MinHeapifySecond(0, pVtab->heap, pCur->heapsize);
			} else {
				pCur->eof = 1;
			}

			return SQLITE_OK;

//if exists filter
		} else {

//printf("2\n");

			for (input = 0; input < pVtab->cardinality; input++) {
				//pVtab->currentPosition[input] = pVtab->position[input] + partNo;
				if (noOfSubjectsGlobal[pVtab->position[input]] == 0) {
					//finished++;
					//printf("fin\n");
					pVtab->finished++;
					continue;
				}
				//printf("current:%i \n", pVtab->currentPosition[input]);
				int iterpos=input * 2;
				subjects = &objectsGlobal[pVtab->position[input] ];
				subjectIds = subjectsGlobal[pVtab->position[input] ];

				pVtab->iters[iterpos] = 1;
				const uint32_t *v2 =  subjects[0];
				const uint32_t subjectId = subjectIds[0];
				//; v2 = rb_iter_next(iter)) {
				//pVtab->iters[iterpos] = 1;

				pCur->sv[input] = v2;
				//pVtab->currentPosition[heapPos] = pVtab->currentPosition[input];
				//pVtab->heap2[heapPos] = malloc(sizeof(struct NodeBoth));
				pVtab->heap2[heapPos]->first = subjectId;
 
				pVtab->heap2[heapPos]->second = pCur->sv[input][0];
				//printf("first: %i second: %i v2: %i current:%i\n", pVtab->heap2[heapPos]->first, pVtab->heap2[heapPos]->second, v2->objects[0], pVtab->currentPosition[input]);
				pVtab->heap2[heapPos]->pos = input;  // index of array

				pVtab->heap2[heapPos]->nextsecond = 1;
				pVtab->heap2[heapPos]->nextfirst = 1;
				heapPos++;
			}
			pCur->heapsize = pVtab->cardinality - pVtab->finished;
			//MinHeapSecond(pCur->heapsize, pVtab->heap); // Create the heap

			// Now one by one get the minimum element from min
			// heap and replace it with next element of its array

			if (pVtab->finished < pVtab->cardinality) {
				MinHeapBoth(pCur->heapsize, pVtab->heap2);
				// Get the minimum element and store it in output
				struct NodeBoth *root = pVtab->heap2[0];
				//output[count] = root.element;
				//printf("next element: %i \n", root->second);
				// Find the next elelement that will replace current
				// root of heap. The next element belongs to same
				// array as the current root.
				pCur->o = root->second;
				pCur->s = root->first;
				if (1 < sizesGlobal[pVtab->position[root->pos]][pVtab->iters[root->pos * 2]]) { 
					root->nextsecond = 2;
					root->second = pCur->sv[root->pos][1];
				} else if (noOfSubjectsGlobal[pVtab->position[root->pos]]
						> 1) {
					root->nextsecond = 1;
					root->nextfirst = 2;
					pCur->sv[root->pos] =
							&objectsGlobal[pVtab->position[root->pos]][1];
					root->second = pCur->sv[root->pos][1];
					root->first = subjectsGlobal[pVtab->position[root->pos]][1];
				} else {
					pVtab->finished++;
					root->first = INT_MAX;
				}

				// Replace root with next element of array
				//pVtab->heap[0] = root;
				MinHeapifyBoth(0, pVtab->heap2, pCur->heapsize);
			} else {
				pCur->eof = 1;
			}
		}

		return SQLITE_OK;
	}

	uint32_t subject = sqlite3_value_int(argv[0]);
	int partI = subject % (pVtab->partitions);
	//pCur->i = subject % (pVtab->partitions);
	//pCur->i = subject & 0x00000001	;
//if (idxNum & 0x80000000) {
	int shard;
	shard = -1;
	if (idxNum & partition) {
		//printf("in\n");
		//if(idxNum<0){
		int partition;
		if (idxNum & secondShard) {
			//printf("shard\n");
			partition = sqlite3_value_int(argv[argc - 1]);

			shard = sqlite3_value_int(argv[argc - 2]);
			stick_this_thread_to_core(shard);
			//printf("sticked\n");
		} else {
//printf("not shard\n");
			partition = sqlite3_value_int(argv[argc - 1]);
			stick_this_thread_to_core(partition);
		}
		//printf("partition:%i\n", partition);
		if (partition != partI) {
			pCur->eof = 1;
			return SQLITE_OK;
		}
	}
pCur->shard = shard;
	//int pstion = 0;
	//for (pstion = 0; pstion < pVtab->cardinality; pstion++) {
	//	pVtab->currentPosition[pstion] = pVtab->position[pstion] + partI;

	//}
//printf("before\n");
//int start=pVtab->currentPosition*SUMMARYSIZE;
//=summariesGlobal[pVtab->currentPosition];
//printf("after\n");	
// pCur->s = sqlite3_value_int64(argv[0]);
	// pCur->i = (sqlite3_value_int64(argv[0])%(pVtab->partitions));
	//pCur->i =pCur -> s & 0x00000003	;
	pCur->eof = 0;
	//printf("seraching... position: %i\n", pVtab->currentPosition);
//int asasa=noOfSubjectsGlobal[pVtab->currentPosition];
//printf("asasa: %i \n", (pVtab->iters[partI]+1));
	/*papiCounter++;
	 if(!(papiCounter%31)){
	 retval=PAPI_start_counters(events, numEvents);
	 if(retval != PAPI_OK)
	 printf("error papi: %i\n", retval);}*/
//struct subjectVector *f=interpolationSearch(subjects, subject, subjectIds, noOfSubjectsGlobal[pVtab->currentPosition], &(pVtab->iters[partI]), windowSizeGlobal[pVtab->currentPosition]);
//printf("after seraching\n");	
//struct subjectVector *f= binarySearchSubject(subjects, subject, subjectIds, 0, noOfSubjectsGlobal[pVtab->currentPosition], &(pVtab->iters[partI]));
//struct subjectVector *f= scanSubject(subjects, subject, subjectIds, noOfSubjectsGlobal[pVtab->currentPosition], &(pVtab->iters[partI]));
//printf("going to search\n");
//struct subjectVector *f= binarySearchSubject(subjects, subject, subjectIds, 0, noOfSubjectsGlobal[pVtab->currentPosition], &(pVtab->iters[partI]));
//printf("current:%i\n", pVtab->iters[partI]);
//__builtin_prefetch(&summariesIds[0]);
	int f;
	pCur->s = subject;
	if (idxNum & second) {
		//both s and o are bound
		pCur->o = sqlite3_value_int(argv[1]);
		pCur->both = 1;
		int input;

		for (input = 0; input < pVtab->cardinality; input++) {
            //pVtab->currentPosition[input] = pVtab->position[input] + partI;
			
			int iterpos=input * 2;
			f = summaryBinarySearchSubject(pVtab->position[input], subject, 0, 	&(pVtab->iters[iterpos]));
			if (f) {
				int itersecond=iterpos+ 1 ;
				//if (pVtab->iters[itersecond] >= sizesGlobal[pVtab->position[input]][pVtab->iters[iterpos]]) {
                    if(pVtab->iters[iterpos]){
            pVtab->iters[itersecond]=sizesGlobal[pVtab->position[input]][pVtab->iters[iterpos]-1];
        }
        else{
            pVtab->iters[itersecond]=0;
        }

				//}
			/*	int distance = pCur->o
						- subjectsGlobal[pVtab->position[input]][itersecond];

				if (distance < 1000 && distance > -1000) {
					//printf("current out: %i  subsrc:%i \n", f->objects[pVtab->iters[pVtab->partitions+partI]], pVtab->iters[pVtab->partitions+partI]);
					if (scanObject(pCur->o, subjectsGlobal[pVtab->position[input]], sizesGlobal[pVtab->position[input]][pVtab->iters[iterpos]],
							&(pVtab->iters[itersecond]))) {
						return SQLITE_OK;
					}
				} else {*/

					int left, right, mid;
					/* Initializations */
					left = pVtab->iters[itersecond];
					right = sizesGlobal[pVtab->position[input]][pVtab->iters[iterpos]] - 1;
                    //printf("left: %i right: %i \n", left, right);
					while (left <= right) {
						//binary seacrh on objects
						mid = (left + right) / 2;

						if (pCur->o == objectsGlobal[pVtab->position[input]][mid]) {
							//element found
                            //printf("found \n");

							pVtab->iters[itersecond] = mid;
							return SQLITE_OK;
						} else if (pCur->o > objectsGlobal[pVtab->position[input]][mid]) {
							left = mid + 1;
						} else {
							right = mid - 1;
						}
					}

				//}
			}
//end for input
		}

		pCur->eof = 1;

		return SQLITE_OK;
	}
	int input;
	pVtab->finished = 0;
    if(pVtab->existsFilter){
        pCur->both = 1;
    }
    else{
        pCur->both = 0;
    }
    int iterPosition=0;
	for (input = 0; input < pVtab->cardinality; input++) {
//printf("8 input:%i \n", input);
//int iterPosition=pVtab->partitions*2*input+pVtab->partitions; 
        //pVtab->currentPosition[input] = pVtab->position[input] + partI;
		//int iterPosition = pVtab->partitions * 2 * input;
		
		if (pVtab->filters[input] < 0) {
//printf("9\n");
			f = summaryBinarySearchSubject(pVtab->position[input], subject, 0, &(pVtab->iters[iterPosition]));
//printf("10\n");
			if (f && pCur->both == 1) {
//printf("11\n");
				pCur->o = subject;
				return SQLITE_OK;
			}

		} else {
//printf("12\n");
			
//rdf type search


            //f=pVtab->filterVectors[input];
			if (pVtab->filterVectors[input]) {


/*
int distance = subject
					- pVtab->filterVectors[input][pVtab->iters[iterPosition + 1]];

			if (distance < 1000 && distance > -1000) {
				//printf("current out: %i  subsrc:%i \n", f->objects[pVtab->iters[pVtab->partitions+partI]], pVtab->iters[pVtab->partitions+partI]);
				if (scanObject(subject, pVtab->filterVectors[input], pVtab->end[input],
						&(pVtab->iters[iterPosition + 1]))) {
pCur->o = subject;
					return SQLITE_OK;
				}

			} else {*/

				int left, right, mid;
				/* Initializations */
				left = pVtab->iters[iterPosition+1];
				right = sizesGlobal[pVtab->position[input]][pVtab->iters[iterPosition]] - 1;

				while (left <= right) {
					//binary seacrh on objects
					mid = (left + right) / 2;

					if (subject == pVtab->filterVectors[input][mid]) {
						//element found
                            pCur->o = subject;
						//pVtab->iters[iterPosition + 1] = mid;
						return SQLITE_OK;
					} else if (subject > pVtab->filterVectors[input][mid]) {
						left = mid + 1;
					} else {
						right = mid - 1;
					}
				}

//}
f=0;
//printf("mid:: %d\n", mid);
//pVtab->iters[pVtab->partitions+partI]=mid;
			//}
//printf("13\n");
//printf("obj0: %i\n", f->objects[0]);
//printf("no of obj: %i \n",  f->noOfObjects);
//printf("iter no: %i \n", input*pVtab->cardinality+pVtab->partitions+partI2);
//printf("iter: %i \n", pVtab->iters[input*pVtab->partitions*2+pVtab->partitions+partI2]);
				/*pVtab->iters[iterPosition + pVtab->partitions + partI2]=0;
				if (scanObject(subject, f->objects, f->noOfObjects,
						&(pVtab->iters[iterPosition
								+ pVtab->partitions + partI2]))) {

					// pCur->s=subject;
					pCur->o = subject;
					return SQLITE_OK;
				} else {
//printf("14\n");
					f = NULL;
				}*/
			}
//printf("15\n");
		}

		
		if (!f) {
			pVtab->finished++;
            iterPosition+= 2;
            pCur->sv[input]=NULL;
			continue;
		}
        else{
            pCur->sv[input] = objectsGlobal[pVtab->position[input]];
            if(pVtab->iters[iterPosition]){
            pVtab->iters[iterPosition+1] =sizesGlobal[pVtab->position[input]][pVtab->iters[iterPosition]-1];
        }
        else{
            pVtab->iters[iterPosition+1] =0;
        }
        pVtab->maxSize[input]=sizesGlobal[pVtab->position[input]][pVtab->iters[iterPosition]];
        }

		/*if (shard>-1){
		 //printf("shard>-1\n");
		 //only s is bound, iterate over shard of objects
		 int shardSize;
		 shardSize=(f->noOfObjects+pVtab->partitions-1)/pVtab->partitions;
		 pVtab->iters[iterPosition+pVtab->partitions+partI]=shardSize*shard;
		 pVtab->maxSize[input]=pVtab->iters[iterPosition+pVtab->partitions+partI]+shardSize;
		 if(pVtab->maxSize[input]>f->noOfObjects){
		 pVtab->maxSize[input]=f->noOfObjects;
		 }
		 if(pVtab->iters[iterPosition+pVtab->partitions+partI]>f->noOfObjects-1){
		 pCur->sv[input]=NULL;
		 pVtab->finished++;
		 pVtab->iters[iterPosition+pVtab->partitions+partI]=0;
		 }




		 }
		 else{
		 */
		//pCur->both = 0;
		//pVtab->iters[iterPosition + pVtab->partitions + partI] = 0;
		//pVtab->maxSize[input] = f->noOfObjects;
		//	}

		
        iterPosition+= 2;
	}
    if(pVtab->existsFilter){
		pCur->eof = 1;
        return SQLITE_OK;
	}
    //printf("filteri...\n");
	int heapPos = 0;
	for (input = 0; input < pVtab->cardinality; input++) {
		if (!pCur->sv[input]) {
			//finished++;

			continue;
		}
		/*if(pCur->both==1){
		 pCur->s=subject;
		 pCur->o=subject;
		 return SQLITE_OK;
		 }*/
		//pVtab->heap[heapPos] = malloc(sizeof(struct NodeSecond));

		pVtab->heap[heapPos]->second = pCur->sv[input][pVtab->iters[(input*2)+1]];
		pVtab->heap[heapPos]->pos = input;  // index of array

		pVtab->heap[heapPos]->nextsecond = pVtab->iters[(input*2)+1]+1;
		heapPos++;
	}
	pCur->heapsize = pVtab->cardinality - pVtab->finished;
	//MinHeapSecond(pCur->heapsize, pVtab->heap); // Create the heap

	// Now one by one get the minimum element from min
	// heap and replace it with next element of its array

	if (pVtab->finished < pVtab->cardinality) {
//printf("20\n");
		MinHeapSecond(pCur->heapsize, pVtab->heap);
		// Get the minimum element and store it in output
		struct NodeSecond *root = pVtab->heap[0];
		//output[count] = root.element;
		//printf("next element: %i \n", root->second);
		// Find the next elelement that will replace current
		// root of heap. The next element belongs to same
		// array as the current root.
		while (pCur->shard > -1
				&& root->second % pVtab->partitions != pCur->shard) {
			if (root->nextsecond < pVtab->maxSize[root->pos]) {
				root->second = pCur->sv[root->pos][root->nextsecond];
				root->nextsecond++;
			} else {
				pVtab->finished++;
				root->second = INT_MAX;
				if (pVtab->finished == pVtab->cardinality) {
					pCur->eof = 1;
					return SQLITE_OK;
				}
			}

			// Replace root with next element of array
			//pVtab->heap[0] = root;
			MinHeapifySecond(0, pVtab->heap, pCur->heapsize);
			root = pVtab->heap[0];
		}

		pCur->o = root->second;
		if (root->nextsecond < pVtab->maxSize[root->pos]) {
			root->second = pCur->sv[root->pos][root->nextsecond];
			root->nextsecond++;

		} else {
			pVtab->finished++;
			root->second = INT_MAX;
		}

		// Replace root with next element of array
		//pVtab->heap[0] = root;
		MinHeapifySecond(0, pVtab->heap, pCur->heapsize);
	} else {
		pCur->eof = 1;
	}

	return SQLITE_OK;
}

static int unionwrapperBestIndex(sqlite3_vtab *tab,
		sqlite3_index_info *pIdxInfo) {

	int i;

	int col = 0;
	//int inequality = 1;
	pIdxInfo->idxNum = 0;

	pIdxInfo->estimatedCost = 1000;
	pIdxInfo->estimatedRows = 1000;
	const struct sqlite3_index_constraint *pConstraint;
	pConstraint = pIdxInfo->aConstraint;

	for (i = 0; i < pIdxInfo->nConstraint; i++, pConstraint++) {
		if (pConstraint->usable == 0)
			continue;
		pIdxInfo->idxNum |= 1 << pConstraint->iColumn;
		// printf("contrained iCol:%i\n", pConstraint->iColumn);
		if (pConstraint->iColumn == 2) {
			//partiton
			//scan whole table
			//inequality = -1;
			int j;
			int notUsedConstraints;
			notUsedConstraints = 0;
			int shard = -1;
			const struct sqlite3_index_constraint *pConstraint2;
			pConstraint2 = pIdxInfo->aConstraint;
			for (j = 0; j < pIdxInfo->nConstraint; j++, pConstraint2++) {
				//count not used contraints
				if (pConstraint2->usable == 0) {
					notUsedConstraints++;
					//printf("not usable:%i \n", j);
				}

				if (pConstraint->iColumn == 3) {
					//scan only specific shard
					shard = j;
					//notUsedConstraints--;
				}

			}

			pIdxInfo->aConstraintUsage[i].argvIndex = pIdxInfo->nConstraint
					- notUsedConstraints;
			pIdxInfo->aConstraintUsage[i].omit = 1;

			if (shard > -1) {
				//printf("shard:%i \n", shard);
				pIdxInfo->aConstraintUsage[shard].argvIndex =
						pIdxInfo->nConstraint - notUsedConstraints - 1;
				pIdxInfo->aConstraintUsage[shard].omit = 1;
			}

			continue;
			//pIdxInfo->idxNum =4;
			//return SQLITE_OK;
		}

		//col+=pConstraint->iColumn+1;
		col++;
		pIdxInfo->estimatedCost -=30;
		if (pConstraint->iColumn == 0) {
			pIdxInfo->aConstraintUsage[i].argvIndex = 1;
		} else {
			pIdxInfo->aConstraintUsage[i].argvIndex = 2;
		}
		pIdxInfo->aConstraintUsage[i].omit = 1;
	}
	//0 scan table specific partition
	//1 only s
	//2 both s and o
	//-1 only s specific partition
	//-2 both s and o specific partition
	//pIdxInfo->idxNum = col * inequality;

	return SQLITE_OK;

}

/*
 ** A virtual table module that provides read-only access to a
 ** Tcl global variable namespace.
 */
static sqlite3_module unionwrapperModule = {  0, /* iVersion */

unionwrapperConnect, unionwrapperConnect, unionwrapperBestIndex,
		unionwrapperDisconnect, unionwrapperDisconnect, unionwrapperOpen, /* xOpen - open a cursor */
		unionwrapperClose, /* xClose - close a cursor */
		unionwrapperFilter, /* xFilter - configure scan constraints */
		unionwrapperNext, /* xNext - advance a cursor */
		unionwrapperEof, /* xEof - check for end of scan */
		unionwrapperColumn, /* xColumn - read data */
		unionwrapperRowid, /* xRowid - read data */
		0, /* xUpdate */
		0, /* xBegin */
		0, /* xSync */
		0, /* xCommit */
		0, /* xRollback */
		0, /* xFindMethod */
		0, /* xRename */
};

#endif /* SQLITE_OMIT_VIRTUALTABLE */

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_unionwrapper_init(sqlite3 *db, char **pzErrMsg,
		const sqlite3_api_routines *pApi) {
	int rc = SQLITE_OK;
	SQLITE_EXTENSION_INIT2(pApi);
#ifndef SQLITE_OMIT_VIRTUALTABLE
	rc = sqlite3_create_module(db, "unionwrapper", &unionwrapperModule, 0);
#endif
	return rc;
}

/************** End of unionwrapper.c ************************************************/


int sqlite3_parj_init(sqlite3 *db, char **pzErrMsg,
		const sqlite3_api_routines *pApi) {
	int rc = SQLITE_OK;
	SQLITE_EXTENSION_INIT2(pApi);
#ifndef SQLITE_OMIT_VIRTUALTABLE
	rc = sqlite3_create_module(db, "parj", &parjModule, 0);
	rc = sqlite3_create_module(db, "memorywrapper", &memorywrapperModule, 0);
        rc = sqlite3_create_module(db, "dictionary", &dictionaryModule, 0);
	rc = sqlite3_create_module(db, "unionwrapper", &unionwrapperModule, 0);
	rc = sqlite3_create_module(db, "stat", &statModule, 0);
#endif
	return rc;
}

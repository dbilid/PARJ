#ifndef SQLITE_OMIT_VIRTUALTABLE
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#define _GNU_SOURCE
#include <inttypes.h>
//#include <raptor2/raptor2.h>
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
	int threads, inverse;
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

static int * __restrict__ sizesOfFirstArrays;// __attribute__((aligned(0x1000000)));
static int * __restrict__ windowSizeGlobal;// __attribute__((aligned(0x1000000)));
static const uint32_t ** __restrict__ firstArrays;
//each first array holds the distinct subjects of a S-O table, or distinct objects
//of a O-S table
static const uint32_t ** __restrict__ secondArrays;
//each second array holds all the objects of a S-O table, or all
//the subjects of a O-S table. The offsets corresponding to each
//element of the first array of the table are given from positionsAtSecondArray
//for example we have the S-O table with tuples: (2, 5), (2, 6), (2,7), (3, 4), (3, 10)
//The first array is: [2, 3]
//the second array is: [5, 6, 7, 4, 10]
//the positionsAtSecondArray is [3, 5], denoting that the objects corresponding to second subject
//(value 3) start at position 3 of the second array and end at position (5-1)=4 of the second array
//(a value 0 at the beginning of positionsAtSecondArray is ommited
static const uint32_t ** __restrict__ positionsAtSecondArray;

static uint32_t maxId=0;
//static unsigned long binaryCounter1=0L;
//static unsigned long scanCounter=0L;
static const uint32_t ** __restrict__ bitmapsGlobal;
//static uint32_t **bitmapsValuesGlobal;
pthread_mutex_t mutexload;

int loadMemoryData(sqlite3 *, int, int);
int stick_this_thread_to_core(int);
static inline  int adaptiveSubjectSearch(
		int, const uint32_t, const int, int*);
static inline  int scanSubject(
		int, uint32_t, int*);
static inline  int bitmapSearch(
		int, uint32_t, int*);
static inline  int binarySubjectSearch(
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
			pCur->result[i+2]=firstArrays[index][0]; //min
			pCur->result[i+3]=firstArrays[index][sizesOfFirstArrays[index]-1]; //max
			pCur->result[i+4]=sizesOfFirstArrays[index]; //distinct
			pCur->result[i+5]=positionsAtSecondArray[index][sizesOfFirstArrays[index]-1]; //all
			index++; //inverse
			pCur->result[i+6]=j;
			pCur->result[i+7]=1;
			pCur->result[i+8]=firstArrays[index][0]; //min
			pCur->result[i+9]=firstArrays[index][sizesOfFirstArrays[index]-1]; //max
			pCur->result[i+10]=sizesOfFirstArrays[index]; //distinct
			pCur->result[i+11]=positionsAtSecondArray[index][sizesOfFirstArrays[index]-1]; //all

		}		

	}
	else if(mode==1){
		int index = (sqlite3_value_int(argv[1])*2)+1;
		//printf("index: %i  \n", index);
		pCur->result= sqlite3_malloc(sizeof(uint64_t) * sizesOfFirstArrays[index] * 4);
		pCur->size=sizesOfFirstArrays[index] * 4;
		//printf("noOfSubj: %i \n", sizesOfFirstArrays[index]);
		int previous=0;
		pCur->counter=1;
		pCur->value=firstArrays[index][0];
		int i;
		for(i=0;i<sizesOfFirstArrays[index];i++){
			int j=i*4;
			pCur->result[j]=firstArrays[index][i];
			//printf("next o: %i \n", pCur->result[j]);
			if(i==0){
				pCur->result[j+1]=secondArrays[index][0];
			}else{
				pCur->result[j+1]=secondArrays[index][positionsAtSecondArray[index][i-1]];
			}
			pCur->result[j+2]=secondArrays[index][positionsAtSecondArray[index][i]-1];
			pCur->result[j+3]=positionsAtSecondArray[index][i]-previous;
			//printf("value: %i \n", secondArrays[index][positionsAtSecondArray[index][i]-1]);
			//printf("min: %i, max: %i,diff: %i \n", pCur->result[j+1], pCur->result[j+2], pCur->result[j+3]); 
			previous=positionsAtSecondArray[index][i];
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
			int f1=adaptiveSubjectSearch(typeIndex, -prop1, 0, &start);
			int f2=adaptiveSubjectSearch(typeIndex, -prop2, 0, &start2);
			if(f1 && f2){
				int startPos=0;
				if(start>0){
					startPos=positionsAtSecondArray[typeIndex][start-1];
				}
				int endPos=positionsAtSecondArray[typeIndex][start];
				int startPos2=0;
				if(start2>0){
					startPos2=positionsAtSecondArray[typeIndex][start2-1];
				}
				int endPos2=positionsAtSecondArray[typeIndex][start2];
				int k=startPos;
				int j=startPos2;
				while(k<endPos && j<endPos2 && pCur->result[0]<STATLIMIT){
					if(secondArrays[typeIndex][k]==secondArrays[typeIndex][j]){
						pCur->result[0]++;
						k++;
						j++;
					}
					else if (secondArrays[typeIndex][k]<secondArrays[typeIndex][j]){
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
		//if(sizesOfFirstArrays[index1]>10000){
		//	step=sizesOfFirstArrays[index1]/10000;
		//}
		//printf("step:%i \n", step);
		int counter=1;
		int pos=0;
		int i;
		for(i=0;i<sizesOfFirstArrays[index1] && pCur->result[0]<STATLIMIT;i+=step){
			//printf("i: %i to find: %i index1 : %i index2 :%i\n", i, firstArrays[index1][i], index1, index2);
			counter++;
			int f=adaptiveSubjectSearch(index2, firstArrays[index1][i], 0, &pos);
			if(f){
				//printf("pos: %i \n", pos);
				int startPos2=0;
				if(pos>0){
					startPos2=positionsAtSecondArray[index2][pos-1];
				}
				int endPos2=positionsAtSecondArray[index2][pos];
				int startPos1=0;
				if(i>0){
					startPos1=positionsAtSecondArray[index1][i-1];
				}
				int endPos1=positionsAtSecondArray[index1][i];
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
	int f1=adaptiveSubjectSearch(typeIndex, -type, 0, &start);
	uint64_t result=0;

	if(f1){
		int startPos=0;
		int pos=0;

		if(start>0){
			startPos=positionsAtSecondArray[typeIndex][start-1];
		}
		int endPos=positionsAtSecondArray[typeIndex][start];
		int k=startPos;

		if(start>0){
			startPos=positionsAtSecondArray[typeIndex][start-1];
		}

		int temp=adaptiveSubjectSearch(propIndex, secondArrays[typeIndex][k], 0, &pos);

		while(k<endPos && result<STATLIMIT){
			k++;
			int f2=adaptiveSubjectSearch(propIndex, secondArrays[typeIndex][k], 0, &pos);
			if(f2){
				//printf("pos: %i \n", pos);
				int startPos2=0;
				if(pos>0){
					startPos2=positionsAtSecondArray[propIndex][pos-1];
				}
				int endPos2=positionsAtSecondArray[propIndex][pos];
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


/**************** start memorywrapper.c ***********/

typedef struct memorywrapper_vtab memorywrapper_vtab;
struct memorywrapper_vtab {
	sqlite3_vtab base; /* Base class - must be first */
	//char* zSql[0];
	uint16_t threads;
	uint16_t inverse;
    //inverse is 0 for S-O tables and 1 for O-S tables
	int position;
    //position of the array that corresponds to this table in the firstArrays
    //for example position of S-O table for prop10 is 20, whereas position of
    //the O-S table for prop10 is 21. See memorywrapperConnect for how this is set
	int maxSize;
	int iter0;
	int iter1;
	int pad[9];

};

/* A memorywrapper cursor object */
typedef struct memorywrapper_cursor memorywrapper_cursor;
struct memorywrapper_cursor {
	memorywrapper_vtab* pVtab;
	const uint32_t * objects;

	uint32_t o;
	uint32_t s;
	int end;
	int both;
	//0->only s, 1->both s, o, 2-> scan
	int eof;

};

//float real_time, proc_time, mflops;
//  long long flpins;
//  int retval;

/*
 * memorywrapperConnect is used to create a memory table for a specific propNo and
 * table type (S-O or O-S).
 * argv[3] gives the number of threads that will be used, argv[4] gives the property number and argv[5] is
 * set to 0 for S-O tables and is set to 1 for O-S tables
 * For example, the sqlite command: "create virtual table memorywrapperprop10 using memorywrapper(32, 10, 0)"
 * calls memorywrapperConnect with argv[3]=32, argv[4]=10 and argv[5]=0 and creates a memory table that
 * corresponds to the S-O table from property 10 and for 32 threads.
 * Special value <0 for property number denotes that data loading into main memory should be performed (no
 * virtual table is created). A single call with property number <0 must occur before any memory table is created.
*/
static int memorywrapperConnect(sqlite3 *db, void *pAux, int argc,
		const char * const *argv, sqlite3_vtab **ppVtab, char **pzErr) {
	int thrd;

	sscanf(argv[3], "%d", &thrd);
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
    /* first: value on the first array (S for S-O tables or O for O-S tables)
     * second: value on the second array
     * partition: used in leftmost tables to denote a specific partition corresponding to thread
     * secondShard: used in leftmost tables when additionally a filter exists to denote a shard
     * on the second array that corresponds to thread
    */
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

	pVt->threads = thrd;

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
	*ppCursor = (sqlite3_vtab_cursor*) &pCur->pVtab;

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

	if (pCur->both == 2) {
    //scan whole table portion (both first and second arrays)
		if (pCur->end
				> pVtab->iter1) {
            //continue scanning the second array
			pCur->o = pCur->objects[pVtab->iter1++];
		} else {
            //given second array has finished 
			if (sizesOfFirstArrays[pVtab->position]
					> pVtab->iter0) {
                //move to the next item in first array and first item on 
                //corresponding second array
				pVtab->iter1 = positionsAtSecondArray[pVtab->position][pVtab->iter0-1];
				pCur->end = positionsAtSecondArray[pVtab->position][pVtab->iter0];				
				pCur->s = firstArrays[pVtab->position][pVtab->iter0];
				pCur->o = pCur->objects[pVtab->iter1++];
				pVtab->iter0+=pVtab->threads;

			} else {
                //there are no more elements in first array. Scan finished
				pVtab->iter0 = 0;
				pCur->eof = 1;
			}

		}
	}

	else if (pCur->both) {
        //both first and second values are bound. there is no next value
		pCur->eof = 1;
	} else {
        //only first is bound. Scan the second array for the given first
        //check if there are more seconds to output
		if (pVtab->maxSize
				> pVtab->iter1) {
			//printf("next single");

			pCur->o = pCur->objects[pVtab->iter1++];
		}

		else {
            //scanning of second array has finished.
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

const unsigned char first = 1;
const unsigned char second = 1 << 1;
const unsigned char partition = 1 << 2;
const unsigned char secondShard = 1 << 3;


static int memorywrapperFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
		const char *idxStr, int argc, sqlite3_value **argv) {

	memorywrapper_cursor *pCur = (memorywrapper_cursor *) pVtabCursor;
	memorywrapper_vtab *pVtab = (memorywrapper_vtab *) pCur->pVtab;
    //idxNum has been set from memorywrapperBestIndex. 
    //the first bit is set if first column is bound,
    //the second bit if second column is bound,
    //the third bit if partition is bound and
    //the fourth bit if secondShard is bound
	if (idxNum == partition) {
		//only partition column is constrained
        //scan the whole table portion (both first and second arrays)
		pCur->both = 2;
		int partNo;
		partNo = sqlite3_value_int(argv[0]);
		stick_this_thread_to_core(partNo);

		pCur->eof = 1;

		pVtab->iter0=partNo;

		if (pVtab->iter0 >sizesOfFirstArrays[pVtab->position] - 1) {
			pCur->eof = 1;
			pVtab->iter1 = 0;
		}



		else {

			if(pVtab->iter0){
				pVtab->iter1=positionsAtSecondArray[pVtab->position][pVtab->iter0-1];
			}
			else{
				pVtab->iter1=0;
			}
			pCur->end=positionsAtSecondArray[pVtab->position][pVtab->iter0];
			pCur->objects = &secondArrays[pVtab->position][0];
			pCur->s = firstArrays[pVtab->position][pVtab->iter0];
			pCur->o = pCur->objects[ pVtab->iter1++];
			pCur->eof = 0;
			pVtab->iter0+=pVtab->threads;

		}

		return SQLITE_OK;
	}

    //in the following code in this method we know that
    //first column is contrained

	uint32_t subject = sqlite3_value_int(argv[0]);

	int shard;
	shard = -1;

    //first check if partition and secondShard are also
    //contrained
	if (idxNum & partition) {
		int partition;
		if (idxNum & secondShard) {

			partition = sqlite3_value_int(argv[argc - 1]);

			shard = sqlite3_value_int(argv[argc - 2]);
			stick_this_thread_to_core(shard);

		} else {

			partition = sqlite3_value_int(argv[argc - 1]);
			stick_this_thread_to_core(partition);
		}

	}

	pCur->eof = 0;
    //seacrh for the specific contrained value in the first array
	int f = adaptiveSubjectSearch(pVtab->position, subject, 0,
			&(pVtab->iter0));

	if (f) {

		if(pVtab->iter0){
			pVtab->iter1=positionsAtSecondArray[pVtab->position][pVtab->iter0-1];
		}
		else{
			pVtab->iter1=0;
		}
		pCur->end=positionsAtSecondArray[pVtab->position][pVtab->iter0];
		pCur->objects=&secondArrays[pVtab->position][0];

		pCur->s = subject;

		if (idxNum & second) {
			//Second value is also contrained
            //use binary search in the second array
            //to search for the specific value
			pCur->o = sqlite3_value_int(argv[1]);
			pCur->both = 1;
			int left, right, mid;

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
            //given second value not found. return empty result
			pCur->eof = 1;

		} else if (shard > -1) {
            //second value is not contrained and secondShard is set
            //start scanning the specific shard second array for the given
            //first value
			int noOfObjects=pCur->end - pVtab->iter1 + 1;
			//printf("shard>-1\n");
			//only s is bound, iterate over shard of objects
			int shardSize;
			shardSize = (noOfObjects + pVtab->threads - 1)
				/ pVtab->threads;
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
            //second value is not contrained and secondShard is NOT set
            //start scanning the whole second array for the given
            //first value
			pCur->both = 0;
			pCur->o = pCur->objects[pVtab->iter1++];
			//prefetch
			/*if(pVtab->position==160){
			  int mod=pCur->o%32;
			  uint32_t *next;
			  next=firstArrays[32+mod];
			  __builtin_prefetch(&next[sizesOfFirstArrays[32+mod]/2]);
			  }*/
			pVtab->maxSize = pCur->end ;
		}
	} else {
        //given first value not found. return empty result
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
	pIdxInfo->idxNum = 0;

	pIdxInfo->estimatedCost = 1000;
	pIdxInfo->estimatedRows = 1000;
	const struct sqlite3_index_constraint *pConstraint;
	pConstraint = pIdxInfo->aConstraint;

	for (i = 0; i < pIdxInfo->nConstraint; i++, pConstraint++) {
		if (pConstraint->usable == 0)
			continue;
		pIdxInfo->idxNum |= 1 << pConstraint->iColumn;
        //set the bit of idxNum that corresponds to column number to 1
		if (pConstraint->iColumn == 2) {
			//partiton
			//scan whole table
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
		//pIdxInfo->estimatedCost -=30;
		if (pConstraint->iColumn == 0) {
			pIdxInfo->aConstraintUsage[i].argvIndex = 1;
		} else {
			pIdxInfo->aConstraintUsage[i].argvIndex = 2;
		}
		pIdxInfo->aConstraintUsage[i].omit = 1;
	}
	


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
		__builtin_prefetch(&firstArrays[position][*iterOuter]);
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
	//if(*iterOuter >= sizesOfFirstArrays){
	//      printf(">>>>>>!!!!!!!!: %i  %i  \n", *iterOuter, sizesOfFirstArrays);
	//}
	//struct subjectVector* result=NULL;
	if (subject == firstArrays[position][*iterOuter]) {
		return 1;
	} else if (subject > firstArrays[position][*iterOuter]) {
		while (*iterOuter < sizesOfFirstArrays[position] - 1) {
			(*iterOuter)++;
			if (subject == firstArrays[position][*iterOuter]) {
				return 1;
			} else if (subject < firstArrays[position][*iterOuter]) {
				return 0;
			}
		}
	} else {
		while (*iterOuter > 0) {
			(*iterOuter)--;
			if (subject == firstArrays[position][*iterOuter]) {
				return 1;
			} else if (subject > firstArrays[position][*iterOuter]) {
				return 0;
			}
		}

	}
	return 0;
}

static inline int adaptiveSubjectSearch(
		const int position, const uint32_t subject, int start, int* iterOuter) {
	//printf("current:%i \n", *iterOuter);
	const int distance = subject - firstArrays[position][*iterOuter];
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
		return binarySubjectSearch(position, subject, 0, iterOuter);
	}


}


static inline int binarySubjectSearch(
		int position, uint32_t subject, int start, int* iterOuter) {
	int left, right, mid;
	/* Initializations */
	left = start;
	right = sizesOfFirstArrays[position] - 1;

	while (left <= right) {
		mid = (left + right) / 2;
		///printf("left%i right:5 mid:%i \n", left, right,  mid);
		//binaryCounter++;

		if (subject == firstArrays[position][mid]) {
			//element found
			//printf("mid:%i\n", mid);
			(*iterOuter) = mid;
			return 1;
			//return result;
		} else if (subject > firstArrays[position][mid]) {
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



	sizesOfFirstArrays[index] = countDistinct;
	firstArrays[index] = subjectIds;
	secondArrays[index] = objectIds;
	positionsAtSecondArray[index] = counts;
	//printf("no of subjs: %i \n", sizesOfFirstArrays[index]);

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

int loadMemoryData(sqlite3 *db, int threads, int loadDictionary) {
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

	sizesOfFirstArrays = malloc(
			threads * 2 * (maxPropId + 1) * sizeof(int));
	windowSizeGlobal = malloc(
			threads * 2 * (maxPropId + 1) * sizeof(int));
	firstArrays = malloc(threads * 2 * (maxPropId + 1) * sizeof(uint32_t*));
	secondArrays = malloc(threads * 2 * (maxPropId + 1) * sizeof(uint32_t*));
	positionsAtSecondArray = malloc(threads * 2 * (maxPropId + 1) * sizeof(uint32_t*));
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
	pthread_t threadscreated[2 * (maxPropId + 1)];
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
		tables[propNo].threads = threads;
		tables[propNo].inverse = 0;
		tables[propNo].dbdir = dbdir;
		//tables[propNo].db=db;

		int code;
		code = pthread_create(&(threadscreated[propNo]), &attr, loadTable,
				&tables[propNo]);
		//pthread_join(threads[propNo], NULL);
		if (code != 0)
			printf("\ncan't create thread :[%s]", strerror(code));

	}
	for (propNo = 0; propNo < maxPropId + 1; propNo++) {

		//tables[propNo+maxPropId+1]=malloc(sizeof(struct tableInfo));
		tables[propNo + maxPropId + 1].propNo = propNo;
		tables[propNo + maxPropId + 1].threads = threads;
		tables[propNo + maxPropId + 1].inverse = 1;
		tables[propNo + maxPropId + 1].dbdir = dbdir;
		//tables[propNo+maxPropId+1].db=db;

		int code;
		code = pthread_create(&(threadscreated[maxPropId + 1 + propNo]), &attr,
				loadTable, &tables[propNo + maxPropId + 1]);
		//pthread_join(threads[maxPropId+1+propNo], NULL);
		if (code != 0)
			printf("\ncan't create thread :[%s]", strerror(code));

	}
	long t;
	pthread_attr_destroy(&attr);
	for (t = 0; t < 2 * (maxPropId + 1); t++) {

		pthread_join(threadscreated[t], &status);
	}
	if(USEBITMAP){
		popcount32e_init();
	}
	long maxNoOfSubjs=0;
	long indexOfMaxNoOfSubjs=0;
	printf("finished loading tables	\n");

	for (t = 0; t < 2 * (maxPropId + 1); t++) {
		if(sizesOfFirstArrays[t]>maxNoOfSubjs){
			maxNoOfSubjs=sizesOfFirstArrays[t];
			indexOfMaxNoOfSubjs=t;
		}
	}
	//printf("1\n");
	int window;

	//int range=((firstArrays[indexOfMaxNoOfSubjs][maxNoOfSubjs-1]-firstArrays[indexOfMaxNoOfSubjs][0])/maxNoOfSubjs)*window;
	int k;
	uint32_t toFind=firstArrays[indexOfMaxNoOfSubjs][0];
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
	//printf("index: %i max: %i last:%i first:%i \n", indexOfMaxNoOfSubjs, maxNoOfSubjs, firstArrays[indexOfMaxNoOfSubjs][maxNoOfSubjs-1], firstArrays[indexOfMaxNoOfSubjs][0]);
	do{
		window=nextWindow;
		int range=((firstArrays[indexOfMaxNoOfSubjs][maxNoOfSubjs-1]-firstArrays[indexOfMaxNoOfSubjs][0])/maxNoOfSubjs)*window;
		iter=0;
		long timeBinary=getTime();
		int results=0;
		toFind=firstArrays[indexOfMaxNoOfSubjs][0];
		if(USEBITMAP){
			for(k=0;k<10000&&toFind<firstArrays[indexOfMaxNoOfSubjs][maxNoOfSubjs-1];k++, toFind+=range){
				//printf("tofind: %i \n", toFind);
				if(bitmapSearch(indexOfMaxNoOfSubjs, toFind, iterPtr)) results++; 
			}

		}
		else{
			for(k=0;k<10000&&toFind<firstArrays[indexOfMaxNoOfSubjs][maxNoOfSubjs-1];k++, toFind+=range){
				//printf("tofind: %i \n", toFind);
				if(binarySubjectSearch(indexOfMaxNoOfSubjs, toFind, 0, iterPtr)) results++; 
			}
		}

		timeBinary=getTime()-timeBinary;

		//printf("binary: %ld k:%i results: %i \n", getTime()-time, k, results);
		toFind=firstArrays[indexOfMaxNoOfSubjs][0];
		iter=0;
		int results2=0;
		long timeScan = getTime();

		for(k=0;k<10000&&toFind<firstArrays[indexOfMaxNoOfSubjs][maxNoOfSubjs-1];k++, toFind+=range){
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
		int range = firstArrays[t][sizesOfFirstArrays[t] - 1] -  firstArrays[t][0];

		int estimatedWindow = 0;
		if (sizesOfFirstArrays[t] < window) {
			windowSizeGlobal[t] = window;

		} else {
			estimatedWindow = range / sizesOfFirstArrays[t];
			estimatedWindow *= window;
			windowSizeGlobal[t] = estimatedWindow;
		}
	}
	//printf("4\n");
	free(sql);
	pthread_mutex_destroy(&mutexload);


}


int sqlite3_parj_init(sqlite3 *db, char **pzErrMsg,
		const sqlite3_api_routines *pApi) {
	int rc = SQLITE_OK;
	SQLITE_EXTENSION_INIT2(pApi);
#ifndef SQLITE_OMIT_VIRTUALTABLE
	rc = sqlite3_create_module(db, "memorywrapper", &memorywrapperModule, 0);
	rc = sqlite3_create_module(db, "dictionary", &dictionaryModule, 0);
	rc = sqlite3_create_module(db, "stat", &statModule, 0);
#endif
	return rc;
}

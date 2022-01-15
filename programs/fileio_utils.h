#ifndef FILEIO_UTILS_HEADER
#define FILEIO_UTILS_HEADER

#include "../lib/common/mem.h"     /* U32, U64 */
#include "timefn.h"
#include "fileio_types.h"
#include "platform.h"
#include "util.h"

/*-*************************************
*  Constants
***************************************/
#define ADAPT_WINDOWLOG_DEFAULT 23   /* 8 MB */
#define DICTSIZE_MAX (32 MB)   /* protection against large input (attack scenario) */

/* Default file permissions 0666 (modulated by umask) */
#if !defined(_WIN32)
/* These macros aren't defined on windows. */
#define DEFAULT_FILE_PERMISSIONS (S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH)
#else
#define DEFAULT_FILE_PERMISSIONS (0666)
#endif

/*-*************************************
*  Macros
***************************************/
#define KB *(1 <<10)
#define MB *(1 <<20)
#define GB *(1U<<30)
#undef MAX
#define MAX(a,b) ((a)>(b) ? (a) : (b))

extern FIO_display_prefs_t g_display_prefs;

#define DISPLAY(...)         fprintf(stderr, __VA_ARGS__)
#define DISPLAYOUT(...)      fprintf(stdout, __VA_ARGS__)
#define DISPLAYLEVEL(l, ...) { if (g_display_prefs.displayLevel>=l) { DISPLAY(__VA_ARGS__); } }

static const U64 g_refreshRate = SEC_TO_MICRO / 6;
extern UTIL_time_t g_displayClock;

#define READY_FOR_UPDATE() ((g_display_prefs.progressSetting != FIO_ps_never) && UTIL_clockSpanMicro(g_displayClock) > g_refreshRate)
#define DELAY_NEXT_UPDATE() { g_displayClock = UTIL_getTime(); }
#define DISPLAYUPDATE(l, ...) {                              \
        if (g_display_prefs.displayLevel>=l && (g_display_prefs.progressSetting != FIO_ps_never)) { \
            if (READY_FOR_UPDATE() || (g_display_prefs.displayLevel>=4)) { \
                DELAY_NEXT_UPDATE();                         \
                DISPLAY(__VA_ARGS__);                        \
                if (g_display_prefs.displayLevel>=4) fflush(stderr);       \
    }   }   }

#undef MIN  /* in case it would be already defined */
#define MIN(a,b)    ((a) < (b) ? (a) : (b))


#define EXM_THROW(error, ...)                                             \
{                                                                         \
    DISPLAYLEVEL(1, "zstd: ");                                            \
    DISPLAYLEVEL(5, "Error defined at %s, line %i : \n", __FILE__, __LINE__); \
    DISPLAYLEVEL(1, "error %i : ", error);                                \
    DISPLAYLEVEL(1, __VA_ARGS__);                                         \
    DISPLAYLEVEL(1, " \n");                                               \
    exit(error);                                                          \
}

#define CHECK_V(v, f)                                \
    v = f;                                           \
    if (ZSTD_isError(v)) {                           \
        DISPLAYLEVEL(5, "%s \n", #f);                \
        EXM_THROW(11, "%s", ZSTD_getErrorName(v));   \
    }
#define CHECK(f) { size_t err; CHECK_V(err, f); }


/* Avoid fseek()'s 2GiB barrier with MSVC, macOS, *BSD, MinGW */
#if defined(_MSC_VER) && _MSC_VER >= 1400
#   define LONG_SEEK _fseeki64
#   define LONG_TELL _ftelli64
#elif !defined(__64BIT__) && (PLATFORM_POSIX_VERSION >= 200112L) /* No point defining Large file for 64 bit */
#  define LONG_SEEK fseeko
#  define LONG_TELL ftello
#elif defined(__MINGW32__) && !defined(__STRICT_ANSI__) && !defined(__NO_MINGW_LFS) && defined(__MSVCRT__)
#   define LONG_SEEK fseeko64
#   define LONG_TELL ftello64
#elif defined(_WIN32) && !defined(__DJGPP__)
#   include <windows.h>
    static int LONG_SEEK(FILE* file, __int64 offset, int origin) {
        LARGE_INTEGER off;
        DWORD method;
        off.QuadPart = offset;
        if (origin == SEEK_END)
            method = FILE_END;
        else if (origin == SEEK_CUR)
            method = FILE_CURRENT;
        else
            method = FILE_BEGIN;

        if (SetFilePointerEx((HANDLE) _get_osfhandle(_fileno(file)), off, NULL, method))
            return 0;
        else
            return -1;
    }
    static __int64 LONG_TELL(FILE* file) {
        LARGE_INTEGER off, newOff;
        off.QuadPart = 0;
        newOff.QuadPart = 0;
        SetFilePointerEx((HANDLE) _get_osfhandle(_fileno(file)), off, &newOff, FILE_CURRENT);
        return newOff.QuadPart;
    }
#else
#   define LONG_SEEK fseek
#   define LONG_TELL ftell
#endif


/* **********************************************************************
 *  AsyncIO functionality
 ************************************************************************/
#include "../lib/common/pool.h"
#include "../lib/common/threading.h"

#define MAX_IO_JOBS          (10)

typedef struct {
    /* These struct fields should be set only on creation and not changed afterwards */
    POOL_ctx* threadPool;
    int totalIoJobs;
    FIO_prefs_t* prefs;
    POOL_function poolFunction;

    /* Controls the file we currently write to, make changes only by using provided utility functions */
    FILE* file;
    unsigned storedSkips; // only used for write io pool

    /* The jobs and availableJobsCount fields are accessed by both the main and writer threads and should
     * only be mutated after locking the mutex */
    ZSTD_pthread_mutex_t ioJobsMutex;
    void* availableJobs[MAX_IO_JOBS];
    int availableJobsCount;
} io_pool_ctx_t;

typedef struct {
    io_pool_ctx_t base;

    int reachedEof;
    U64 nextReadOffset;
    U64 waitingOnOffset;

    U8 *srcBufferBase;
    size_t srcBufferBaseSize;
    U8 *srcBuffer;
    size_t srcBufferLoaded;

    void* completedJobs[MAX_IO_JOBS];
    int completedJobsCount;
    ZSTD_pthread_cond_t jobCompletedCond;
} read_pool_ctx_t;

typedef struct {
    /* These fields are automatically set and shouldn't be changed by non WritePool code. */
    void *ctx;
    FILE* file;
    void *buffer;
    size_t bufferSize;

    /* This field should be changed before a job is queued for execution and should contain the number
     * of bytes to write from the buffer. */
    size_t usedBufferSize;
    U64 offset;
} io_job_t;


/* IoPool_releaseIoJob:
 * Releases an acquired job back to the pool. Doesn't execute the job. */
void IoPool_releaseIoJob(io_job_t *job);

/* IoPool_free:
 * Release a previously allocated write thread pool. Makes sure all takss are done and released. */
void IoPool_free(io_pool_ctx_t* ctx);

/* IoPool_acquireJob:
 * Returns an available write job to be used for a future write. */
io_job_t* IoPool_acquireJob(io_pool_ctx_t *ctx);

/* IoPool_enqueueAndReacquireWriteJob:
 * Queues a write job for execution and acquires a new one.
 * After execution `job`'s pointed value would change to the newly acquired job.
 * Make sure to set `usedBufferSize` to the wanted length before call.
 * The queued job shouldn't be used directly after queueing it. */
void IoPool_enqueueAndReacquireWriteJob(io_job_t **job);

/* WritePool_sparseWriteEnd:
 * Ends sparse writes to the current file.
 * Blocks on completion of all current write jobs before executing. */
void WritePool_sparseWriteEnd(io_pool_ctx_t* ctx);

/* IoPool_setFile:
 * Sets the destination file for future files in the pool.
 * Requires completion of all queues write jobs and release of all otherwise acquired jobs.
 * Also requires ending of sparse write if a previous file was used in sparse mode. */
void IoPool_setFile(io_pool_ctx_t *ctx, FILE* file);

/* WritePool_closeDstFile:
 * Ends sparse write and closes the writePool's current file and sets the file to NULL.
 * Requires completion of all queues write jobs and release of all otherwise acquired jobs.  */
int WritePool_closeDstFile(io_pool_ctx_t *ctx);

/* WritePool_create:
 * Allocates and sets and a new write pool including its included jobs. */
io_pool_ctx_t* WritePool_create(FIO_prefs_t* const prefs, size_t bufferSize);

/* ReadPool_create:
 * Allocates and sets and a new write pool including its included jobs. */
read_pool_ctx_t* ReadPool_create(FIO_prefs_t* const prefs, size_t bufferSize);

/* ReadPool_free:
 * Allocates and sets and a new write pool including its included jobs. */
void ReadPool_free(read_pool_ctx_t* ctx);

void ReadPool_consumeBytes(read_pool_ctx_t *ctx, size_t n);

size_t ReadPool_readBuffer(read_pool_ctx_t *ctx, size_t n);

size_t ReadPool_consumeAndReadAll(read_pool_ctx_t *ctx);

void ReadPool_setFile(read_pool_ctx_t *ctx, FILE* file);

void ReadPool_startReading(read_pool_ctx_t *ctx);

#endif /* FILEIO_UTILS_HEADER */
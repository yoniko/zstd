#include "platform.h"
#include <stdio.h>      /* fprintf, open, fdopen, fread, _fileno, stdin, stdout */
#include <stdlib.h>     /* malloc, free */
#include <assert.h>
#include <errno.h>      /* errno */
#include "timefn.h"     /* UTIL_getTime, UTIL_clockSpanMicro */

#if defined (_MSC_VER)
#  include <sys/stat.h>
#  include <io.h>
#endif

#include "fileio_utils.h"

FIO_display_prefs_t g_display_prefs = {2, FIO_ps_auto};
UTIL_time_t g_displayClock = UTIL_TIME_INITIALIZER;

/* **********************************************************************
 *  Sparse write
 ************************************************************************/

/** FIO_fwriteSparse() :
*  @return : storedSkips,
*            argument for next call to FIO_fwriteSparse() or FIO_fwriteSparseEnd() */
static unsigned
FIO_fwriteSparse(FILE* file,
                 const void* buffer, size_t bufferSize,
                 const FIO_prefs_t* const prefs,
                 unsigned storedSkips)
{
    const size_t* const bufferT = (const size_t*)buffer;   /* Buffer is supposed malloc'ed, hence aligned on size_t */
    size_t bufferSizeT = bufferSize / sizeof(size_t);
    const size_t* const bufferTEnd = bufferT + bufferSizeT;
    const size_t* ptrT = bufferT;
    static const size_t segmentSizeT = (32 KB) / sizeof(size_t);   /* check every 32 KB */

    if (prefs->testMode) return 0;  /* do not output anything in test mode */

    if (!prefs->sparseFileSupport) {  /* normal write */
        size_t const sizeCheck = fwrite(buffer, 1, bufferSize, file);
        if (sizeCheck != bufferSize)
            EXM_THROW(70, "Write error : cannot write decoded block : %s",
                      strerror(errno));
        return 0;
    }

    /* avoid int overflow */
    if (storedSkips > 1 GB) {
        if (LONG_SEEK(file, 1 GB, SEEK_CUR) != 0)
        EXM_THROW(91, "1 GB skip error (sparse file support)");
        storedSkips -= 1 GB;
    }

    while (ptrT < bufferTEnd) {
        size_t nb0T;

        /* adjust last segment if < 32 KB */
        size_t seg0SizeT = segmentSizeT;
        if (seg0SizeT > bufferSizeT) seg0SizeT = bufferSizeT;
        bufferSizeT -= seg0SizeT;

        /* count leading zeroes */
        for (nb0T=0; (nb0T < seg0SizeT) && (ptrT[nb0T] == 0); nb0T++) ;
        storedSkips += (unsigned)(nb0T * sizeof(size_t));

        if (nb0T != seg0SizeT) {   /* not all 0s */
            size_t const nbNon0ST = seg0SizeT - nb0T;
            /* skip leading zeros */
            if (LONG_SEEK(file, storedSkips, SEEK_CUR) != 0)
                EXM_THROW(92, "Sparse skip error ; try --no-sparse");
            storedSkips = 0;
            /* write the rest */
            if (fwrite(ptrT + nb0T, sizeof(size_t), nbNon0ST, file) != nbNon0ST)
                EXM_THROW(93, "Write error : cannot write decoded block : %s",
                          strerror(errno));
        }
        ptrT += seg0SizeT;
    }

    {   static size_t const maskT = sizeof(size_t)-1;
        if (bufferSize & maskT) {
            /* size not multiple of sizeof(size_t) : implies end of block */
            const char* const restStart = (const char*)bufferTEnd;
            const char* restPtr = restStart;
            const char* const restEnd = (const char*)buffer + bufferSize;
            assert(restEnd > restStart && restEnd < restStart + sizeof(size_t));
            for ( ; (restPtr < restEnd) && (*restPtr == 0); restPtr++) ;
            storedSkips += (unsigned) (restPtr - restStart);
            if (restPtr != restEnd) {
                /* not all remaining bytes are 0 */
                size_t const restSize = (size_t)(restEnd - restPtr);
                if (LONG_SEEK(file, storedSkips, SEEK_CUR) != 0)
                    EXM_THROW(92, "Sparse skip error ; try --no-sparse");
                if (fwrite(restPtr, 1, restSize, file) != restSize)
                    EXM_THROW(95, "Write error : cannot write end of decoded block : %s",
                              strerror(errno));
                storedSkips = 0;
            }   }   }

    return storedSkips;
}

static void
FIO_fwriteSparseEnd(const FIO_prefs_t* const prefs, FILE* file, unsigned storedSkips)
{
    if (prefs->testMode) assert(storedSkips == 0);
    if (storedSkips>0) {
        assert(prefs->sparseFileSupport > 0);  /* storedSkips>0 implies sparse support is enabled */
        (void)prefs;   /* assert can be disabled, in which case prefs becomes unused */
        if (LONG_SEEK(file, storedSkips-1, SEEK_CUR) != 0)
            EXM_THROW(69, "Final skip error (sparse file support)");
        /* last zero must be explicitly written,
         * so that skipped ones get implicitly translated as zero by FS */
        {   const char lastZeroByte[1] = { 0 };
            if (fwrite(lastZeroByte, 1, 1, file) != 1)
                EXM_THROW(69, "Write error : cannot write last zero : %s", strerror(errno));
        }   }
}


/* **********************************************************************
 *  AsyncIO functionality
 ************************************************************************/

/* ***********************************
 *  General IoPool implementation
 *************************************/

static io_job_t *IoPool_createIoJob(io_pool_ctx_t *ctx, size_t bufferSize) {
    void *buffer;
    io_job_t *job;
    job = (io_job_t*) malloc(sizeof(io_job_t));
    buffer = malloc(bufferSize);
    if(!job || !buffer)
    EXM_THROW(101, "Allocation error : not enough memory");
    job->buffer = buffer;
    job->bufferSize = bufferSize;
    job->usedBufferSize = 0;
    job->file = NULL;
    job->ctx = ctx;
    job->offset = 0;
    return job;
}


/* IoPool_createThreadPool:
 * Creates a thread pool and a mutex for threaded IO pool.
 * Displays warning if asyncio is requested but MT isn't available. */
static void IoPool_createThreadPool(io_pool_ctx_t *ctx, const FIO_prefs_t *prefs) {
    ctx->threadPool = NULL;
    if(prefs->asyncIO) {
        if (ZSTD_pthread_mutex_init(&ctx->ioJobsMutex, NULL))
        EXM_THROW(102,"Failed creating write availableJobs mutex");
        /* We want MAX_IO_JOBS-2 queue items because we need to always have 1 free buffer to
         * decompress into and 1 buffer that's actively written to disk and owned by the writing thread. */
        assert(MAX_IO_JOBS >= 2);
        ctx->threadPool = POOL_create(1, MAX_IO_JOBS - 2);
        if (!ctx->threadPool)
        EXM_THROW(104, "Failed creating writer thread pool");
    }
}

/* IoPool_init:
 * Allocates and sets and a new write pool including its included availableJobs. */
static void IoPool_init(io_pool_ctx_t *ctx, FIO_prefs_t* const prefs, POOL_function poolFunction, size_t bufferSize) {
    int i;
    IoPool_createThreadPool(ctx, prefs);
    ctx->prefs = prefs;
    ctx->poolFunction = poolFunction;
    ctx->totalIoJobs = ctx->threadPool ? MAX_IO_JOBS : 1;
    ctx->availableJobsCount = ctx->totalIoJobs;
    for(i=0; i < ctx->availableJobsCount; i++) {
        ctx->availableJobs[i] = IoPool_createIoJob(ctx, bufferSize);
    }
    ctx->file = NULL;
}


/* IoPool_releaseIoJob:
 * Releases an acquired job back to the pool. Doesn't execute the job. */
static void IoPool_releaseIoJob(io_job_t *job) {
    io_pool_ctx_t *ctx = (io_pool_ctx_t *) job->ctx;
    if(ctx->threadPool) {
        ZSTD_pthread_mutex_lock(&ctx->ioJobsMutex);
        assert(ctx->availableJobsCount < MAX_IO_JOBS);
        ctx->availableJobs[ctx->availableJobsCount++] = job;
        ZSTD_pthread_mutex_unlock(&ctx->ioJobsMutex);
    } else {
        assert(ctx->availableJobsCount == 0);
        ctx->availableJobsCount++;
    }
}

/* IoPool_join:
 * Waits for all tasks in the pool to finish executing. */
static void IoPool_join(io_pool_ctx_t* ctx) {
    if(ctx->threadPool)
        POOL_joinJobs(ctx->threadPool);
}

/* IoPool_free:
 * Release a previously allocated write thread pool. Makes sure all takss are done and released. */
static void IoPool_destroy(io_pool_ctx_t* ctx) {
    int i;
    if(ctx->threadPool) {
        /* Make sure we finish all tasks and then free the resources */
        IoPool_join(ctx);
        /* Make sure we are not leaking availableJobs */
        assert(ctx->availableJobsCount == ctx->totalIoJobs);
        POOL_free(ctx->threadPool);
        ZSTD_pthread_mutex_destroy(&ctx->ioJobsMutex);
    }
    assert(ctx->file == NULL);
    for(i=0; i<ctx->availableJobsCount; i++) {
        io_job_t* job = (io_job_t*) ctx->availableJobs[i];
        free(job->buffer);
        free(job);
    }
}

/* IoPool_acquireJob:
 * Returns an available io job to be used for a future io. */
static io_job_t* IoPool_acquireJob(io_pool_ctx_t *ctx) {
    io_job_t *job;
    assert(ctx->file != NULL || ctx->prefs->testMode);
    if(ctx->threadPool) {
        ZSTD_pthread_mutex_lock(&ctx->ioJobsMutex);
        assert(ctx->availableJobsCount > 0);
        job = (io_job_t*) ctx->availableJobs[--ctx->availableJobsCount];
        ZSTD_pthread_mutex_unlock(&ctx->ioJobsMutex);
    } else {
        assert(ctx->availableJobsCount == 1);
        ctx->availableJobsCount--;
        job = (io_job_t*)ctx->availableJobs[0];
    }
    job->usedBufferSize = 0;
    job->file = ctx->file;
    job->offset = 0;
    return job;
}


/* IoPool_setFile:
 * Sets the destination file for future files in the pool.
 * Requires completion of all queues write jobs and release of all otherwise acquired jobs.
 * Also requires ending of sparse write if a previous file was used in sparse mode. */
static void IoPool_setFile(io_pool_ctx_t *ctx, FILE* file) {
    assert(ctx!=NULL);
    IoPool_join(ctx);
    assert(ctx->availableJobsCount == ctx->totalIoJobs);
    ctx->file = file;
}

static FILE* IoPool_getFile(io_pool_ctx_t *ctx) {
    return ctx->file;
}

/* IoPool_enqueueJob:
 * Enqueues an io job for execution.
 * The queued job shouldn't be used directly after queueing it. */
static void IoPool_enqueueJob(io_job_t *job) {
    io_pool_ctx_t* ctx = (io_pool_ctx_t *)job->ctx;
    if(ctx->threadPool)
        POOL_add(ctx->threadPool, ctx->poolFunction, job);
    else
        ctx->poolFunction(job);
}

/* ***********************************
 *  WritePool implementation
 *************************************/

/* WritePool_acquireJob:
 * Returns an available write job to be used for a future write. */
io_job_t* WritePool_acquireJob(write_pool_ctx_t *ctx) {
    return IoPool_acquireJob(&ctx->base);
}

/* WritePool_enqueueAndReacquireWriteJob:
 * Queues a write job for execution and acquires a new one.
 * After execution `job`'s pointed value would change to the newly acquired job.
 * Make sure to set `usedBufferSize` to the wanted length before call.
 * The queued job shouldn't be used directly after queueing it. */
void WritePool_enqueueAndReacquireWriteJob(io_job_t **job) {
    IoPool_enqueueJob(*job);
    *job = IoPool_acquireJob((io_pool_ctx_t *)(*job)->ctx);
}

/* WritePool_sparseWriteEnd:
 * Ends sparse writes to the current file.
 * Blocks on completion of all current write jobs before executing. */
void WritePool_sparseWriteEnd(write_pool_ctx_t *ctx) {
    assert(ctx != NULL);
    if(ctx->base.threadPool)
        POOL_joinJobs(ctx->base.threadPool);
    FIO_fwriteSparseEnd(ctx->base.prefs, ctx->base.file, ctx->storedSkips);
    ctx->storedSkips = 0;
}

/* WritePool_setFile:
 * Sets the destination file for future writes in the pool.
 * Requires completion of all queues write jobs and release of all otherwise acquired jobs.
 * Also requires ending of sparse write if a previous file was used in sparse mode. */
void WritePool_setFile(write_pool_ctx_t *ctx, FILE* file) {
    IoPool_setFile(&ctx->base, file);
    assert(ctx->storedSkips == 0);
}

/* WritePool_releaseIoJob:
 * Releases an acquired job back to the pool. Doesn't execute the job. */
void WritePool_releaseIoJob(io_job_t *job) {
    IoPool_releaseIoJob(job);
}

/* WritePool_closeFile:
 * Ends sparse write and closes the writePool's current file and sets the file to NULL.
 * Requires completion of all queues write jobs and release of all otherwise acquired jobs.  */
int WritePool_closeFile(write_pool_ctx_t *ctx) {
    FILE *dstFile = ctx->base.file;
    assert(dstFile!=NULL || ctx->base.prefs->testMode!=0);
    WritePool_sparseWriteEnd(ctx);
    IoPool_setFile(&ctx->base, NULL);
    return fclose(dstFile);
}

/* WritePool_executeWriteJob:
 * Executes a write job synchronously. Can be used as a function for a thread pool. */
static void WritePool_executeWriteJob(void* opaque){
    io_job_t* job = (io_job_t*) opaque;
    write_pool_ctx_t* ctx = (write_pool_ctx_t*) job->ctx;
    ctx->storedSkips = FIO_fwriteSparse(job->file, job->buffer, job->usedBufferSize, ctx->base.prefs, ctx->storedSkips);
    IoPool_releaseIoJob(job);
}

/* WritePool_create:
 * Allocates and sets and a new write pool including its included jobs. */
write_pool_ctx_t* WritePool_create(FIO_prefs_t* const prefs, size_t bufferSize) {
    write_pool_ctx_t* ctx = (write_pool_ctx_t*) malloc(sizeof(write_pool_ctx_t));
    if(!ctx) EXM_THROW(100, "Allocation error : not enough memory");
    IoPool_init(&ctx->base, prefs, WritePool_executeWriteJob, bufferSize);
    ctx->storedSkips = 0;
    return ctx;
}

/* WritePool_free:
 * Frees and releases a writePool and its resources. Closes destination file if needs to. */
void WritePool_free(write_pool_ctx_t* ctx) {
    /* Make sure we finish all tasks and then free the resources */
    if(WritePool_getFile(ctx))
        WritePool_closeFile(ctx);
    IoPool_destroy(&ctx->base);
    assert(ctx->storedSkips==0);
    free(ctx);
}


/* ***********************************
 *  ReadPool implementation
 *************************************/
static void ReadPool_releaseAllCompletedJobs(read_pool_ctx_t* ctx) {
    int i;
    for(i=0; i<ctx->completedJobsCount; i++) {
        io_job_t* job = (io_job_t*) ctx->completedJobs[i];
        IoPool_releaseIoJob(job);
    }
    ctx->completedJobsCount = 0;
}

/* ReadPool_addJobToCompleted */
static void ReadPool_addJobToCompleted(io_job_t *job) {
    read_pool_ctx_t *ctx = (read_pool_ctx_t *)job->ctx;
    if(ctx->base.threadPool)
        ZSTD_pthread_mutex_lock(&ctx->base.ioJobsMutex);
    assert(ctx->completedJobsCount < MAX_IO_JOBS);
    ctx->completedJobs[ctx->completedJobsCount++] = job;
    if(ctx->base.threadPool) {
        ZSTD_pthread_cond_signal(&ctx->jobCompletedCond);
        ZSTD_pthread_mutex_unlock(&ctx->base.ioJobsMutex);
    }
}

/* assuming ioJobsMutex is locked */
static io_job_t* ReadPool_findWaitingJob(read_pool_ctx_t *ctx) {
    io_job_t *job = NULL;
    int i;
    for (i=0; i<ctx->completedJobsCount; i++) {
        job = (io_job_t *) ctx->completedJobs[i];
        if (job->offset == ctx->waitingOnOffset) {
            ctx->completedJobs[i] = ctx->completedJobs[--ctx->completedJobsCount];
            return job;
        }
    }
    return NULL;
}

/* ReadPool_getNextCompletedJob */
static io_job_t* ReadPool_getNextCompletedJob(read_pool_ctx_t *ctx) {
    io_job_t *job = NULL;
    if(ctx->base.threadPool)
        ZSTD_pthread_mutex_lock(&ctx->base.ioJobsMutex);

    job = ReadPool_findWaitingJob(ctx);

    while (!job && (ctx->base.availableJobsCount + ctx->completedJobsCount < ctx->base.totalIoJobs)) {
        assert(ctx->base.threadPool != NULL);
        ZSTD_pthread_cond_wait(&ctx->jobCompletedCond, &ctx->base.ioJobsMutex);
        job = ReadPool_findWaitingJob(ctx);
    }

    if(job) {
        assert(job->offset == ctx->waitingOnOffset);
        ctx->waitingOnOffset += job->usedBufferSize;
    }

    if(ctx->base.threadPool)
        ZSTD_pthread_mutex_unlock(&ctx->base.ioJobsMutex);
    return job;
}


/* ReadPool_executeReadJob:
 * Executes a read job synchronously. Can be used as a function for a thread pool. */
static void ReadPool_executeReadJob(void* opaque){
    io_job_t* job = (io_job_t*) opaque;
    read_pool_ctx_t* ctx = (read_pool_ctx_t *)job->ctx;
    if(ctx->reachedEof) {
        job->usedBufferSize = 0;
        ReadPool_addJobToCompleted(job);
        return;
    }
    job->usedBufferSize = fread(job->buffer, 1, job->bufferSize, job->file);
    if(job->usedBufferSize < job->bufferSize) {
        if(ferror(job->file)) {
            EXM_THROW(37, "Read error");
        } else if(feof(job->file)) {
            ctx->reachedEof = 1;
        } else
        EXM_THROW(37, "Unexpected short read");
    }
    ReadPool_addJobToCompleted(job);
}

static void ReadPool_enqueueRead(read_pool_ctx_t *ctx) {
    io_job_t *job = IoPool_acquireJob(&ctx->base);
    job->offset = ctx->nextReadOffset;
    ctx->nextReadOffset += job->bufferSize;
    IoPool_enqueueJob(job);
}

static void ReadPool_startReading(read_pool_ctx_t *ctx) {
    int i;
    for (i = 0; i < ctx->base.availableJobsCount; i++) {
        ReadPool_enqueueRead(ctx);
    }
}

/* ReadPool_setFile:
 * Sets the source file for future read in the pool. Initiates reading immediately if file is not NULL.
 * Waits for all current enqueued tasks to complete if a previous file was set. */
void ReadPool_setFile(read_pool_ctx_t *ctx, FILE* file) {
    assert(ctx!=NULL);
    IoPool_join(&ctx->base);
    ReadPool_releaseAllCompletedJobs(ctx);
    IoPool_setFile(&ctx->base, file);
    ctx->nextReadOffset = 0;
    ctx->waitingOnOffset = 0;
    ctx->srcBuffer = ctx->srcBufferBase;
    ctx->reachedEof = 0;
    if(file != NULL)
        ReadPool_startReading(ctx);
}

FILE* WritePool_getFile(write_pool_ctx_t *ctx) {
    return IoPool_getFile(&ctx->base);
}

/* ReadPool_create:
 * Allocates and sets and a new readPool including its included jobs.
 * bufferSize should be set to the maximal buffer we want to read at a time, will also be used
 * as our basic read size. */
read_pool_ctx_t* ReadPool_create(FIO_prefs_t* const prefs, size_t bufferSize) {
    read_pool_ctx_t* ctx = (read_pool_ctx_t*) malloc(sizeof(read_pool_ctx_t));
    if(!ctx) EXM_THROW(100, "Allocation error : not enough memory");
    IoPool_init(&ctx->base, prefs, ReadPool_executeReadJob, bufferSize);

    ctx->srcBufferBaseSize = 2 * bufferSize;
    ctx->srcBufferBase = (U8*) malloc(ctx->srcBufferBaseSize);
    ctx->srcBuffer = ctx->srcBufferBase;
    ctx->srcBufferLoaded = 0;
    ctx->completedJobsCount = 0;

    if(ctx->base.threadPool)
        if (ZSTD_pthread_cond_init(&ctx->jobCompletedCond, NULL))
            EXM_THROW(103,"Failed creating write jobCompletedCond mutex");

    return ctx;
}

/* ReadPool_free:
 * Frees and releases a readPool and its resources. Closes source file. */
void ReadPool_free(read_pool_ctx_t* ctx) {
    if(ReadPool_getFile(ctx))
        ReadPool_closeFile(ctx);
    if(ctx->base.threadPool)
        ZSTD_pthread_cond_destroy(&ctx->jobCompletedCond);
    IoPool_destroy(&ctx->base);
    free(ctx->srcBufferBase);
    free(ctx);
}

/* ReadPool_consumeBytes:
 * Consumes byes from srcBuffer's beginning and updates srcBufferLoaded accordingly. */
void ReadPool_consumeBytes(read_pool_ctx_t *ctx, size_t n) {
    assert(n <= ctx->srcBufferLoaded);
    assert(ctx->srcBuffer + n <= ctx->srcBufferBase + ctx->srcBufferBaseSize);
    ctx->srcBufferLoaded -= n;
    ctx->srcBuffer += n;
}

/* ReadPool_fillBuffer:
 * Makes sure buffer has at least n bytes loaded (as long as n is not bigger than the initalized bufferSize).
 * Returns if srcBuffer has at least n bytes loaded or if we've reached the end of the file.
 * Return value is the number of bytes added to the buffer.
 * Note that srcBuffer might have up to 2 times bufferSize bytes. */
size_t ReadPool_fillBuffer(read_pool_ctx_t *ctx, size_t n) {
    io_job_t *job;
    size_t srcBufferOffsetFromBase;
    size_t srcBufferRemainingSpace;
    size_t bytesRead = 0;
    assert(n <= ctx->srcBufferBaseSize/2);
    while (ctx->srcBufferLoaded < n) {
        job = ReadPool_getNextCompletedJob(ctx);
        if(job == NULL)
            break;
        srcBufferOffsetFromBase = ctx->srcBuffer - ctx->srcBufferBase;
        srcBufferRemainingSpace = ctx->srcBufferBaseSize - (srcBufferOffsetFromBase + ctx->srcBufferLoaded);
        if(job->usedBufferSize > srcBufferRemainingSpace) {
            memmove(ctx->srcBufferBase, ctx->srcBuffer, ctx->srcBufferLoaded);
            ctx->srcBuffer = ctx->srcBufferBase;
        }
        memcpy(ctx->srcBuffer + ctx->srcBufferLoaded, job->buffer, job->usedBufferSize);
        bytesRead += job->usedBufferSize;
        ctx->srcBufferLoaded += job->usedBufferSize;
        if(job->usedBufferSize < job->bufferSize) {
            IoPool_releaseIoJob(job);
            break;
        }
        IoPool_releaseIoJob(job);
        ReadPool_enqueueRead(ctx);
    }
    return bytesRead;
}

/* ReadPool_consumeAndRefill:
 * Consumes the current buffer and refills it with bufferSize bytes. */
size_t ReadPool_consumeAndRefill(read_pool_ctx_t *ctx) {
    ReadPool_consumeBytes(ctx, ctx->srcBufferLoaded);
    return ReadPool_fillBuffer(ctx, ctx->srcBufferBaseSize/2);
}

/* ReadPool_getFile:
 * Returns the current file set for the read pool. */
FILE* ReadPool_getFile(read_pool_ctx_t *ctx) {
    return IoPool_getFile(&ctx->base);
}

/* ReadPool_closeFile:
 * Closes the current set file. Waits for all current enqueued tasks to complete and resets state. */
int ReadPool_closeFile(read_pool_ctx_t *ctx) {
    FILE* file = ReadPool_getFile(ctx);
    ReadPool_setFile(ctx, NULL);
    return fclose(file);
}

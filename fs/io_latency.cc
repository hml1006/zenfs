/*
 * io_latency.cc
 *
 *  Created on: 2022-05-17
 *      Author: hml1006
 */
#include "io_latency.h"
#include <set>
#include <mutex>
#include <cstdio>
#include <string>
#include <thread>
#include <unistd.h>

// total calls
std::atomic<uint64_t> TotalReqs[TargetEnd] = {};
// total latency
std::atomic<uint64_t> TotalLatency[TargetEnd] = {};
// max latency
std::atomic<uint64_t> MaxLatency[TargetEnd] = {};

// us latency, 10 us an item
std::atomic<uint64_t> LatencyStatUs[TargetEnd][LATENCY_STAT_US_LEN] = {};
// ms latency, 1 ms an item
std::atomic<uint64_t> LatencyStatMs[TargetEnd][LATENCY_STAT_MS_LEN] = {};
std::atomic<uint64_t> LatencyStat100Ms[TargetEnd] = {};
uint64_t LatencyStatUsTmp[TargetEnd][LATENCY_STAT_US_LEN] = {};
uint64_t LatencyStatMsTmp[TargetEnd][LATENCY_STAT_MS_LEN] = {};
std::atomic<int> PWriteDataLen[PWRITE_DATA_ARR_LEN] = {};
std::atomic<int> PWriteDataLen1MB = 0;
int PWriteDataLenTmp[1024] = {};

// pre total calls
uint64_t PreTotalReqs[TargetEnd] = {};
// pre second total latency
uint64_t PreTotalLatency[TargetEnd] = {};

static std::set<long int> append_thread_set;
static std::mutex append_thread_set_mutex;

FILE *latency_log_file = NULL;
std::atomic_flag has_inited(false);

const char* ZenfsGetLatencyTargetName(enum LatencyTargetIndex id)
{
	switch (id) {
	case ZoneFileAppend:
		return "ZoneFile::Append()";
	case ZoneFileSparseAppend:
		return "ZoneFile::SparseAppend()";
	case ZoneFileBufferedAppend:
		return "ZoneFile::BufferedAppend()";
	case SystemPread:
		return "pread()";
	case SystemPwrite:
		return "pwrite()";
	case ZoneFilePositionedRead:
		return "ZoneFile::PositionedRead()";
	case ZoneFileBufferedAppendZoneAppend:
		return "ZoneFile::BufferedAppend()->Zone::Append()";
	case ZoneFileSparseAppendZoneAppend:
		return "ZoneFile::SparseAppend()->Zone::Append()";
	case ZoneFileAppendZoneAppend:
		return "ZoneFile::Append()->Zone::Append()";

	case ZoneWritableBufferedWrite:
		return "ZonedWritableFile::BufferedWrite()";
	case ZoneWritableFlushBuffer:
		return "ZonedWritableFile::FlushBuffer()";
	case ZoneWritableDataSync:
		return "ZonedWritableFile::DataSync()";
	case ZoneWritablePositionedAppend:
		return "ZonedWritableFile::PositionedAppend()";
	case ZoneWritableAppend:
		return "ZonedWritableFile::Append()";

	case ZoneWritableDataSyncFlushBuffer:
		return "ZonedWritableFile::DataSync()->FlushBuffer()";
	case ZoneWritableFlushBufferZoneFileSparseAppend:
		return "ZonedWritableFile::FlushBuffer()->ZoneFile::SparseAppend()";
	case ZoneWritableFlushBufferZoneFileBufferdAppend:
		return "ZonedWritableFile::FlushBuffer()->ZoneFile::BufferedAppend()";
	case ZoneWritableBufferedWriteFlushBuffer:
		return "ZonedWritableFile::BufferedWrite()->FlushBuffer()";
	case ZoneWritableAppendBufferedWrite:
		return "ZonedWritableFile::Append()->BufferedWrite()";
	case ZoneWritableAppendZoneFileAppend:
		return "ZonedWritableFile::Append()->ZoneFile::Append()";
	case ZoneWritablePositionedAppendBufferedWrite:
		return "ZonedWritableFile::PositionedAppend()->BufferedWrite()";
	case ZoneWritablePositionedAppendZoneFileAppend:
		return "ZonedWritableFile::PositionedAppend()->ZoneFile::Append()";

	case ZoneSeqReadZoneFilePositionedRead:
		return "ZonedSequentialFile::Read()->ZoneFile::PositionedRead()";
	case ZoneSeqRead:
		return "ZonedSequentialFile::Read";
	case ZoneSeqPositionedReadZoneFilePositionedRead:
		return "ZonedSequentialFile::PositionedRead()->ZoneFile::PositionedRead()";
	case ZoneSeqPositionedRead:
		return "ZonedSequentialFile::PositionedRead()";

	case ZoneRandomReadZoneFilePositionedRead:
		return "ZonedRandomAccessFile::Read()->ZoneFile::PositionedRead()";
	case ZoneRandomRead:
		return "ZonedRandomAccessFile::Read()";
	default:
		return "";
	}
}

static int ZenfsSetLatencyLog(const char *file)
{
	if (latency_log_file)
		return -1;

	latency_log_file = fopen(file, "w+");
	if (!latency_log_file) {
		return -1;
	}

	return 0;
}

static void ZenfsCloseLatencyLog()
{
	if (latency_log_file) {
		fclose(latency_log_file);
	}
}

#define TIME_STEP	5
static time_t second = 0;

static void ZenfsShowLatency()
{
	struct tm *current;
	char fmt_time[32] = {0};
	current = localtime(&second);
	strftime(fmt_time, sizeof(fmt_time), "%Y-%m-%d %H:%M:%S", current);
	for (int i = TargetStart + 1; i < TargetEnd; i++) {
		uint64_t total_reqs =TotalReqs[i].load();
		uint64_t total_latency = TotalLatency[i].load();
		uint64_t max_latency = MaxLatency[i].fetch_and(0);

		uint64_t reqs = total_reqs - PreTotalReqs[i];
		uint64_t latency = total_latency - PreTotalLatency[i];
		uint64_t average_latency = latency / (reqs? reqs : 1);

		PreTotalReqs[i] = total_reqs;
		PreTotalLatency[i] = total_latency;

		for (int j = 0; j < LATENCY_STAT_US_LEN; j++) {
			LatencyStatUsTmp[i][j] = LatencyStatUs[i][j].fetch_and(0);
		}
		for (int j = 0; j < LATENCY_STAT_MS_LEN; j++) {
			LatencyStatMsTmp[i][j] = LatencyStatMs[i][j].fetch_and(0);
		}
		uint64_t greater_than_100ms = LatencyStat100Ms[i].fetch_and(0);
		if (latency_log_file && (reqs != 0)) {
			fprintf(latency_log_file, "\n===================================time %s==================================\n", fmt_time);
			fprintf(latency_log_file, "latency[%s](us) => max: %lu, avg: %lu, count: %lu, total: %lu\n",
					ZenfsGetLatencyTargetName((LatencyTargetIndex)i), max_latency, average_latency, reqs, latency);
			fprintf(latency_log_file, "**************************************************************************\n");
			fprintf(latency_log_file, "latency\t\tcount\n");
			for (int j = 0; j < LATENCY_STAT_US_LEN; j++) {
				if (LatencyStatUsTmp[i][j]) {
					fprintf(latency_log_file, "%d-%d us\t\t%lu\n", j * US_LATENCY_STEP, (j + 1) * US_LATENCY_STEP, LatencyStatUsTmp[i][j]);
				}
			}
			for (int j = 0; j < LATENCY_STAT_MS_LEN; j++) {
				if (LatencyStatMsTmp[i][j]) {
					fprintf(latency_log_file, "%d ms\t\t%lu\n", j + 1, LatencyStatMsTmp[i][j]);
				}
			}
			if (greater_than_100ms) {
				fprintf(latency_log_file, ">100 ms\t\t\t%lu\n", greater_than_100ms);
			}
		}
	}
	fprintf(latency_log_file, "pwrite len(KB)\tcount\n");
	for (int i = 0; i < PWRITE_DATA_ARR_LEN; i++) {
		int count = PWriteDataLen[i].fetch_and(0);
		if (count > 0) {
			fprintf(latency_log_file, "%d-%d\t%d\n", i, i + 1, count);
		}
	}
	uint64_t large_then_1MB = PWriteDataLen1MB.fetch_and(0);
	if (large_then_1MB) {
		fprintf(latency_log_file, "> 1MB\t%ld\n", large_then_1MB);
	}
	append_thread_set_mutex.lock();
	fprintf(latency_log_file, "zone append thread id list, num = %ld\n", append_thread_set.size());
	for (auto it = append_thread_set.begin(); it != append_thread_set.end(); it++) {
		fprintf(latency_log_file, "%ld  ", *it);
	}
	append_thread_set_mutex.unlock();
}

void LoopShowLatency()
{
	for(;;) {
		sleep(1);
		second = time(NULL);
		ZenfsShowLatency();
	}
}


void RecordThreadId()
{
	std::lock_guard<std::mutex> lock(append_thread_set_mutex);
	append_thread_set.insert(gettid());
}

void ZenfsLatencyInit()
{
	has_inited.test_and_set();
	ZenfsSetLatencyLog("/tmp/zenfs_latency.log");

	atexit(ZenfsCloseLatencyLog);

	std::thread t(LoopShowLatency);
	t.detach();
}


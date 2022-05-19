/*
 * io_latency.cc
 *
 *  Created on: 2022-05-17
 *      Author: hml1006
 */
#include "io_latency.h"
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
std::atomic<uint64_t> LatencyStatUs[TargetEnd][LATENCY_STAT_LEN] = {};
// ms latency, 1 ms an item
std::atomic<uint64_t> LatencyStatMs[TargetEnd][LATENCY_STAT_LEN] = {};
std::atomic<uint64_t> LatencyStat100Ms[TargetEnd] = {};
uint64_t LatencyStatUsTmp[TargetEnd][LATENCY_STAT_LEN] = {};
uint64_t LatencyStatMsTmp[TargetEnd][LATENCY_STAT_LEN] = {};

// pre total calls
uint64_t PreTotalReqs[TargetEnd] = {};
// pre second total latency
uint64_t PreTotalLatency[TargetEnd] = {};


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
static int second = 0;

static void ZenfsShowLatency()
{
	for (int i = TargetStart + 1; i < TargetEnd; i++) {
		uint64_t total_reqs =TotalReqs[i].load();
		uint64_t total_latency = TotalLatency[i].load();
		uint64_t max_latency = MaxLatency[i].fetch_and(0);

		uint64_t reqs = total_reqs - PreTotalReqs[i];
		uint64_t latency = total_latency - PreTotalLatency[i];
		uint64_t average_latency = latency / (reqs? reqs : 1);

		PreTotalReqs[i] = total_reqs;
		PreTotalLatency[i] = total_latency;

		for (int j = 0; j < LATENCY_STAT_LEN; j++) {
			LatencyStatUsTmp[i][j] = LatencyStatUs[i][j].fetch_and(0);
			LatencyStatMsTmp[i][j] = LatencyStatMs[i][j].fetch_and(0);
		}
		uint64_t greater_than_100ms = LatencyStat100Ms[i].fetch_and(0);
		if (latency_log_file && (reqs != 0)) {
			fprintf(latency_log_file, "===================================time %d==================================\n", second);
			fprintf(latency_log_file, "latency[%s](us) => max: %lu, avg: %lu, count: %lu, total: %lu\n",
					ZenfsGetLatencyTargetName((LatencyTargetIndex)i), max_latency, average_latency, reqs, latency);
			fprintf(latency_log_file, "**************************************************************************\n");
			fprintf(latency_log_file, "latency\t\tcount\n");
			for (int j = 0; j < LATENCY_STAT_LEN; j++) {
				if (0 != LatencyStatUsTmp[i][j]) {
					fprintf(latency_log_file, "%d-%d us\t\t%lu\n", j * US_LATENCY_STEP, (j + 1) * US_LATENCY_STEP, LatencyStatUsTmp[i][j]);
				}
			}
			for (int j = 0; j < LATENCY_STAT_LEN; j++) {
				if (0 != LatencyStatMsTmp[i][j]) {
					fprintf(latency_log_file, "%d ms\t\t%lu\n", j + 1, LatencyStatMsTmp[i][j]);
				}
			}
			fprintf(latency_log_file, ">100 ms\t\t\t%lu\n", greater_than_100ms);
		}
	}
}
void LoopShowLatency()
{
	for(;;) {
		sleep(1);
		ZenfsShowLatency();
		second += TIME_STEP;
	}
}

void ZenfsLatencyInit()
{
	has_inited.test_and_set();
	ZenfsSetLatencyLog("/tmp/zenfs_latency.log");

	atexit(ZenfsCloseLatencyLog);

	std::thread t(LoopShowLatency);
	t.detach();
}


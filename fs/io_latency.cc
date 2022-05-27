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

// latency less than 16 us
std::atomic<uint64_t> LatencyStatLess16Us[TargetEnd][US_LATENCY_STEP];
// us latency
std::atomic<uint64_t> LatencyStatUs[TargetEnd][LATENCY_STAT_US_LEN] = {};
// ms latency, 1 ms an item
std::atomic<uint64_t> LatencyStatMs[TargetEnd][LATENCY_STAT_MS_LEN] = {};
std::atomic<uint64_t> LatencyStat100Ms[TargetEnd] = {};
uint64_t LatencyStatLess16UsTmp[TargetEnd][US_LATENCY_STEP] = {};
uint64_t LatencyStatUsTmp[TargetEnd][LATENCY_STAT_US_LEN] = {};
uint64_t LatencyStatMsTmp[TargetEnd][LATENCY_STAT_MS_LEN] = {};
std::atomic<int> PWriteDataLen[PWRITE_DATA_ARR_LEN] = {};
std::atomic<int> PWriteDataLen1MB = 0;
int PWriteDataLenTmp[1024] = {};

// pre total calls
uint64_t PreTotalReqs[TargetEnd] = {};
// pre second total latency
uint64_t PreTotalLatency[TargetEnd] = {};
// pre latency
uint64_t PreLatency[TargetEnd] = {};
// pre call count
uint64_t PreCount[TargetEnd] = {};
// pre max latency
uint64_t PreMaxLatency[TargetEnd] = {};
// pre average latency
uint64_t PreAvgLatency[TargetEnd] = {};

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
	case ZoneAppend:
		return "Zone::Append()";
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
		return "ZonedSequentialFile::Read()";
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


static void ShowCallTrace()
{
	fprintf(latency_log_file, "\nwrite******************************************************************************************************************************\n");
	fprintf(latency_log_file, "                                                  [pwrite<a:%3ld,c:%6ld,t:%6ld>]\n", PreAvgLatency[SystemPwrite], PreCount[SystemPwrite], PreLatency[SystemPwrite]);
	fprintf(latency_log_file, "                                                            |\n                                                            |\n");
	fprintf(latency_log_file, "                                               [Zone::Append()<a:%3ld,c:%6ld,t:%6ld>]\n", PreAvgLatency[ZoneAppend], PreCount[ZoneAppend], PreLatency[ZoneAppend]);
	fprintf(latency_log_file, "                     _______________________________________|_____________________________________________________\n");
	fprintf(latency_log_file, "                    |                                       |                                                     |\n");
	fprintf(latency_log_file, "             <a:%3ld,c:%6ld,t:%6ld>               <a:%3ld,c:%6ld,t:%6ld>                              <a:%3ld,c:%6ld,t:%6ld>\n", PreAvgLatency[ZoneFileAppendZoneAppend], PreCount[ZoneFileAppendZoneAppend], PreLatency[ZoneFileAppendZoneAppend],
			PreAvgLatency[ZoneFileSparseAppendZoneAppend], PreCount[ZoneFileSparseAppendZoneAppend], PreLatency[ZoneFileSparseAppendZoneAppend],PreAvgLatency[ZoneFileBufferedAppendZoneAppend], PreCount[ZoneFileBufferedAppendZoneAppend], PreLatency[ZoneFileBufferedAppendZoneAppend]);
	fprintf(latency_log_file, "                    |                                       |                                                     |\n");
	fprintf(latency_log_file, "  [ZoneFile::Append()<a:%3ld,c:%6ld,t:%6ld>]   [ZoneFile::SparseAppend()<a:%3ld,c:%6ld,t:%6ld>]    [ZoneFile::BufferedAppend()<a:%3ld,c:%6ld,t:%6ld>]\n", PreAvgLatency[ZoneFileAppend],
			PreCount[ZoneFileAppend], PreLatency[ZoneFileAppend], PreAvgLatency[ZoneFileSparseAppend], PreCount[ZoneFileSparseAppend], PreLatency[ZoneFileSparseAppend], PreAvgLatency[ZoneFileBufferedAppend], PreCount[ZoneFileBufferedAppend], PreLatency[ZoneFileBufferedAppend]);
	fprintf(latency_log_file, "                    |  |                                        |__________________              __________________|\n                    |  |                                                          |              |\n");
	fprintf(latency_log_file, "                    |  |                                         <a:%3ld,c:%6ld,t:%6ld>   <a:%3ld,c:%6ld,t:%6ld>\n", PreAvgLatency[ZoneWritableFlushBufferZoneFileSparseAppend], PreCount[ZoneWritableFlushBufferZoneFileSparseAppend], PreLatency[ZoneWritableFlushBufferZoneFileSparseAppend],
			PreAvgLatency[ZoneWritableFlushBufferZoneFileBufferdAppend], PreCount[ZoneWritableFlushBufferZoneFileBufferdAppend], PreLatency[ZoneWritableFlushBufferZoneFileBufferdAppend]);
	fprintf(latency_log_file, "                    |  |______________________________                            |              |\n");
	fprintf(latency_log_file, "                    |                                |                   [ZonedWritableFile::FlushBuffer()<a:%3ld,c:%6ld,t:%6ld>]--------------------\n", PreAvgLatency[ZoneWritableFlushBuffer], PreCount[ZoneWritableFlushBuffer], PreLatency[ZoneWritableFlushBuffer]);
	fprintf(latency_log_file, "                    |                                |                                       |                                                         |\n");
	fprintf(latency_log_file, "                    |                                |                        <a:%3ld,c:%6ld,t:%6ld>                                                |\n", PreAvgLatency[ZoneWritableBufferedWriteFlushBuffer], PreCount[ZoneWritableBufferedWriteFlushBuffer], PreLatency[ZoneWritableBufferedWriteFlushBuffer]);
	fprintf(latency_log_file, "    <a:%3ld,c:%6ld,t:%6ld>                        |                                       |                                                         |\n", PreAvgLatency[ZoneWritablePositionedAppendZoneFileAppend], PreCount[ZoneWritablePositionedAppendZoneFileAppend], PreLatency[ZoneWritablePositionedAppendZoneFileAppend]);
	fprintf(latency_log_file, "                    |                                |             [ZonedWritableFile::BufferedWrite()<a:%3ld,c:%8ld,t:%8ld>]                   |\n", PreAvgLatency[ZoneWritableBufferedWrite], PreCount[ZoneWritableBufferedWrite], PreLatency[ZoneWritableBufferedWrite]);
	fprintf(latency_log_file, "                    |             ___________________|________________________|              |                                                         |\n");
	fprintf(latency_log_file, "                    |            |                   |                                       |                                             <a:%3ld,c:%6ld,t:%6ld>\n", PreAvgLatency[ZoneWritableDataSyncFlushBuffer], PreCount[ZoneWritableDataSyncFlushBuffer], PreLatency[ZoneWritableDataSyncFlushBuffer]);
	fprintf(latency_log_file, "                    |  <a:%3ld,c:%6ld,t:%6ld>  <a:%3ld,c:%6ld,t:%6ld>     <a:%3ld,c:%6ld,t:%6ld>                                              |\n", PreAvgLatency[ZoneWritablePositionedAppendBufferedWrite], PreCount[ZoneWritablePositionedAppendBufferedWrite], PreLatency[ZoneWritablePositionedAppendBufferedWrite],PreAvgLatency[ZoneWritableAppendZoneFileAppend], PreCount[ZoneWritableAppendZoneFileAppend], PreLatency[ZoneWritableAppendZoneFileAppend],
			PreAvgLatency[ZoneWritableAppendBufferedWrite], PreCount[ZoneWritableAppendBufferedWrite], PreLatency[ZoneWritableAppendBufferedWrite]);
	fprintf(latency_log_file, "                    |            |                   |________________________________       |                                                         |\n                    |            |                                                    |      |                                                         |\n");
	fprintf(latency_log_file, "[ZonedWritableFile::PositionedAppend()<a:%3ld,c:%6ld,t:%6ld>]            [ZonedWritableFile::Append()<a:%3ld,c:%6ld,t:%6ld>]       [ZonedWritableFile::DataSync()<a:%3ld,c:%6ld,t:%6ld>]\n", PreAvgLatency[ZoneWritablePositionedAppend], PreCount[ZoneWritablePositionedAppend], PreLatency[ZoneWritablePositionedAppend],PreAvgLatency[ZoneWritableAppend], PreCount[ZoneWritableAppend], PreLatency[ZoneWritableAppend],PreAvgLatency[ZoneWritableDataSync], PreCount[ZoneWritableDataSync], PreLatency[ZoneWritableDataSync]);
	fprintf(latency_log_file, "write******************************************************************************************************************************\n");

	fprintf(latency_log_file, "read******************************************************************************************************************************\n");
	fprintf(latency_log_file, "                                                              [pread()<a:%3ld,c:%6ld,t:%6ld>]\n", PreAvgLatency[SystemPread], PreCount[SystemPread], PreLatency[SystemPread]);
	fprintf(latency_log_file, "                                                                         |\n                                                                         |\n");
	fprintf(latency_log_file, "                                                         [ZoneFile::PositionedRead()<a:%3ld,c:%6ld,t:%6ld>]\n", PreAvgLatency[ZoneFilePositionedRead], PreCount[ZoneFilePositionedRead], PreLatency[ZoneFilePositionedRead]);
	fprintf(latency_log_file, "                                                                         |\n");
	fprintf(latency_log_file, "                _________________________________________________________|___________________________________________________________\n");
	fprintf(latency_log_file, "                |                                                        |                                                          |\n");
	fprintf(latency_log_file, "                |                                                        |                                                          |\n");
	fprintf(latency_log_file, "        <a:%3ld,c:%6ld,t:%6ld>                                  <a:%3ld,c:%6ld,t:%6ld>                                    <a:%3ld,c:%6ld,t:%6ld>\n", PreAvgLatency[ZoneSeqReadZoneFilePositionedRead], PreCount[ZoneSeqReadZoneFilePositionedRead], PreLatency[ZoneSeqReadZoneFilePositionedRead],
			PreAvgLatency[ZoneSeqPositionedReadZoneFilePositionedRead], PreCount[ZoneSeqPositionedReadZoneFilePositionedRead], PreLatency[ZoneSeqPositionedReadZoneFilePositionedRead],
			PreAvgLatency[ZoneRandomReadZoneFilePositionedRead], PreCount[ZoneRandomReadZoneFilePositionedRead], PreLatency[ZoneRandomReadZoneFilePositionedRead]);
	fprintf(latency_log_file, "                |                                                        |                                                          |\n");
	fprintf(latency_log_file, "[ZonedSequentialFile::Read()<a:%3ld,c:%6ld,t:%6ld>]  [ZonedSequentialFile::PositionedRead()<a:%3ld,c:%6ld,t:%6ld>]   [ZonedRandomAccessFile::Read()<a:%3ld,c:%6ld,t:%6ld>]\n", PreAvgLatency[ZoneSeqRead], PreCount[ZoneSeqRead], PreLatency[ZoneSeqRead],
			PreAvgLatency[ZoneSeqPositionedRead], PreCount[ZoneSeqPositionedRead], PreLatency[ZoneSeqPositionedRead],
			PreAvgLatency[ZoneRandomRead], PreCount[ZoneRandomRead], PreLatency[ZoneRandomRead]);
	fprintf(latency_log_file, "read******************************************************************************************************************************\n");
}

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
		PreMaxLatency[i] = max_latency;
		PreAvgLatency[i] = average_latency;
		PreCount[i] = reqs;
		PreLatency[i] = latency;

		for (int j = 0; j < US_LATENCY_STEP; j++) {
			LatencyStatLess16UsTmp[i][j] = LatencyStatLess16Us[i][j].fetch_and(0);
		}
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
			for (int j = 0; j < US_LATENCY_STEP; j++) {
				if (LatencyStatLess16UsTmp[i][j]) {
					fprintf(latency_log_file, "%d  us\t\t%lu\n", j, LatencyStatLess16UsTmp[i][j]);
				}
			}
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
		ShowCallTrace();
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


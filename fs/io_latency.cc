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

// pre second total calls
uint64_t PreSecondTotalReqs[TargetEnd] = {};
// pre second total latency
uint64_t PreSecondTotalLatency[TargetEnd] = {};


FILE *latency_log_file = NULL;
std::atomic_flag has_inited(false);

const char* ZenfsGetLatencyTargetName(enum LatencyTargetIndex id)
{
	switch (id) {
	case ZoneFileAppendId:
		return "ZoneFileAppend";
	case SparseAppendId:
		return "SparseAppend";
	case BufferedAppendId:
		return "BufferedAppend";
	case preadId:
		return "pread";
	case pwriteId:
		return "pwrite";
	case PositionedReadId:
		return "PositionedRead";
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

static int second = 0;

static void ZenfsShowLatency()
{
	for (int i = TargetStart + 1; i < TargetEnd; i++) {
		uint64_t total_reqs =TotalReqs[i].load();
		uint64_t total_latency = TotalLatency[i].load();
		uint64_t max_latency = MaxLatency[i].fetch_and(0);

		uint64_t reqs = total_reqs - PreSecondTotalReqs[i];
		uint64_t latency = total_latency - PreSecondTotalLatency[i];
		uint64_t average_latency = latency / (reqs? reqs : 1);

		PreSecondTotalReqs[i] = total_reqs;
		PreSecondTotalLatency[i] = total_latency;

		if (latency_log_file || (reqs != 0)) {
			fprintf(latency_log_file, "time: %d, latency[%s](us) => max: %lu, avg: %lu, count: %lu, total: %lu\n", second,
					ZenfsGetLatencyTargetName((LatencyTargetIndex)i), max_latency, average_latency, reqs, latency);
		}
	}
}
void LoopShowLatency()
{
	for(;;) {
		sleep(1);
		ZenfsShowLatency();
		second++;
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


/*
 * io_latency.h
 *
 *  Created on: 2022-05-17
 *      Author: hml1006
 */

#ifndef PLUGIN_ZENFS_FS_IO_LATENCY_H_
#define PLUGIN_ZENFS_FS_IO_LATENCY_H_

#include <sys/time.h>
#include <cstdio>
#include <cstdint>
#include <atomic>
#include <string>

#define likely(x)       __builtin_expect(!!(x), 1)
#define unlikely(x)     __builtin_expect(!!(x), 0)

enum LatencyTargetIndex {
	TargetStart = 0,

	PushExtentId,
	GetExtent1Id,
	GetExtent2Id,
	ZoneAppendId,
	ZoneFileAppendId,
	SparseAppendId,
	BufferedAppendId,
	preadId,
	pwriteId,
	PositionedReadId,

	TargetEnd // end of target index
};
// total calls
extern std::atomic<uint64_t> TotalReqs[TargetEnd];
// total latency
extern std::atomic<uint64_t> TotalLatency[TargetEnd];
// max latency
extern std::atomic<uint64_t> MaxLatency[TargetEnd];

// latency begin
#define DEBUG_STEP_LATENCY_START(targetId) 	struct timeval targetId##start; do { gettimeofday(&targetId##start, NULL); } while(0)

#define US_PER_SECOND 1000000

extern std::atomic_flag has_inited;
extern void ZenfsLatencyInit();
// latency end
#define DEBUG_STEP_LATENCY_END(targetId)	do { \
		struct timeval targetId##end; \
		gettimeofday(&targetId##end, NULL); \
		TotalReqs[targetId]++; \
		uint64_t us = (targetId##end.tv_sec - targetId##start.tv_sec) * US_PER_SECOND + targetId##end.tv_usec - targetId##start.tv_usec; \
		if (likely(us > 0)) { \
			TotalLatency[targetId] += us; \
			if (us > MaxLatency[targetId]) { \
				MaxLatency[targetId] = us; \
			} \
		} \
		if (unlikely(!has_inited.test_and_set())) { \
			ZenfsLatencyInit(); \
		} \
	} while(0)


#endif /* PLUGIN_ZENFS_FS_IO_LATENCY_H_ */

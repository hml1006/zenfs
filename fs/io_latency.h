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
#include <sys/syscall.h>

#define likely(x)       __builtin_expect(!!(x), 1)
#define unlikely(x)     __builtin_expect(!!(x), 0)

enum LatencyTargetIndex {
	TargetStart = 0,

	ZoneFileAppend,
	ZoneFileSparseAppend,
	ZoneFileBufferedAppend,
	ZoneFilePositionedRead,
	SystemPread,
	SystemPwrite,

	ZoneFileBufferedAppendZoneAppend,
	ZoneFileSparseAppendZoneAppend,
	ZoneFileAppendZoneAppend,

	ZoneWritableBufferedWrite,
	ZoneWritableFlushBuffer,
	ZoneWritableDataSync,
	ZoneWritablePositionedAppend,

	ZoneWritableDataSyncFlushBuffer,
	ZoneWritableFlushBufferZoneFileSparseAppend,
	ZoneWritableFlushBufferZoneFileBufferdAppend,
	ZoneWritableBufferedWriteFlushBuffer,
	ZoneWritableAppendBufferedWrite,
	ZoneWritableAppendZoneFileAppend,
	ZoneWritableAppend,
	ZoneWritablePositionedAppendBufferedWrite,
	ZoneWritablePositionedAppendZoneFileAppend,

	ZoneSeqReadZoneFilePositionedRead,
	ZoneSeqRead,
	ZoneSeqPositionedReadZoneFilePositionedRead,
	ZoneSeqPositionedRead,
	ZoneRandomReadZoneFilePositionedRead,
	ZoneRandomRead,

	TargetEnd // end of target index
};
// total calls
extern std::atomic<uint64_t> TotalReqs[TargetEnd];
// total latency
extern std::atomic<uint64_t> TotalLatency[TargetEnd];

#define US_LATENCY_STEP 16
#define LATENCY_STAT_US_LEN (1024 / US_LATENCY_STEP)
#define LATENCY_STAT_MS_LEN 100
// us latency, 10 us an item
extern std::atomic<uint64_t> LatencyStatUs[TargetEnd][LATENCY_STAT_US_LEN];
// ms latency, 1 ms an item
extern std::atomic<uint64_t> LatencyStatMs[TargetEnd][LATENCY_STAT_MS_LEN];
// greater than 100 ms
extern std::atomic<uint64_t> LatencyStat100Ms[TargetEnd];

#define PWRITE_DATA_ARR_LEN 1025

extern std::atomic<int> PWriteDataLen[PWRITE_DATA_ARR_LEN];
extern std::atomic<int> PWriteDataLen1MB;

#define PWRITE_LEN_STAT(len)	do { \
		if (len <= 1024 * (PWRITE_DATA_ARR_LEN - 1)) { \
			PWriteDataLen[len / 1024]++; \
		} else if (len > 1024 * (PWRITE_DATA_ARR_LEN - 1)) { \
			PWriteDataLen1MB++; \
		} \
	} while(0)

// max latency
extern std::atomic<uint64_t> MaxLatency[TargetEnd];

// latency begin
#define DEBUG_STEP_LATENCY_START(targetId) 	struct timeval targetId##start; do { gettimeofday(&targetId##start, NULL); } while(0)

#define US_PER_SECOND 1000000

extern std::atomic_flag has_inited;
extern void ZenfsLatencyInit();
extern void RecordThreadId();
// latency end
#define DEBUG_STEP_LATENCY_END(targetId)	do { \
		struct timeval targetId##end; \
		gettimeofday(&targetId##end, NULL); \
		int64_t us = (targetId##end.tv_sec - targetId##start.tv_sec) * US_PER_SECOND + targetId##end.tv_usec - targetId##start.tv_usec; \
		if (unlikely(us < 0)) { \
			us = 0; \
		} \
		TotalReqs[targetId]++; \
		TotalLatency[targetId] += us; \
		if ((uint64_t)us > MaxLatency[targetId]) { \
			MaxLatency[targetId] = us; \
		} \
		if (likely(us < 1000)) { \
			LatencyStatUs[targetId][us / US_LATENCY_STEP]++; \
		} else if (us < LATENCY_STAT_MS_LEN * 1000) { \
			LatencyStatMs[targetId][us / 1000]++; \
		} else { \
			LatencyStat100Ms[targetId]++; \
		} \
		if (unlikely(!has_inited.test_and_set())) { \
			ZenfsLatencyInit(); \
		} \
	} while(0)


#endif /* PLUGIN_ZENFS_FS_IO_LATENCY_H_ */

/*
 * stat.hpp
 *
 *  Created on: 2022-05-10
 *      Author: mengliang.huang
 */

#ifndef STAT_HPP_
#define STAT_HPP_

#include <sys/time.h>
#include <unistd.h>
#include <atomic>
#include <ctime>
#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <tuple>
#include <cstring>
#include <cmath>
#include <unordered_map>
#include <map>
#include <cstdio>

namespace shannon {

#define likely(x)       __builtin_expect(!!(x), 1)
#define unlikely(x)     __builtin_expect(!!(x), 0)

class MeasurePoint;
class SecondStat;
class StatItem;
class Statistic;
class Display;

class SecondStat {
public:
	SecondStat(time_t second)
	{
		Clear();
		time_ = second;
	}

	time_t GetTime()
	{
		return time_;
	}

	int GetTotalReq()
	{
		return total_reqs;
	}

	// 根据延迟数量级增加该数量级的计数器
	void IncreaseCount(int usecs)
	{
		total_reqs++;
		total_latency_ += usecs;
		auto it = latency_record_.find(usecs);
		if (it != latency_record_.end()) {
			it->second++;
		} else {
			latency_record_.insert({usecs, 1});
		}
	}

	void Clear()
	{
		latency_record_.clear();
		total_reqs = 0;
		total_latency_ = 0;
	}

	int GetTotalLatency()
	{
		return total_latency_;
	}

	std::tuple<int, int, int, int, int, int> GetLatency()
	{
		int medium = total_reqs / 2;
		int no75th = total_reqs / 4 * 3;
		int no95th = total_reqs / 100 * 95;
		int no99th = total_reqs / 100 * 99;

		int min_latency = 0;
		int medium_latency = 0;
		int no75th_latency = 0;
		int no95th_latency = 0;
		int no99th_latency = 0;

		int num = 0;
		for (auto it = latency_record_.begin(); it != latency_record_.end(); it++) {
			if (it->second > 0) {
				num += it->second;

				if (0 == min_latency) {
					min_latency = it->first;
				}
				if (0 == medium_latency) {
					if (num >= medium) {
						medium_latency = it->first;
					}
				}
				if (0 == no75th_latency) {
					if (num >= no75th) {
						no75th_latency = it->first;
					}
				}
				if (0 == no95th_latency) {
					if (num >= no95th) {
						no95th_latency = it->first;
					}
				}
				if (0 == no99th_latency) {
					if (num >= no99th) {
						no99th_latency = it->first;
					}
				}
			}
		}

		int average = total_latency_ / (total_reqs? total_reqs : 1);
		return {average, min_latency, medium_latency, no75th_latency, no95th_latency, no99th_latency};
	}

private:
	time_t time_; // 哪一秒的数据

	int total_reqs{0};

	uint64_t total_latency_{0};
	std::map<int, int> latency_record_;
};

class StatItem {
public:
	StatItem(std::string stat_name)
	{
		stat_name_ = stat_name;
		start_time_ = time(NULL);
	}
	std::string GetStatName()
	{
		return stat_name_;
	}

	void SetParent(std::shared_ptr<StatItem> parent)
	{
		parent_ = parent;
	}

	std::shared_ptr<StatItem> GetParent()
	{
		return parent_;
	}

	void AddMeasureData(time_t current, int duration_usec)
	{
		std::lock_guard<std::mutex> lock(second_stats_mutex_);
		if (unlikely(second_stats_.find(current) == second_stats_.end())) {
			second_stats_.insert(std::pair<time_t, std::shared_ptr<SecondStat>>(current, std::shared_ptr<SecondStat>(new SecondStat(current))));

			// 删除2秒前的统计数据,避免数据暴涨
			std::vector<time_t> need_rm;
			for (auto it = second_stats_.begin(); it != second_stats_.end(); it++) {
				if (current - it->second->GetTime() > 2) {
					need_rm.push_back(it->first);
				}
			}
			for (auto it = need_rm.begin(); it != need_rm.end(); it++) {
				second_stats_.erase(*it);
			}
		}

		auto second = second_stats_.find(current)->second;
		second->IncreaseCount(duration_usec);
	}

	void RemoveSecondStat(time_t time_point)
	{
		std::lock_guard<std::mutex> lock(second_stats_mutex_);
		second_stats_.erase(time_point);
	}

	std::shared_ptr<SecondStat> GetSecondStat(time_t time_point)
	{
		std::lock_guard<std::mutex> lock(second_stats_mutex_);
		auto it = second_stats_.find(time_point);
		if (it != second_stats_.end()) {
			return it->second;
		}
		return nullptr;
	}

private:
	std::unordered_map<time_t, std::shared_ptr<SecondStat>> second_stats_;
	std::shared_ptr<StatItem> parent_{nullptr};
	std::mutex second_stats_mutex_;
	std::string stat_name_;
	time_t start_time_;
};


class Display {
public:
	Display(std::shared_ptr<StatItem> stat)
	{
		stat_item_ = stat;
		file_ = fdopen(1, "a+");
	}

	void SetOutPut(FILE *file)
	{
		file_ = file;
	}


	void PrintSecond(time_t second)
	{
		if (likely(stat_item_)) {
			auto second_stat = stat_item_->GetSecondStat(second);
			if (likely(second_stat)) {
				int average, min, medium, no75th, no95th, no99th;
				std::tie(average, min, medium, no75th, no95th, no99th) = second_stat->GetLatency();
				if (file_) {
					auto parent = stat_item_->GetParent();
					int percent = 0;
					if (nullptr != parent) {
						auto parent_second_stat = parent->GetSecondStat(second);
						percent =(second_stat->GetTotalLatency() * 1.0 / (parent_second_stat->GetTotalLatency()? parent_second_stat->GetTotalLatency() : 1)) * 100;
					}

					fprintf(file_, "Name: %s  time: %ld, requests: %d, total latency: %d, percent: %d %%, average latency: %d us, min/medium/75th/95th/99th, latency: %d/%d/%d/%d/%d us\n",
							stat_item_->GetStatName().c_str(), second_stat->GetTime(), second_stat->GetTotalReq(), second_stat->GetTotalLatency(), percent, average, min, medium, no75th, no95th, no99th);
				}
			}
		}
	}

private:
	std::shared_ptr<StatItem> stat_item_;
	FILE *file_;
};

class Statistic {
public:
	static void Init()
	{
		Statistic::current_stat_fd = -1;
		for (int i = 0; i < 1024; i++) {
			stats_.push_back(nullptr);
		}
	}

	static void InitWithLog(std::string log)
	{
		Init();
		log_ = log;
	}

	// stat_name 标记一个统计指标
	static int AllocStat(std::string stat_name)
	{
		if (current_stat_fd == (int)(stats_.size() - 1)) {
			return -1;
		}
		if (stat_name.empty()) {
			return -1;
		}
		current_stat_fd++;
		stats_[current_stat_fd] = std::shared_ptr<StatItem>(new StatItem(stat_name));
		return current_stat_fd;
	}

	// 查询统计指标名称
	static std::string GetStatName(int stat)
	{
		if (current_stat_fd >= (int)stats_.size()) {
			return "";
		}
		auto item = stats_[stat];
		if (item != nullptr) {
			return item->GetStatName();
		}

		return "";
	}

	static std::shared_ptr<StatItem> GetStatByFd(int stat_fd)
	{
		if (unlikely(stat_fd >= (int)stats_.size())) {
			return nullptr;
		}
		if (unlikely(stat_fd < 0)) {
			return nullptr;
		}
		return stats_[stat_fd];
	}

	static void LoopStat()
	{
		std::vector<std::shared_ptr<Display>> displays;
		current_display_time_ = time(NULL);
		FILE *log = NULL;
		if (!log_.empty()) {
			log = fopen(log_.c_str(), "a+");
		}
		for (auto it = stats_.begin(); it != stats_.end(); it++) {
			Display *display = new Display(*it);
			if (log) {
				display->SetOutPut(log);
			}
			displays.push_back(std::shared_ptr<Display>(display));
		}

		while(true) {
			usleep(500 * 1000);
			if (time(NULL) - current_display_time_ == 2) {
				for (auto it = displays.begin(); it != displays.end(); it++) {
					(*it)->PrintSecond(current_display_time_);
				}
				current_display_time_++;
			}
		}
	}

private:
	static std::string log_;
	static int current_stat_fd;
	static std::vector<std::shared_ptr<StatItem>> stats_;
	static time_t current_display_time_;
};

// 用计算时间的对象, 析构时间减去构造函数时间为时间间隔
class MeasurePoint {
public:
	MeasurePoint(int stat_fd)
	{
		stat_fd_ = stat_fd;
		gettimeofday(&start_, NULL);
	}
	~MeasurePoint()
	{
		struct timeval end;
		gettimeofday(&end, NULL);

		// 计算耗时
		int sec = end.tv_sec - start_.tv_sec;
		int usec = end.tv_usec - start_.tv_usec;
		int duration_usec = sec * 1000000 + usec;

		// 添加到统计
		auto item = Statistic::GetStatByFd(stat_fd_);
		if (likely(item != nullptr)) {
			item->AddMeasureData(end.tv_sec, duration_usec);
		}
	}
private:
	struct timeval start_;
	int stat_fd_;
};
}


#endif /* STAT_HPP_ */

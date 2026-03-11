//
// Copyright (C) 2015 Yahoo Japan Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#pragma once

#include <string>
#include <vector>
#include <queue>
#include <map>
#include <set>
#include <unordered_set>
#include <unordered_map>
#include <iostream>
#include <sstream>
#include <fstream>
#include <cassert>
#include <cstdlib>
#include <cmath>
#include <cfloat>
#include <climits>
#include <iomanip>
#include <algorithm>
#include <typeinfo>
#include <limits>

#include <execinfo.h>
#include <sys/time.h>
#include <fcntl.h>
#if defined(__linux__)
#include <sys/sysinfo.h>
#endif

#include "NGT/defines.h"
#include "NGT/SharedMemoryAllocator.h"

#ifdef NGT_HALF_FLOAT
#include "NGT/half.hpp"
#endif

#define ADVANCED_USE_REMOVED_LIST
#define SHARED_REMOVED_LIST

namespace NGT {
typedef unsigned int ObjectID;
typedef float Distance;
#ifdef NGT_HALF_FLOAT
typedef half_float::half float16;
#endif

// Aligned memory allocation helpers
inline void *alignedAlloc64(size_t size) {
#if __cplusplus >= 201703L
  return ::operator new(size, std::align_val_t(64));
#else
  void *ptr = nullptr;
  if (posix_memalign(&ptr, 64, size) != 0) {
    throw std::bad_alloc();
  }
  return ptr;
#endif
}

inline void alignedFree64(void *ptr) {
#if __cplusplus >= 201703L
  ::operator delete(ptr, std::align_val_t(64));
#else
  free(ptr);
#endif
}

class quint8 {
 public:
  quint8() {}
  quint8(uint8_t v) : value(v) {}
  quint8 &operator=(uint8_t v) {
    value = v;
    return *this;
  }
  operator uint8_t() const { return value; }
  uint8_t get() { return value; }
  uint8_t value;
};
class qsint8 {
 public:
  qsint8() {}
  qsint8(int8_t v) : value(v) {}
  qsint8 &operator=(int8_t v) {
    value = v;
    return *this;
  }
  operator int8_t() const { return value; }
  int8_t get() { return value; }
  int8_t value;
};

#ifdef NGT_PQ4
class qint4 {
 public:
  qint4() {}
  qint4(uint8_t v) : value(v) {}
  qint4 &operator=(uint8_t &v) {
    value = v;
    return *this;
  }
  uint8_t upper() const { return value >> 4; }
  uint8_t lower() const { return value & 0xF; }
  operator uint8_t() const { return value; }
  uint8_t get() const { return value; }
  friend std::ostream &operator<<(std::ostream &os, qint4 &v) {
    os << static_cast<int>(v.upper()) << ":" << static_cast<int>(v.lower());
    return os;
  }
  uint8_t value;
};
#endif

#ifdef NGT_BFLOAT
class bfloat16 {
 public:
  bfloat16(float fp32) { value = fp32ToBf16(fp32); }
  explicit operator float() const { return bf16ToFp32(value); }
  explicit operator double() const { return static_cast<double>(bf16ToFp32(value)); }
  static uint16_t fp32ToBf16(const float fp32) {
    union {
      float fp;
      uint32_t ui;
    } b32;
    b32.fp = fp32;
    if ((b32.ui & 0xffff) > 0x7fff) {
      b32.ui = (b32.ui >> 16) | 0x1;
    } else {
      b32.ui = b32.ui >> 16;
    }
    return static_cast<uint16_t>(b32.ui & 0xffff);
  }

  static float bf16ToFp32(const uint16_t bf16) {
    union {
      float fp;
      uint32_t ui;
    } b32;
    b32.ui = bf16;
    b32.ui <<= 16;
    b32.ui |= 0x7fff;
    return b32.fp;
  }

  uint16_t value;
};
#endif

#define NGTThrowException(MESSAGE) throw NGT::Exception(__FILE__, __FUNCTION__, (size_t)__LINE__, MESSAGE)
#define NGTThrowSpecificException(MESSAGE, TYPE)                                                             \
  throw NGT::TYPE(__FILE__, __FUNCTION__, (size_t)__LINE__, MESSAGE)

class Exception : public std::exception {
 public:
  Exception() : message("No message") {}
  Exception(const std::string &file, const std::string &function, size_t line, std::stringstream &m) {
    set(file, function, line, m.str());
  }
  Exception(const std::string &file, const std::string &function, size_t line, const std::string &m) {
    set(file, function, line, m);
  }
  void set(const std::string &file, const std::string &function, size_t line, const std::string &m) {
    std::stringstream ss;
    ss << file << ":" << function << ":" << line << ": " << m;
    message = ss.str();
  }
  ~Exception() throw() {}
  Exception &operator=(const Exception &e) {
    message = e.message;
    return *this;
  }
  virtual const char *what() const throw() { return message.c_str(); }
  std::string &getMessage() { return message; }

 protected:
  std::string message;
};

class Args : public std::map<std::string, std::string> {
 public:
  Args() {}
  Args(int argc, char **argv, std::string noVal = "") {
    argC = argc;
    argV = argv;
    parse(noVal);
  }

  void parse(std::string noVal = "") {
    auto argc = argC;
    auto argv = argV;
    clear();
    std::vector<std::string> opts;
    int optcount = 0;
    insert(std::make_pair(std::string("#-"), std::string(argv[0])));
    noVal += "h";
    for (int i = 1; i < argc; ++i) {
      opts.push_back(std::string(argv[i]));
      if ((argv[i][0] == '-') && (noVal.find(argv[i][1]) != std::string::npos)) {
        opts.push_back("t");
      }
    }
    for (auto i = opts.begin(); i != opts.end(); ++i) {
      std::string &opt = *i;
      std::string key, value;
      if (opt.size() > 2 && opt.substr(0, 2) == "--") {
        auto pos = opt.find('=');
        if (pos == std::string::npos) {
          key   = opt.substr(2);
          value = "";
        } else {
          key   = opt.substr(2, pos - 2);
          value = opt.substr(++pos);
        }
      } else if (opt.size() > 1 && opt[0] == '-') {
        if (opt.size() == 2) {
          key = opt[1];
          ++i;
          if (i != opts.end()) {
            value = *i;
          } else {
            value = "";
            --i;
          }
        } else {
          key   = opt[1];
          value = opt.substr(2);
        }
      } else {
        key   = "#" + std::to_string(optcount++);
        value = opt;
      }
      auto status = insert(std::make_pair(key, value));
      if (!status.second) {
        std::cerr << "Args: Duplicated options. [" << opt << "]" << std::endl;
      }
    }
  }

  std::set<std::string> getUnusedOptions() {
    std::set<std::string> o;
    for (auto i = begin(); i != end(); ++i) {
      o.insert((*i).first);
    }
    for (auto i = usedOptions.begin(); i != usedOptions.end(); ++i) {
      o.erase(*i);
    }
    return o;
  }
  std::string checkUnusedOptions() {
    auto uopt = getUnusedOptions();
    std::stringstream msg;
    if (!uopt.empty()) {
      msg << "Unused options: ";
      for (auto i = uopt.begin(); i != uopt.end(); ++i) {
        msg << *i << " ";
      }
    }
    return msg.str();
  }
  std::string &find(const char *s) { return get(s); }
  char getChar(const char *s, char v) {
    try {
      return get(s)[0];
    } catch (...) {
      return v;
    }
  }
  std::string getString(const char *s, const char *v) {
    try {
      return get(s);
    } catch (...) {
      return v;
    }
  }
  std::string &get(const char *s) {
    Args::iterator ai;
    ai = map<std::string, std::string>::find(std::string(s));
    if (ai == this->end()) {
      std::stringstream msg;
      msg << s << ": Not specified" << std::endl;
      NGTThrowException(msg.str());
    }
    usedOptions.insert(ai->first);
    return ai->second;
  }
  bool getBool(const char *s) {
    try {
      get(s);
    } catch (...) {
      return false;
    }
    return true;
  }
  long getl(const char *s, long v) {
    char *e;
    long val;
    try {
      val = strtol(get(s).c_str(), &e, 10);
    } catch (...) {
      return v;
    }
    if (*e != 0) {
      std::stringstream msg;
      msg << "ARGS::getl: Illegal string. Option=-" << s << " Specified value=" << get(s)
          << " Illegal string=" << e << std::endl;
      NGTThrowException(msg.str());
    }
    return val;
  }
  float getf(const char *s, float v) {
    char *e;
    float val;
    try {
      val = strtof(get(s).c_str(), &e);
    } catch (...) {
      return v;
    }
    if (*e != 0) {
      std::stringstream msg;
      msg << "ARGS::getf: Illegal string. Option=-" << s << " Specified value=" << get(s)
          << " Illegal string=" << e << std::endl;
      NGTThrowException(msg.str());
    }
    return val;
  }
  std::set<std::string> usedOptions;
  int argC;
  char **argV;
};

class Timer {
 public:
  Timer() { reset(); }
  void reset() {
    time  = 0;
    ntime = 0;
    count = 0;
  }

  void start() {
    struct timespec res;
    clock_getres(CLOCK_REALTIME, &res);
    reset();
    clock_gettime(CLOCK_REALTIME, &startTime);
  }

  void restart() { clock_gettime(CLOCK_REALTIME, &startTime); }

  void stop() {
    clock_gettime(CLOCK_REALTIME, &stopTime);
    sec  = stopTime.tv_sec - startTime.tv_sec;
    nsec = stopTime.tv_nsec - startTime.tv_nsec;
    if (nsec < 0) {
      sec -= 1;
      nsec += 1000000000L;
    }
    time += (double)sec + (double)nsec / 1000000000.0;
    ntime += sec * 1000000000L + nsec;
    count++;
  }

  void add(Timer &t) {
    time += t.time;
    ntime += t.ntime;
  }

  friend std::ostream &operator<<(std::ostream &os, Timer &t) {
    show(os, t.time);
    if (t.count > 1) {
      os << "*" << t.count;
    }
    return os;
  }

  static void show(std::ostream &os, double time) {
    if (time < 1.0) {
      time *= 1000.0;
      os << std::setprecision(6) << time << " (ms)";
    } else if (time < 60.0) {
      os << std::setprecision(6) << time << " (s)";
    } else {
      time /= 60.0;
      if (time < 60.0) {
        os << std::setprecision(6) << time << " (m)";
      } else {
        time /= 60.0;
        os << std::setprecision(6) << time << " (h)";
      }
    }
  }

  struct timespec startTime;
  struct timespec stopTime;

  int64_t sec;
  int64_t nsec;
  int64_t ntime; // nano second
  double time;   // second
  size_t count;
};

class Timers {
 public:
  Timers() : prev(nullptr) {
    std::string label = "timers initialize";
    size_t n          = 100;
    for (size_t i = 0; i < n; i++) {
      restart(label);
      stop();
    }
    std::cerr << "Timers: Time=" << timers[label].second.time << " Count=" << timers[label].second.count
              << std::endl;
    overHead = timers[label].second.time / timers[label].second.count;
    std::cerr << "Timers: over head=" << overHead << std::endl;
    timers.clear();
  }
  Timer &get(const std::string t) {
    if (timers.count(t) == 0) {
      timers[t].first = timers.size();
    }
    prev = &timers[t].second;
    return *prev;
  }
  Timer &get() {
    if (prev != nullptr) {
      return *prev;
    } else {
      return get("*");
    }
  }
  void start(const std::string &t) { get(t).start(); }
  void start() { get().start(); }
  void restart(const std::string &t) { get(t).restart(); }
  void restart() { get().restart(); }
  void stop(const std::string &t) { get(t).stop(); }
  void stop() { get().stop(); }
  void show(std::ostream &os, bool timeSort = true) {
    double total = 0.0;
    std::vector<std::pair<std::string, std::pair<int, NGT::Timer>>> ts;
    for (auto i = timers.begin(); i != timers.end(); ++i) {
      std::pair<std::string, std::pair<int, NGT::Timer>> time;
      time                    = *i;
      double oh               = overHead * time.second.second.count;
      time.second.second.time = time.second.second.time - oh < 0.0 ? 0.0 : time.second.second.time - oh;
      total += time.second.second.time;
      ts.emplace_back(time);
    }
    if (timeSort) {
      std::sort(ts.begin(), ts.end(),
                [](const std::pair<std::string, std::pair<int, NGT::Timer>> &a,
                   const std::pair<std::string, std::pair<int, NGT::Timer>> &b) {
                  return a.second.second.time > b.second.second.time;
                });
    } else {
      std::sort(ts.begin(), ts.end(),
                [](const std::pair<std::string, std::pair<int, NGT::Timer>> &a,
                   const std::pair<std::string, std::pair<int, NGT::Timer>> &b) {
                  return a.second.first < b.second.first;
                });
    }
    for (auto i = ts.begin(); i != ts.end(); ++i) {
      os << (*i).second.first << "\t" << (*i).second.second << "\t" << (*i).first << std::endl;
    }
    os << "Total time=";
    Timer::show(os, total);
    os << std::endl;
  }
  std::unordered_map<std::string, std::pair<int, NGT::Timer>> timers;
  Timer *prev;
  double overHead;
};

class TimersProvider {
 public:
  static Timers &getTimers() {
    static Timers timers;
    return timers;
  }
};

class Common {
 public:
  static void tokenize(const std::string &str, std::vector<std::string> &token, const std::string seps) {
    std::string::size_type current = 0;
    std::string::size_type next;
    while ((next = str.find_first_of(seps, current)) != std::string::npos) {
      token.push_back(str.substr(current, next - current));
      current = next + 1;
    }
    std::string t = str.substr(current);
    token.push_back(t);
  }

  static double strtod(const std::string &str) {
    char *e;
    double val = std::strtod(str.c_str(), &e);
    if (*e != 0) {
      std::stringstream msg;
      msg << "Invalid string. " << e;
      NGTThrowException(msg);
    }
    return val;
  }

  static float strtof(const std::string &str) {
    char *e;
    double val = std::strtof(str.c_str(), &e);
    if (*e != 0) {
      std::stringstream msg;
      msg << "Invalid string. " << e;
      NGTThrowException(msg);
    }
    return val;
  }

  static long strtol(const std::string &str, int base = 10) {
    char *e;
    long val = std::strtol(str.c_str(), &e, base);
    if (*e != 0) {
      std::stringstream msg;
      msg << "Invalid string. " << e;
      NGTThrowException(msg);
    }
    return val;
  }

#if defined(__linux__)
  static unsigned long getTotalRam() {
    struct sysinfo info;
    sysinfo(&info);
    return info.totalram;
  }
#endif

  template <typename T>
  static void extractVector(const std::string &textLine, const std::string &sep, T &object) {
    std::vector<std::string> tokens;
    NGT::Common::tokenize(textLine, tokens, sep);
    size_t idx;
    for (idx = 0; idx < tokens.size(); idx++) {
      if (tokens[idx].size() == 0) {
        if (idx + 1 == tokens.size()) {
          break;
        }
        std::stringstream msg;
        msg << "Common::extractVector: No data. sep=(" << sep << "):" << idx << ": " << textLine;
        NGTThrowException(msg);
      }
      char *e;
      double v = ::strtod(tokens[idx].c_str(), &e);
      if (*e != 0) {
        std::cerr << "Common::extractVector: Warning! Not numerical value. [" << e << "] " << std::endl;
        break;
      }
      object.push_back(v);
    }
  }

  static std::string getProcessStatus(const std::string &stat) {
    pid_t pid = getpid();
    std::stringstream str;
    str << "/proc/" << pid << "/status";
    std::ifstream procStatus(str.str());
    if (!procStatus.fail()) {
      std::string line;
      while (getline(procStatus, line)) {
        std::vector<std::string> tokens;
        NGT::Common::tokenize(line, tokens, ": \t");
        if (tokens[0] == stat) {
          for (size_t i = 1; i < tokens.size(); i++) {
            if (tokens[i].empty()) {
              continue;
            }
            return tokens[i];
          }
        }
      }
    }
    return "-1";
  }

  // size unit is kbyte
  static int getProcessVmSize() { return strtol(getProcessStatus("VmSize")); }
  static int getProcessVmPeak() { return strtol(getProcessStatus("VmPeak")); }
  static int getProcessVmRSS() { return strtol(getProcessStatus("VmRSS")); }
  static int getProcessVmHWM() { return strtol(getProcessStatus("VmHWM")); }
  static int getSystemHWM() {
#if defined(__linux__)
    struct sysinfo info;
    sysinfo(&info);
    return info.totalram / 1024;
#else
    return 0;
#endif
  }
  static std::string sizeToString(float size) {
    char unit = 'K';
    if (size > 1024) {
      size /= 1024;
      unit = 'M';
    }
    if (size > 1024) {
      size /= 1024;
      unit = 'G';
    }
    size = round(size * 100) / 100;
    std::stringstream str;
    str << size << " " << unit;
    return str.str();
  }

  static std::string getProcessVmSizeStr() { return sizeToString(getProcessVmSize()); }
  static std::string getProcessVmPeakStr() { return sizeToString(getProcessVmPeak()); }
  static std::string getProcessVmRSSStr() { return sizeToString(getProcessVmRSS()); }
  static std::string getProcessVmHWMStr() { return sizeToString(getProcessVmHWM()); }
  static std::string getSystemHWMStr() { return sizeToString(getSystemHWM()); }
};

class CpuInfo {
 public:
  enum SimdType {
    SimdTypeAVX             = 0,
    SimdTypeAVX2            = 1,
    SimdTypeAVX512F         = 2,
    SimdTypeAVX512VL        = 3,
    SimdTypeAVX512BW        = 4,
    SimdTypeAVX512DQ        = 5,
    SimdTypeAVX512CD        = 6,
    SimdTypeAVX512ER        = 7,
    SimdTypeAVX512PF        = 8,
    SimdTypeAVX512VBMI      = 9,
    SimdTypeAVX512IFMA      = 10,
    SimdTypeAVX5124VNNIW    = 11,
    SimdTypeAVX5124FMAPS    = 12,
    SimdTypeAVX512VPOPCNTDQ = 13,
    SimdTypeAVX512VBMI2     = 14,
    SimdTypeAVX512VNNI      = 15
  };
  CpuInfo() {}
  static bool is(SimdType type) {
#ifndef __BUILTIN_CPU_SUPPORTS__
    return true;
#else
    __builtin_cpu_init();
    switch (type) {
#if defined(__AVX__)
    case SimdTypeAVX: return __builtin_cpu_supports("avx") > 0; break;
#endif
#if defined(__AVX2__)
    case SimdTypeAVX2: return __builtin_cpu_supports("avx2") > 0; break;
#endif
#if defined(__AVX512F__)
    case SimdTypeAVX512F: return __builtin_cpu_supports("avx512f") > 0; break;
#endif
#if defined(__AVX512VL__)
    case SimdTypeAVX512VL: return __builtin_cpu_supports("avx512vl") > 0; break;
#endif
#if defined(__AVX512BW__)
    case SimdTypeAVX512BW: return __builtin_cpu_supports("avx512bw") > 0; break;
#endif
#if defined(__AVX512DQ__)
    case SimdTypeAVX512DQ: return __builtin_cpu_supports("avx512dq") > 0; break;
#endif
#if defined(__AVX512CD__)
    case SimdTypeAVX512CD: return __builtin_cpu_supports("avx512cd") > 0; break;
#endif
#if defined(__AVX512ER__)
    case SimdTypeAVX512ER: return __builtin_cpu_supports("avx512er") > 0; break;
#endif
#if defined(__AVX512PF__)
    case SimdTypeAVX512PF: return __builtin_cpu_supports("avx512pf") > 0; break;
#endif
#if defined(__AVX512VBMI__)
    case SimdTypeAVX512VBMI: return __builtin_cpu_supports("avx512vbmi") > 0; break;
#endif
#if defined(__AVX512IFMA__)
    case SimdTypeAVX512IFMA: return __builtin_cpu_supports("avx512ifma") > 0; break;
#endif
#if defined(__AVX5124VNNIW__)
    case SimdTypeAVX5124VNNIW: return __builtin_cpu_supports("avx5124vnniw") > 0; break;
#endif
#if defined(__AVX5124FMAPS__)
    case SimdTypeAVX5124FMAPS: return __builtin_cpu_supports("avx5124fmaps") > 0; break;
#endif
#if defined(__AVX512VPOPCNTDQ__)
    case SimdTypeAVX512VPOPCNTDQ: return __builtin_cpu_supports("avx512vpopcntdq") > 0; break;
#endif
#if defined(__AVX512VBMI2__)
    case SimdTypeAVX512VBMI2: return __builtin_cpu_supports("avx512vbmi2") > 0; break;
#endif
#if defined(__AVX512VNNI__)
    case SimdTypeAVX512VNNI: return __builtin_cpu_supports("avx512vnni") > 0; break;
#endif
    default: break;
    }
    return false;
#endif
  }
  static bool isAVX512() { return is(SimdTypeAVX512F); };
  static bool isAVX2() { return is(SimdTypeAVX2); };
  static void showSimdTypes() { std::cout << getSupportedSimdTypes() << std::endl; }
  static std::string getSupportedSimdTypes() {
    static constexpr char const *simdTypes[] = {
        "avx",        "avx2",         "avx512f",      "avx512vl",        "avx512bw",
        "avx512dq",   "avx512cd",     "avx512er",     "avx512pf",        "avx512vbmi",
        "avx512ifma", "avx5124vnniw", "avx5124fmaps", "avx512vpopcntdq", "avx512vbmi2",
        "avx512vnni"};
    std::string types;
    int size = sizeof(simdTypes) / sizeof(simdTypes[0]);
    for (int i = 0; i < size; i++) {
      if (is(static_cast<SimdType>(i))) {
        types += simdTypes[i];
      }
      if (i != size) {
        types += " ";
      }
    }
    return types;
  }
};

class StdOstreamRedirector {
 public:
  StdOstreamRedirector(bool e = false, const std::string path = "/dev/null",
                       mode_t m = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH, int f = 2) {
    logFilePath = path;
    mode        = m;
    logFD       = -1;
    fdNo        = f;
    enabled     = e;
  }
  ~StdOstreamRedirector() { end(); }

  void enable() { enabled = true; }
  void disable() { enabled = false; }
  void set(bool e) { enabled = e; }
  void bgin(bool e) {
    set(e);
    begin();
  }
  void begin() {
    if (!enabled) {
      return;
    }
    if (logFilePath == "/dev/null") {
      logFD = open(logFilePath.c_str(), O_WRONLY | O_APPEND, mode);
    } else {
      logFD = open(logFilePath.c_str(), O_CREAT | O_WRONLY | O_APPEND, mode);
    }
    if (logFD < 0) {
      std::cerr << "Logger: Cannot begin logging." << std::endl;
      logFD = -1;
      return;
    }
    savedFdNo = dup(fdNo);
    std::cerr << std::flush;
    dup2(logFD, fdNo);
  }

  void end() {
    if (logFD < 0) {
      return;
    }
    std::cerr << std::flush;
    dup2(savedFdNo, fdNo);
    close(savedFdNo);
    savedFdNo = -1;
    close(logFD);
    logFD = -1;
  }

  std::string logFilePath;
  mode_t mode;
  int logFD;
  int savedFdNo;
  int fdNo;
  bool enabled;
};

template <class TYPE> class CompactVector {
 public:
  typedef TYPE *iterator;

  CompactVector() : vector(0), vectorSize(0), allocatedSize(0) {}
  virtual ~CompactVector() { clear(); }

  void clear() {
    if (vector != 0) {
      delete[] vector;
    }
    vector        = 0;
    vectorSize    = 0;
    allocatedSize = 0;
  }

  TYPE &front() { return vector[0]; }
  TYPE &back() { return vector[vectorSize - 1]; }
  bool empty() { return vector == 0; }
  iterator begin() { return &(vector[0]); }
  iterator end() { return begin() + vectorSize; }
  TYPE &operator[](size_t idx) const { return vector[idx]; }

  CompactVector &operator=(CompactVector<TYPE> &v) {
    assert((vectorSize == v.vectorSize) || (vectorSize == 0));
    if (vectorSize == v.vectorSize) {
      for (size_t i = 0; i < vectorSize; i++) {
        vector[i] = v[i];
      }
      return *this;
    } else {
      reserve(v.vectorSize);
      assert(allocatedSize >= v.vectorSize);
      for (size_t i = 0; i < v.vectorSize; i++) {
        push_back(v.at(i));
      }
      vectorSize = v.vectorSize;
      assert(vectorSize == v.vectorSize);
    }
    return *this;
  }

  TYPE &at(size_t idx) const {
    if (idx >= vectorSize) {
      std::stringstream msg;
      msg << "CompactVector: beyond the range. " << idx << ":" << vectorSize;
      NGTThrowException(msg);
    }
    return vector[idx];
  }

  iterator erase(iterator b, iterator e) {
    iterator ret;
    e = end() < e ? end() : e;
    for (iterator i = b; i < e; i++) {
      ret = erase(i);
    }
    return ret;
  }

  iterator erase(iterator i) {
    iterator back = i;
    vectorSize--;
    iterator e = end();
    for (; i < e; i++) {
      *i = *(i + 1);
    }
    return ++back;
  }

  void pop_back() {
    if (vectorSize > 0) {
      vectorSize--;
    }
  }

  iterator insert(iterator &i, const TYPE &data) {
    if (size() == 0) {
      push_back(data);
      return end();
    }
    off_t oft = i - begin();
    extend();
    i          = begin() + oft;
    iterator b = begin();
    for (iterator ci = end(); ci > i && ci != b; ci--) {
      *ci = *(ci - 1);
    }
    *i = data;
    vectorSize++;
    return i + 1;
  }

  void push_back(const TYPE &data) {
    extend();
    vector[vectorSize] = data;
    vectorSize++;
  }

  void reserve(size_t s) {
    if (s <= allocatedSize) {
      return;
    } else {
      TYPE *newptr = new TYPE[s];
      TYPE *dstptr = newptr;
      TYPE *srcptr = vector;
      TYPE *endptr = srcptr + vectorSize;
      while (srcptr < endptr) {
        *dstptr++ = *srcptr;
        (*srcptr).~TYPE();
        srcptr++;
      }
      allocatedSize = s;
      if (vector != 0) {
        delete[] vector;
      }
      vector = newptr;
    }
  }

  void resize(size_t s, TYPE v = TYPE()) {
    if (s > allocatedSize) {
      size_t asize = allocatedSize == 0 ? 1 : allocatedSize;
      while (asize < s) {
        asize <<= 1;
      }
      reserve(asize);
      TYPE *base   = vector;
      TYPE *dstptr = base + vectorSize;
      TYPE *endptr = base + s;
      for (; dstptr < endptr; dstptr++) {
        *dstptr = v;
      }
    }
    vectorSize = s;
  }

  size_t size() const { return (size_t)vectorSize; }

  void extend() { extend(vectorSize); }

  void extend(size_t idx) {
    if (idx >= allocatedSize) {
      uint64_t size = allocatedSize == 0 ? 1 : allocatedSize;
      do {
        size <<= 1;
      } while (size <= idx);
      if (size > 0xffff) {
        std::cerr << "CompactVector is too big. " << size << std::endl;
        abort();
      }
      reserve(size);
    }
  }

  TYPE *vector;
  uint16_t vectorSize;
  uint16_t allocatedSize;
};

class CompactString {
 public:
  CompactString() : vector(0) {}

  CompactString(const CompactString &v) : vector(0) { *this = v; }

  ~CompactString() { clear(); }

  void clear() {
    if (vector != 0) {
      delete[] vector;
    }
    vector = 0;
  }

  CompactString &operator=(const std::string &v) { return *this = v.c_str(); }

  CompactString &operator=(const CompactString &v) { return *this = v.vector; }

  CompactString &operator=(const char *str) {
    if (str == 0 || strlen(str) == 0) {
      clear();
      return *this;
    }
    if (size() != strlen(str)) {
      clear();
      vector = new char[strlen(str) + 1];
    }
    strcpy(vector, str);
    return *this;
  }

  char &at(size_t idx) const {
    if (idx >= size()) {
      NGTThrowException("CompactString: beyond the range");
    }
    return vector[idx];
  }

  char *c_str() { return vector; }
  size_t size() const {
    if (vector == 0) {
      return 0;
    } else {
      return (size_t)strlen(vector);
    }
  }

  char *vector;
};

// BooleanSet has been already optimized.
class BooleanSet {
 public:
  BooleanSet(size_t s) {
    size = (s >> 6) + 1; // 2^6=64
    size = ((size >> 2) << 2) + 4;
    bitvec.resize(size);
  }
  inline uint64_t getBitString(size_t i) { return (uint64_t)1 << (i & (64 - 1)); }
  inline uint64_t &getEntry(size_t i) { return bitvec[i >> 6]; }
  inline bool operator[](size_t i) { return (getEntry(i) & getBitString(i)) != 0; }
  inline void set(size_t i) { getEntry(i) |= getBitString(i); }
  inline void insert(size_t i) { set(i); }
  inline void reset(size_t i) { getEntry(i) &= ~getBitString(i); }
  std::vector<uint64_t> bitvec;
  uint64_t size;
};

template <typename TYPE = uint8_t> class BooleanVectorByEpoch {
 public:
  BooleanVectorByEpoch(size_t s = 0x100000) {
    size  = 1u << (32 - __builtin_clz(s - 1));
    array = new TYPE[size]();
    clear();
    epoch = 0;
    mask  = size - 1;
  }
  ~BooleanVectorByEpoch() { delete[] array; }
  void clear() { memset(array, 0, size * sizeof(TYPE)); };
  void reset() {
    constexpr size_t max = sizeof(TYPE) * 0x100 - 1;
    if (epoch == max) {
      clear();
      epoch = 0;
    }
    epoch += 1;
  }
  bool visit(uint32_t id) {
    id &= mask;
    bool stat = (array[id] == epoch);
    array[id] = epoch;
    return stat;
  };
  TYPE *array;
  size_t epoch = 1;
  size_t size;
  size_t mask;
};

class PropertySet : public std::map<std::string, std::string> {
 public:
  void set(const std::string &key, const std::string &value) {
    iterator it = find(key);
    if (it == end()) {
      insert(std::pair<std::string, std::string>(key, value));
    } else {
      (*it).second = value;
    }
  }
  template <class VALUE_TYPE> void set(const std::string &key, VALUE_TYPE value) {
    std::stringstream vstr;
    auto precision = vstr.precision();
    vstr << std::setprecision(7);
    vstr << value;
    iterator it = find(key);
    if (it == end()) {
      insert(std::pair<std::string, std::string>(key, vstr.str()));
    } else {
      (*it).second = vstr.str();
    }
    vstr << std::setprecision(precision);
  }

  std::string get(const std::string &key) {
    iterator it = find(key);
    if (it != end()) {
      return it->second;
    }
    return "";
  }
  float getf(const std::string &key, float defvalue) {
    iterator it = find(key);
    if (it != end()) {
      char *e   = 0;
      float val = strtof(it->second.c_str(), &e);
      if (*e != 0) {
        std::cerr << "Warning: Illegal property. " << key << ":" << it->second << " (" << e << ")"
                  << std::endl;
        return defvalue;
      }
      return val;
    }
    return defvalue;
  }
  void updateAndInsert(PropertySet &prop) {
    for (std::map<std::string, std::string>::iterator i = prop.begin(); i != prop.end(); ++i) {
      set((*i).first, (*i).second);
    }
  }
  long getl(const std::string &key, long defvalue) {
    iterator it = find(key);
    if (it != end()) {
      char *e   = 0;
      float val = strtol(it->second.c_str(), &e, 10);
      if (*e != 0) {
        std::cerr << "Warning: Illegal property. " << key << ":" << it->second << " (" << e << ")"
                  << std::endl;
      }
      return val;
    }
    return defvalue;
  }
  void load(const std::string &f) {
    std::ifstream st(f);
    if (!st) {
      std::stringstream msg;
      msg << "PropertySet::load: Cannot load the property file " << f << ".";
      NGTThrowException(msg);
    }
    load(st);
  }
  void save(const std::string &f) {
    std::ofstream st(f);
    if (!st) {
      std::stringstream msg;
      msg << "PropertySet::save: Cannot save. " << f << std::endl;
      NGTThrowException(msg);
    }
    save(st);
  }
  void save(std::ofstream &os) {
    for (std::map<std::string, std::string>::iterator i = this->begin(); i != this->end(); i++) {
      os << i->first << "\t" << i->second << std::endl;
    }
  }
  void load(std::ifstream &is) {
    std::string line;
    while (getline(is, line)) {
      std::vector<std::string> tokens;
      NGT::Common::tokenize(line, tokens, "\t");
      if (tokens.size() != 2) {
        std::cerr << "Property file is illegal. " << line << std::endl;
        continue;
      }
      set(tokens[0], tokens[1]);
    }
  }
};

namespace Serializer {
static inline void read(std::istream &is, uint8_t *v, size_t s) { is.read((char *)v, s); }

static inline void write(std::ostream &os, const uint8_t *v, size_t s) { os.write((const char *)v, s); }

template <typename TYPE> void write(std::ostream &os, const TYPE v) {
  os.write((const char *)&v, sizeof(TYPE));
}

template <typename TYPE> void writeAsText(std::ostream &os, const TYPE v) {
  if (typeid(TYPE) == typeid(unsigned char)) {
    os << (int)v;
  } else {
    os << v;
  }
}

template <typename TYPE> void read(std::istream &is, TYPE &v) { is.read((char *)&v, sizeof(TYPE)); }

template <typename TYPE> void readAsText(std::istream &is, TYPE &v) {
  if (typeid(TYPE) == typeid(unsigned char)) {
    unsigned int tmp;
    is >> tmp;
    if (tmp > 255) {
      std::cerr << "Error! Invalid. " << tmp << std::endl;
    }
    v = (TYPE)tmp;
  } else {
    is >> v;
  }
}

template <typename TYPE> void write(std::ostream &os, const std::vector<TYPE> &v) {
  unsigned int s = v.size();
  write(os, s);
  for (unsigned int i = 0; i < s; i++) {
    write(os, v[i]);
  }
}

template <typename TYPE> void writeAsText(std::ostream &os, const std::vector<TYPE> &v) {
  unsigned int s = v.size();
  os << s << " ";
  for (unsigned int i = 0; i < s; i++) {
    writeAsText(os, v[i]);
    os << " ";
  }
}

template <typename TYPE> void write(std::ostream &os, const CompactVector<TYPE> &v) {
  unsigned int s = v.size();
  write(os, s);
  for (unsigned int i = 0; i < s; i++) {
    write(os, v[i]);
  }
}

template <typename TYPE> void writeAsText(std::ostream &os, const CompactVector<TYPE> &v) {
  unsigned int s = v.size();
  for (unsigned int i = 0; i < s; i++) {
    writeAsText(os, v[i]);
    os << " ";
  }
}

template <typename TYPE> void writeAsText(std::ostream &os, TYPE *v, size_t s) {
  os << s << " ";
  for (unsigned int i = 0; i < s; i++) {
    writeAsText(os, v[i]);
    os << " ";
  }
}

template <typename TYPE> void write(std::ostream &os, const std::vector<std::vector<TYPE>> &v) {
  unsigned int s = v.size();
  write(os, s);
  for (unsigned int i = 0; i < s; i++) {
    Serializer::write(os, v[i]);
  }
}

template <typename TYPE> void writeAsText(std::ostream &os, const std::vector<std::vector<TYPE>> &v) {
  unsigned int s = v.size();
  os << s << " ";
  for (unsigned int i = 0; i < s; i++) {
    Serializer::writeAsText(os, v[i]);
    os << " ";
  }
}

template <typename TYPE> void read(std::istream &is, std::vector<TYPE> &v) {
  v.clear();
  unsigned int s;
  read(is, s);
  v.reserve(s);
  for (unsigned int i = 0; i < s; i++) {
    TYPE val;
    read(is, val);
    v.push_back(val);
  }
}

template <typename TYPE> void readAsText(std::istream &is, std::vector<TYPE> &v) {
  v.clear();
  unsigned int s;
  is >> s;
  for (unsigned int i = 0; i < s; i++) {
    TYPE val;
    Serializer::readAsText(is, val);
    v.push_back(val);
  }
}

template <typename TYPE> void read(std::istream &is, CompactVector<TYPE> &v) {
  v.clear();
  unsigned int s;
  read(is, s);
  v.reserve(s);
  for (unsigned int i = 0; i < s; i++) {
    TYPE val;
    read(is, val);
    v.push_back(val);
  }
}

template <typename TYPE> void readAsText(std::istream &is, CompactVector<TYPE> &v) {
  v.clear();
  unsigned int s;
  is >> s;
  for (unsigned int i = 0; i < s; i++) {
    TYPE val;
    Serializer::readAsText(is, val);
    v.push_back(val);
  }
}

template <typename TYPE> void readAsText(std::istream &is, TYPE *v, size_t s) {
  unsigned int size;
  is >> size;
  if (s != size) {
    std::cerr << "readAsText: something wrong. " << size << ":" << s << std::endl;
    return;
  }
  for (unsigned int i = 0; i < s; i++) {
    TYPE val;
    Serializer::readAsText(is, val);
    v[i] = val;
  }
}

template <typename TYPE> void read(std::istream &is, std::vector<std::vector<TYPE>> &v) {
  v.clear();
  unsigned int s;
  read(is, s);
  v.resize(s);
  for (unsigned int i = 0; i < s; i++) {
    Serializer::read(is, v[i]);
  }
}

template <typename TYPE> void readAsText(std::istream &is, std::vector<std::vector<TYPE>> &v) {
  v.clear();
  unsigned int s;
  is >> s;
  v.resize(s);
  for (unsigned int i = 0; i < s; i++) {
    Serializer::readAsText(is, v[i]);
  }
}

} // namespace Serializer

class ObjectSpace;

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
template <class TYPE> class Vector {
 public:
  typedef TYPE *iterator;

  Vector() : vector(0), vectorSize(0), allocatedSize(0) {}

  void clear(SharedMemoryAllocator &allocator) {
    if (vector != 0) {
      allocator.free(allocator.getAddr(vector));
    }
    vector        = 0;
    vectorSize    = 0;
    allocatedSize = 0;
  }

  TYPE &front(SharedMemoryAllocator &allocator) { return (*this).at(0, allocator); }
  TYPE &back(SharedMemoryAllocator &allocator) { return (*this).at(vectorSize - 1, allocator); }
  bool empty() { return vectorSize == 0; }
  iterator begin(SharedMemoryAllocator &allocator) { return (TYPE *)allocator.getAddr((off_t)vector); }
  iterator end(SharedMemoryAllocator &allocator) { return begin(allocator) + vectorSize; }

  Vector &operator=(Vector<TYPE> &v) {
    assert((vectorSize == v.vectorSize) || (vectorSize == 0));
    if (vectorSize == v.vectorSize) {
      for (size_t i = 0; i < vectorSize; i++) {
        (*this)[i] = v[i];
      }
      return *this;
    } else {
      reserve(v.vectorSize);
      assert(allocatedSize >= v.vectorSize);
      for (size_t i = 0; i < v.vectorSize; i++) {
        push_back(v.at(i));
      }
      vectorSize = v.vectorSize;
      assert(vectorSize == v.vectorSize);
    }
    return *this;
  }

  TYPE &at(size_t idx, SharedMemoryAllocator &allocator) {
    if (idx >= vectorSize) {
      std::stringstream msg;
      msg << "Vector: beyond the range. " << idx << ":" << vectorSize;
      NGTThrowException(msg);
    }
    return *(begin(allocator) + idx);
  }

  iterator erase(iterator b, iterator e, SharedMemoryAllocator &allocator) {
    iterator ret;
    e = end(allocator) < e ? end(allocator) : e;
    for (iterator i = b; i < e; i++) {
      ret = erase(i, allocator);
    }
    return ret;
  }

  iterator erase(iterator i, SharedMemoryAllocator &allocator) {
    iterator back = i;
    vectorSize--;
    iterator e = end(allocator);
    for (; i < e; i++) {
      *i = *(i + 1);
    }
    return back;
  }

  void pop_back() {
    if (vectorSize > 0) {
      vectorSize--;
    }
  }

  iterator insert(iterator &i, const TYPE &data, SharedMemoryAllocator &allocator) {
    if (size() == 0) {
      push_back(data, allocator);
      return end(allocator);
    }
    off_t oft = i - begin(allocator);
    extend(allocator);
    i          = begin(allocator) + oft;
    iterator b = begin(allocator);
    for (iterator ci = end(allocator); ci > i && ci != b; ci--) {
      *ci = *(ci - 1);
    }
    *i = data;
    vectorSize++;
    return i + 1;
  }

  void push_back(const TYPE &data, SharedMemoryAllocator &allocator) {
    extend(allocator);
    vectorSize++;
    (*this).at(vectorSize - 1, allocator) = data;
  }

  void reserve(size_t s, SharedMemoryAllocator &allocator) {
    if (s <= allocatedSize) {
      return;
    } else {
      TYPE *newptr = new (allocator) TYPE[s];
      TYPE *dstptr = newptr;
      TYPE *srcptr = (TYPE *)allocator.getAddr(vector);
      TYPE *endptr = srcptr + vectorSize;
      while (srcptr < endptr) {
        *dstptr++ = *srcptr;
        (*srcptr).~TYPE();
        srcptr++;
      }
      allocatedSize = s;
      if (vector != 0) {
        allocator.free(allocator.getAddr(vector));
      }
      vector = allocator.getOffset(newptr);
    }
  }

  void resize(size_t s, SharedMemoryAllocator &allocator, TYPE v = TYPE()) {
    if (s > allocatedSize) {
      size_t asize = allocatedSize == 0 ? 1 : allocatedSize;
      while (asize < s) {
        asize <<= 1;
      }
      reserve(asize, allocator);
      TYPE *base   = (TYPE *)allocator.getAddr(vector);
      TYPE *dstptr = base + vectorSize;
      TYPE *endptr = base + s;
      for (; dstptr < endptr; dstptr++) {
        *dstptr = v;
      }
    }
    vectorSize = s;
  }

  void serializeAsText(std::ostream &os, ObjectSpace *objectspace = 0) {
    unsigned int s = size();
    os << s << " ";
    for (unsigned int i = 0; i < s; i++) {
      Serializer::writeAsText(os, (*this)[i]);
      os << " ";
    }
  }
  void deserializeAsText(std::istream &is, ObjectSpace *objectspace = 0) {
    clear();
    size_t s;
    Serializer::readAsText(is, s);
    resize(s);
    for (unsigned int i = 0; i < s; i++) {
      Serializer::readAsText(is, (*this)[i]);
    }
  }
  size_t size() { return vectorSize; }

 public:
  void extend(SharedMemoryAllocator &allocator) { extend(vectorSize, allocator); }

  void extend(size_t idx, SharedMemoryAllocator &allocator) {
    if (idx >= allocatedSize) {
      uint64_t size = allocatedSize == 0 ? 1 : allocatedSize;
      do {
        size <<= 1;
      } while (size <= idx);
      if (size > 0xffffffff) {
        std::cerr << "Vector is too big. " << size << std::endl;
        abort();
      }
      reserve(size, allocator);
    }
  }

  off_t vector;
  uint32_t vectorSize;
  uint32_t allocatedSize;
};
#endif

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
template <class TYPE> class DynamicLengthVector {
 public:
  class iterator {
   public:
    iterator(void *ptr, uint32_t size) : i(static_cast<uint8_t *>(ptr)), elementSize(size) {}
    iterator operator+(size_t c) { return iterator(i + c * elementSize, elementSize); }
    iterator operator++(int c) {
      i += elementSize;
      return iterator(i, elementSize);
    }
    bool operator<(const iterator &it) const { return i < it.i; }
    TYPE &operator*() { return *reinterpret_cast<TYPE *>(i); }

    uint8_t *i;
    uint32_t elementSize;
  };

  DynamicLengthVector() : vector(0), vectorSize(0), allocatedSize(0), elementSize(0) {}
  ~DynamicLengthVector() {}

  void clear(SharedMemoryAllocator &allocator) {
    if (vector != 0) {
      allocator.free(allocator.getAddr(vector));
    }
    vector        = 0;
    vectorSize    = 0;
    allocatedSize = 0;
  }

  TYPE &front(SharedMemoryAllocator &allocator) { return (*this).at(0, allocator); }
  TYPE &back(SharedMemoryAllocator &allocator) { return (*this).at(vectorSize - 1, allocator); }
  bool empty() { return vectorSize == 0; }
  iterator begin(SharedMemoryAllocator &allocator) {
    return iterator(allocator.getAddr((off_t)vector), elementSize);
  }
  iterator end(SharedMemoryAllocator &allocator) { return begin(allocator) + vectorSize; }

  DynamicLengthVector &operator=(DynamicLengthVector<TYPE> &v) {
    std::cerr << "DynamicLengthVector cannot be copied." << std::endl;
    abort();
  }

  TYPE &at(size_t idx, SharedMemoryAllocator &allocator) {
    if (idx >= vectorSize) {
      std::stringstream msg;
      msg << "Vector: beyond the range. " << idx << ":" << vectorSize;
      NGTThrowException(msg);
    }
    return *(begin(allocator) + idx);
  }

  void copy(TYPE &dst, const TYPE &src) {
    memcpy(reinterpret_cast<unsigned char *>(&dst), reinterpret_cast<const unsigned char *>(&src),
           elementSize);
  }

  iterator erase(iterator b, iterator e, SharedMemoryAllocator &allocator) {
    iterator ret;
    e = end(allocator) < e ? end(allocator) : e;
    for (iterator i = b; i < e; i++) {
      ret = erase(i, allocator);
    }
    return ret;
  }

  iterator erase(iterator i, SharedMemoryAllocator &allocator) {
    iterator back = i;
    vectorSize--;
    iterator e = end(allocator);
    for (; i < e; i++) {
      copy(*i, *(i + 1));
    }
    return back;
  }

  void pop_back() {
    if (vectorSize > 0) {
      vectorSize--;
    }
  }
  iterator insert(iterator &i, const TYPE &data, SharedMemoryAllocator &allocator) {
    if (size() == 0) {
      push_back(data, allocator);
      return end(allocator);
    }
    off_t oft = i - begin(allocator);
    extend(allocator);
    i          = begin(allocator) + oft;
    iterator b = begin(allocator);
    for (iterator ci = end(allocator); ci > i && ci != b; ci--) {
      copy(*ci, *(ci - 1));
    }
    copy(*i, data);
    vectorSize++;
    return i + 1;
  }

  void push_back(const TYPE &data, SharedMemoryAllocator &allocator) {
    extend(allocator);
    vectorSize++;
    copy((*this).at(vectorSize - 1, allocator), data);
  }

  void reserve(size_t s, SharedMemoryAllocator &allocator) {
    if (s <= allocatedSize) {
      return;
    } else {
      uint8_t *newptr = new (allocator) uint8_t[s * elementSize];
      uint8_t *dstptr = newptr;
      uint8_t *srcptr = static_cast<uint8_t *>(allocator.getAddr(vector));
      memcpy(dstptr, srcptr, vectorSize * elementSize);
      allocatedSize = s;
      if (vector != 0) {
        allocator.free(allocator.getAddr(vector));
      }
      vector = allocator.getOffset(newptr);
    }
  }

  void resize(size_t s, SharedMemoryAllocator &allocator, TYPE v = TYPE()) {
    if (s > allocatedSize) {
      size_t asize = allocatedSize == 0 ? 1 : allocatedSize;
      while (asize < s) {
        asize <<= 1;
      }
      reserve(asize, allocator);
      uint8_t *base   = allocator.getAddr(vector);
      uint8_t *dstptr = base + vectorSize;
      for (size_t i = vectorSize; i < s; i++) {
        copy(*(base + i * elementSize), v);
      }
    }
    vectorSize = s;
  }

  void serializeAsText(std::ostream &os, ObjectSpace *objectspace = 0) {
    unsigned int s = size();
    os << s << " ";
    for (unsigned int i = 0; i < s; i++) {
      Serializer::writeAsText(os, (*this)[i]);
      os << " ";
    }
  }

  void deserializeAsText(std::istream &is, ObjectSpace *objectspace = 0) {
    clear();
    size_t s;
    Serializer::readAsText(is, s);
    resize(s);
    for (unsigned int i = 0; i < s; i++) {
      Serializer::readAsText(is, (*this)[i]);
    }
  }

  size_t size() const { return vectorSize; }

 public:
  void extend(SharedMemoryAllocator &allocator) { extend(vectorSize, allocator); }

  void extend(size_t idx, SharedMemoryAllocator &allocator) {
    if (idx >= allocatedSize) {
      uint64_t size = allocatedSize == 0 ? 1 : allocatedSize;
      do {
        size <<= 1;
      } while (size <= idx);
      if (size > 0xffffffff) {
        std::cerr << "Vector is too big. " << size << std::endl;
        abort();
      }
      reserve(size, allocator);
    }
  }

  off_t vector;
  uint32_t vectorSize;
  uint32_t allocatedSize;
  uint32_t elementSize;
};

#else // NGT_SHARED_MEMORY_ALLOCATOR

template <class TYPE> class DynamicLengthVector {
 public:
  class iterator {
   public:
    iterator(void *ptr, uint32_t size) : i(static_cast<uint8_t *>(ptr)), elementSize(size) {}
    iterator operator+(size_t c) { return iterator(i + c * elementSize, elementSize); }
    iterator operator++(int c) {
      i += elementSize;
      return iterator(i, elementSize);
    }
    bool operator<(const iterator &it) const { return i < it.i; }
    TYPE &operator*() { return *reinterpret_cast<TYPE *>(i); }

    uint8_t *i;
    uint32_t elementSize;
  };

  DynamicLengthVector() : vector(0), vectorSize(0), allocatedSize(0), elementSize(0) {}
  ~DynamicLengthVector() { clear(); }

  void clear() {
    if (vector != 0) {
      delete[] vector;
    }
    vector        = 0;
    vectorSize    = 0;
    allocatedSize = 0;
  }

  TYPE &front() { return (*this).at(0); }
  TYPE &back() { return (*this).at(vectorSize - 1); }
  bool empty() { return vectorSize == 0; }
  iterator begin() { return iterator(vector, elementSize); }
  iterator end() { return begin() + vectorSize; }

  DynamicLengthVector &operator=(DynamicLengthVector<TYPE> &v) {
    std::cerr << "DynamicLengthVector cannot be copied." << std::endl;
    abort();
  }

  TYPE &at(size_t idx) {
    if (idx >= vectorSize) {
      std::stringstream msg;
      msg << "Vector: beyond the range. " << idx << ":" << vectorSize;
      NGTThrowException(msg);
    }
    return *(begin() + idx);
  }

  TYPE &operator[](size_t idx) { return *(begin() + idx); }

  void copy(TYPE &dst, const TYPE &src) {
    memcpy(reinterpret_cast<unsigned char *>(&dst), reinterpret_cast<const unsigned char *>(&src),
           elementSize);
  }

  iterator erase(iterator b, iterator e) {
    iterator ret;
    e = end() < e ? end() : e;
    for (iterator i = b; i < e; i++) {
      ret = erase(i);
    }
    return ret;
  }

  iterator erase(iterator i) {
    iterator back = i;
    vectorSize--;
    iterator e = end();
    for (; i < e; i++) {
      copy(*i, *(i + 1));
    }
    return back;
  }

  void pop_back() {
    if (vectorSize > 0) {
      vectorSize--;
    }
  }

  iterator insert(iterator &i, const TYPE &data) {
    if (size() == 0) {
      push_back(data);
      return end();
    }
    off_t oft = i - begin();
    extend();
    i          = begin() + oft;
    iterator b = begin();
    for (iterator ci = end(); ci > i && ci != b; ci--) {
      copy(*ci, *(ci - 1));
    }
    copy(*i, data);
    vectorSize++;
    return i + 1;
  }

  void push_back(const TYPE &data) {
    extend();
    vectorSize++;
    uint8_t buf[elementSize];
    memset(buf, 0, elementSize);
    memcpy(buf, &data, sizeof(data));
    copy((*this).at(vectorSize - 1), *reinterpret_cast<TYPE *>(buf));
  }

  void reserve(size_t s) {
    if (s <= allocatedSize) {
      return;
    } else {
      uint8_t *newptr = new uint8_t[s * elementSize];
      uint8_t *dstptr = newptr;
      uint8_t *srcptr = vector;
      memcpy(dstptr, srcptr, vectorSize * elementSize);
      allocatedSize = s;
      if (vector != 0) {
        delete[] vector;
      }
      vector = newptr;
    }
  }

  void resize(size_t s, TYPE v = TYPE()) {
    if (s > allocatedSize) {
      size_t asize = allocatedSize == 0 ? 1 : allocatedSize;
      while (asize < s) {
        asize <<= 1;
      }
      reserve(asize);
      uint8_t *base = vector;
      uint8_t buf[elementSize];
      memset(buf, 0, elementSize);
      memcpy(buf, &v, sizeof(v));
      for (size_t i = vectorSize; i < s; i++) {
        copy(*reinterpret_cast<TYPE *>(base + i * elementSize), *reinterpret_cast<TYPE *>(buf));
      }
    }
    vectorSize = s;
  }

  void serializeAsText(std::ostream &os, ObjectSpace *objectspace = 0) {
    unsigned int s = size();
    os << s << " ";
    for (unsigned int i = 0; i < s; i++) {
      Serializer::writeAsText(os, (*this)[i]);
      os << " ";
    }
  }

  void deserializeAsText(std::istream &is, ObjectSpace *objectspace = 0) {
    clear();
    size_t s;
    Serializer::readAsText(is, s);
    resize(s);
    for (unsigned int i = 0; i < s; i++) {
      Serializer::readAsText(is, (*this)[i]);
    }
  }

  void serialize(std::ofstream &os, NGT::ObjectSpace *objspace = 0) {
    uint32_t sz = size();
    NGT::Serializer::write(os, sz);
    os.write(reinterpret_cast<char *>(vector), size() * elementSize);
  }

  void deserialize(std::ifstream &is, NGT::ObjectSpace *objectspace = 0) {
    uint32_t sz;
    try {
      NGT::Serializer::read(is, sz);
    } catch (NGT::Exception &err) {
      std::stringstream msg;
      msg << "DynamicLengthVector::deserialize: It might be caused by inconsistency of the valuable type of "
             "the vector. "
          << err.what();
      NGTThrowException(msg);
    }
    resize(sz);
    is.read(reinterpret_cast<char *>(vector), sz * elementSize);
  }

  size_t size() { return vectorSize; }

 public:
  void extend() { extend(vectorSize); }

  void extend(size_t idx) {
    if (idx >= allocatedSize) {
      uint64_t size = allocatedSize == 0 ? 1 : allocatedSize;
      do {
        size <<= 1;
      } while (size <= idx);
      if (size > 0xffffffff) {
        std::cerr << "Vector is too big. " << size << std::endl;
        abort();
      }
      reserve(size);
    }
  }

  uint8_t *vector;
  uint32_t vectorSize;
  uint32_t allocatedSize;
  uint32_t elementSize;
};

#endif // NGT_SHARED_MEMORY_ALLOCATOR

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
class ObjectSpace;
template <class TYPE> class PersistentRepository {
 public:
  typedef Vector<off_t> ARRAY;
  typedef TYPE **iterator;

  PersistentRepository() : array(0) {}
  ~PersistentRepository() { close(); }

  void open(const std::string &mapfile, size_t sharedMemorySize) {
    assert(array == 0);
    SharedMemoryAllocator &allocator = getAllocator();
#ifdef ADVANCED_USE_REMOVED_LIST
    off_t *entryTable = (off_t *)allocator.construct(mapfile, sharedMemorySize);
    if (entryTable == 0) {
      entryTable = (off_t *)construct();
      allocator.setEntry(entryTable);
    }
    assert(entryTable != 0);
    initialize(entryTable);
#else
    void *entry = allocator.construct(mapfile, sharedMemorySize);
    if (entry == 0) {
      array = (ARRAY *)construct();
      allocator.setEntry(array);
    } else {
      array = (ARRAY *)entry;
    }
#endif
  }

  void close() { getAllocator().destruct(); }

#ifdef ADVANCED_USE_REMOVED_LIST
  void *construct() {
    SharedMemoryAllocator &allocator = getAllocator();
    off_t *entryTable                = new (allocator) off_t[2];
    entryTable[0]                    = allocator.getOffset(new (allocator) ARRAY);
    entryTable[1]                    = allocator.getOffset(new (allocator) Vector<size_t>);
    return entryTable;
  }
  void initialize(void *e) {
    SharedMemoryAllocator &allocator = getAllocator();
    off_t *entryTable                = (off_t *)e;
    array                            = (ARRAY *)allocator.getAddr(entryTable[0]);
    removedList                      = (Vector<size_t> *)allocator.getAddr(entryTable[1]);
  }
  size_t removedListPop() {
    assert(removedList->size() != 0);
    size_t idx = removedList->back(allocator);
    removedList->pop_back();
    return idx;
  }
  void removedListPush(size_t id) {
    if (removedList->size() == 0) {
      removedList->push_back(id, allocator);
      return;
    }
    Vector<size_t>::iterator rmi = std::lower_bound(removedList->begin(allocator),
                                                    removedList->end(allocator), id, std::greater<size_t>());
    if ((rmi != removedList->end(allocator)) && ((*rmi) == id)) {
      std::cerr << "removedListPush: already existed! continue... ID=" << id << std::endl;
      return;
    }
    removedList->insert(rmi, id, allocator);
  }
  void removedListRemove(size_t id) {
    if (removedList->size() == 0) {
      return;
    }
    Vector<size_t>::iterator rmi = std::lower_bound(removedList->begin(allocator),
                                                    removedList->end(allocator), id, std::greater<size_t>());
    if ((rmi != removedList->end(allocator)) && ((*rmi) == id)) {
      removedList->erase(rmi, allocator);
    }
  }
#else
  void *construct() {
    SharedMemoryAllocator &allocator = getAllocator();
    return new (allocator) ARRAY;
  }
  void initialize(void *e) {
    assert(array == 0);
    assert(e != 0);
    array = (ARRAY *)e;
  }
#endif
  TYPE *allocate() { return new (allocator) TYPE(allocator); }

  size_t push(TYPE *n) {
    if (size() == 0) {
      push_back(0);
    }
    push_back(n);
    return size() - 1;
  }

  size_t insert(TYPE *n) {
#ifdef ADVANCED_USE_REMOVED_LIST
    if (!removedList->empty()) {
      size_t idx = removedListPop();
      put(idx, n);
      return idx;
    }
#endif
    return push(n);
  }

  size_t insert(size_t idx, TYPE *n) {
#ifdef ADVANCED_USE_REMOVED_LIST
    removedListRemove(idx);
#endif
    put(idx, n);
    return idx;
  }

  bool isEmpty(size_t idx) {
    if (idx < size()) {
      return (*array).at(idx, allocator) == 0;
    } else {
      return true;
    }
  }

  void put(size_t idx, TYPE *n) {
    (*array).extend(idx, allocator);
    if (size() <= idx) {
      resize(idx + 1);
    }
    if ((*this)[idx] != 0) {
      NGTThrowException("put: Not empty");
    }
    set(idx, n);
  }

  void erase(size_t idx) {
    if (isEmpty(idx)) {
      NGTThrowException("erase: Not in-memory or invalid id");
    }
    (*this)[idx]->~TYPE();
    allocator.free((*this)[idx]);
    set(idx, (TYPE *)0);
  }

  void remove(size_t idx) {
    erase(idx);
#ifdef ADVANCED_USE_REMOVED_LIST
    removedListPush(idx);
#endif
  }

  inline TYPE *get(size_t idx) {
    if (isEmpty(idx)) {
      std::stringstream msg;
      msg << "get: Not in-memory or invalid offset of node. " << idx << ":" << array->size();
      NGTThrowException(msg.str());
    }
    return (*this)[idx];
  }

  void serialize(std::ofstream &os, ObjectSpace *objectspace = 0) {
    NGT::Serializer::write(os, array->size());
    for (size_t idx = 0; idx < array->size(); idx++) {
      if ((*this)[idx] == 0) {
        NGT::Serializer::write(os, '-');
      } else {
        NGT::Serializer::write(os, '+');
        if (objectspace == 0) {
          assert(0);
        } else {
          assert(0);
        }
      }
    }
  }

  void deserialize(std::ifstream &is, ObjectSpace *objectspace = 0) {
    if (!is.is_open()) {
      NGTThrowException("NGT::Common: Not open the specified stream yet.");
    }
    deleteAll();
    (*this).push_back((TYPE *)0);
    size_t s;
    NGT::Serializer::read(is, s);
    for (size_t i = 0; i < s; i++) {
      char type;
      NGT::Serializer::read(is, type);
      switch (type) {
      case '-': {
        (*this).push_back((TYPE *)0);
#ifdef ADVANCED_USE_REMOVED_LIST
        if (i != 0) {
          removedListPush(i);
        }
#endif
      } break;
      case '+': {
        if (objectspace == 0) {
          TYPE *v = new (allocator) TYPE(allocator);
          assert(0);
          (*this).push_back(v);
        } else {
          TYPE *v = new (allocator) TYPE(allocator, objectspace);
          assert(0);
          (*this).push_back(v);
        }
      } break;
      default: {
        assert(type == '-' || type == '+');
        break;
      }
      }
    }
  }

  void serializeAsText(std::ofstream &os, ObjectSpace *objectspace = 0) {
    os.setf(std::ios_base::fmtflags(0), std::ios_base::floatfield);
    os << std::setprecision(8);

    os << array->size() << std::endl;
    for (size_t idx = 0; idx < array->size(); idx++) {
      if ((*this)[idx] == 0) {
        os << idx << " - " << std::endl;
      } else {
        os << idx << " + ";
        if (objectspace == 0) {
          (*this)[idx]->serializeAsText(os, allocator);
        } else {
          (*this)[idx]->serializeAsText(os, allocator, objectspace);
        }
        os << std::endl;
      }
    }
    os << std::fixed;
  }

  void deserializeAsText(std::ifstream &is, ObjectSpace *objectspace = 0) {
    if (!is.is_open()) {
      NGTThrowException("NGT::Common: Not open the specified stream yet.");
    }
    deleteAll();
    size_t s;
    NGT::Serializer::readAsText(is, s);
    (*this).reserve(s);
    for (size_t i = 0; i < s; i++) {
      size_t idx;
      NGT::Serializer::readAsText(is, idx);
      if (i != idx) {
        std::cerr << "PersistentRepository: Error. index of a specified import file is invalid. " << idx
                  << ":" << i << std::endl;
      }
      char type;
      NGT::Serializer::readAsText(is, type);
      switch (type) {
      case '-': {
        (*this).push_back((TYPE *)0);
#ifdef ADVANCED_USE_REMOVED_LIST
        if (i != 0) {
          removedListPush(i);
        }
#endif
      } break;
      case '+': {
        if (objectspace == 0) {
          TYPE *v = new (allocator) TYPE(allocator);
          v->deserializeAsText(is, allocator);
          (*this).push_back(v);
        } else {
          TYPE *v = new (allocator) TYPE(allocator, objectspace);
          v->deserializeAsText(is, allocator, objectspace);
          (*this).push_back(v);
        }
      } break;
      default: {
        assert(type == '-' || type == '+');
        break;
      }
      }
    }
  }
  void deleteAll() {
    for (size_t i = 0; i < array->size(); i++) {
      if ((*array).at(i, allocator) != 0) {
        allocator.free((*this)[i]);
      }
    }
    array->clear(allocator);
#ifdef ADVANCED_USE_REMOVED_LIST
    while (!removedList->empty()) {
      removedListPop();
    }
#endif
  }

  void set(size_t idx, TYPE *n) { array->at(idx, allocator) = allocator.getOffset(n); }
  SharedMemoryAllocator &getAllocator() { return allocator; }
  void clear() { array->clear(allocator); }
  iterator begin() { return (iterator)array->begin(allocator); }
  iterator end() { return (iterator)array->end(allocator); }
  bool empty() { return array->empty(); }
  TYPE *operator[](size_t idx) { return (TYPE *)allocator.getAddr((*array).at(idx, allocator)); }
  TYPE *at(size_t idx) { return (TYPE *)allocator.getAddr(array->at(idx, allocator)); }
  void push_back(TYPE *data) { array->push_back(allocator.getOffset(data), allocator); }
  void reserve(size_t s) { array->reserve(s, allocator); }
  void resize(size_t s) { array->resize(s, allocator, (off_t)0); }
  size_t size() { return array->size(); }
  size_t getAllocatedSize() { return array->allocatedSize; }

  ARRAY *array;

  SharedMemoryAllocator allocator;

#ifdef ADVANCED_USE_REMOVED_LIST
  size_t count() { return size() == 0 ? 0 : size() - removedList->size() - 1; }

 protected:
  Vector<size_t> *removedList;
#endif
};

#endif // NGT_SHARED_MEMORY_ALLOCATOR

class ObjectSpace;

template <class TYPE> class Repository : public std::vector<TYPE *> {
 public:
  static TYPE *allocate() { return new TYPE; }

  size_t push(TYPE *n) {
    if (std::vector<TYPE *>::size() == 0) {
      std::vector<TYPE *>::push_back(0);
    }
    std::vector<TYPE *>::push_back(n);
    return std::vector<TYPE *>::size() - 1;
  }

#ifdef ADVANCED_USE_REMOVED_LIST
  void removedListRemove(size_t id) {
    if (!removedList.empty()) {
      std::priority_queue<size_t, std::vector<size_t>, std::greater<size_t>> rl = removedList;
      std::priority_queue<size_t, std::vector<size_t>, std::greater<size_t>> newrl;
      while (rl.size() != 0) {
        if (rl.top() == id) {
          rl.pop();
          while (rl.size() != 0) {
            newrl.push(rl.top());
            rl.pop();
          }
          removedList = newrl;
          break;
        }
        if (rl.top() > id) break;
        newrl.push(rl.top());
        rl.pop();
      }
    }
  }
#endif

  size_t insert(TYPE *n) {
#ifdef ADVANCED_USE_REMOVED_LIST
    if (!removedList.empty()) {
      size_t idx = removedList.top();
      removedList.pop();
      put(idx, n);
      return idx;
    }
#endif
    return push(n);
  }

  size_t insert(size_t idx, TYPE *n) {
#ifdef ADVANCED_USE_REMOVED_LIST
    removedListRemove(idx);
#endif
    put(idx, n);
    return idx;
  }

  bool isEmpty(size_t idx) {
    if (idx < std::vector<TYPE *>::size()) {
      return (*this)[idx] == 0;
    } else {
      return true;
    }
  }

  void put(size_t idx, TYPE *n) {
    if (std::vector<TYPE *>::size() <= idx) {
      std::vector<TYPE *>::resize(idx + 1, 0);
    }
    if ((*this)[idx] != 0) {
      NGTThrowException("put: Not empty");
    }
    (*this)[idx] = n;
  }

  void erase(size_t idx) {
    if (isEmpty(idx)) {
      NGTThrowException("erase: Not in-memory or invalid id");
    }
    delete (*this)[idx];
    (*this)[idx] = 0;
  }

  void remove(size_t idx) {
    erase(idx);
#ifdef ADVANCED_USE_REMOVED_LIST
    removedList.push(idx);
#endif
  }

  TYPE **getPtr() { return &(*this)[0]; }

  inline TYPE *get(size_t idx) {
    if (isEmpty(idx)) {
      std::stringstream msg;
      msg << "get: Not in-memory or invalid offset of node. idx=" << idx << " size=" << this->size();
      NGTThrowException(msg.str());
    }
    return (*this)[idx];
  }

  inline TYPE *getWithoutCheck(size_t idx) { return (*this)[idx]; }

  void serialize(std::ofstream &os, ObjectSpace *objectspace = 0) {
    if (!os.is_open()) {
      NGTThrowException("NGT::Common: Not open the specified stream yet.");
    }
    NGT::Serializer::write(os, std::vector<TYPE *>::size());
    for (size_t idx = 0; idx < std::vector<TYPE *>::size(); idx++) {
      if ((*this)[idx] == 0) {
        NGT::Serializer::write(os, '-');
      } else {
        NGT::Serializer::write(os, '+');
        if (objectspace == 0) {
          (*this)[idx]->serialize(os);
        } else {
          (*this)[idx]->serialize(os, objectspace);
        }
      }
    }
  }

  void deserialize(std::ifstream &is, ObjectSpace *objectspace = 0) {
    if (!is.is_open()) {
      NGTThrowException("NGT::Common: Not open the specified stream yet.");
    }
    deleteAll();
    size_t s;
    NGT::Serializer::read(is, s);
    std::vector<TYPE *>::reserve(s);
    for (size_t i = 0; i < s; i++) {
      char type;
      NGT::Serializer::read(is, type);
      switch (type) {
      case '-': {
        std::vector<TYPE *>::push_back(0);
#ifdef ADVANCED_USE_REMOVED_LIST
        if (i != 0) {
          removedList.push(i);
        }
#endif
      } break;
      case '+': {
        if (objectspace == 0) {
          TYPE *v = new TYPE;
          v->deserialize(is);
          std::vector<TYPE *>::push_back(v);
        } else {
          TYPE *v = new TYPE(objectspace);
          v->deserialize(is, objectspace);
          std::vector<TYPE *>::push_back(v);
        }
      } break;
      default: {
        assert(type == '-' || type == '+');
        break;
      }
      }
    }
  }

  void serializeAsText(std::ofstream &os, ObjectSpace *objectspace = 0) {
    if (!os.is_open()) {
      NGTThrowException("NGT::Common: Not open the specified stream yet.");
    }
    // The format is almost the same as the default and the best in terms of the string length.
    os.setf(std::ios_base::fmtflags(0), std::ios_base::floatfield);
    os << std::setprecision(8);

    os << std::vector<TYPE *>::size() << std::endl;
    for (size_t idx = 0; idx < std::vector<TYPE *>::size(); idx++) {
      if ((*this)[idx] == 0) {
        os << idx << " - " << std::endl;
      } else {
        os << idx << " + ";
        if (objectspace == 0) {
          (*this)[idx]->serializeAsText(os);
        } else {
          (*this)[idx]->serializeAsText(os, objectspace);
        }
        os << std::endl;
      }
    }
    os << std::fixed;
  }

  void deserializeAsText(std::ifstream &is, ObjectSpace *objectspace = 0) {
    if (!is.is_open()) {
      NGTThrowException("NGT::Common: Not open the specified stream yet.");
    }
    deleteAll();
    size_t s;
    NGT::Serializer::readAsText(is, s);
    std::vector<TYPE *>::reserve(s);
    for (size_t i = 0; i < s; i++) {
      size_t idx;
      NGT::Serializer::readAsText(is, idx);
      if (i != idx) {
        std::cerr << "Repository: Error. index of a specified import file is invalid. " << idx << ":" << i
                  << std::endl;
      }
      char type;
      NGT::Serializer::readAsText(is, type);
      switch (type) {
      case '-': {
        std::vector<TYPE *>::push_back(0);
#ifdef ADVANCED_USE_REMOVED_LIST
        if (i != 0) {
          removedList.push(i);
        }
#endif
      } break;
      case '+': {
        if (objectspace == 0) {
          TYPE *v = new TYPE;
          v->deserializeAsText(is);
          std::vector<TYPE *>::push_back(v);
        } else {
          TYPE *v = new TYPE(objectspace);
          v->deserializeAsText(is, objectspace);
          std::vector<TYPE *>::push_back(v);
        }
      } break;
      default: {
        assert(type == '-' || type == '+');
        break;
      }
      }
    }
  }

  void deleteAll() {
    for (size_t i = 0; i < this->size(); i++) {
      if ((*this)[i] != 0) {
        delete (*this)[i];
        (*this)[i] = 0;
      }
    }
    this->clear();
    this->shrink_to_fit();
#ifdef ADVANCED_USE_REMOVED_LIST
    while (!removedList.empty()) {
      removedList.pop();
    }
#endif
  }

  void set(size_t idx, TYPE *n) { (*this)[idx] = n; }

#ifdef ADVANCED_USE_REMOVED_LIST
  size_t count() {
    return std::vector<TYPE *>::size() == 0 ? 0 : std::vector<TYPE *>::size() - removedList.size() - 1;
  }

 protected:
  std::priority_queue<size_t, std::vector<size_t>, std::greater<size_t>> removedList;
#endif
};

#pragma pack(2)
class ObjectDistance {
 public:
  ObjectDistance() : id(0), distance(0.0) {}
  ObjectDistance(unsigned int i, float d) : id(i), distance(d) {}
  inline bool operator==(const ObjectDistance &o) const { return (distance == o.distance) && (id == o.id); }
  inline void set(unsigned int i, float d) {
    id       = i;
    distance = d;
  }
  inline bool operator<(const ObjectDistance &o) const {
    if (distance == o.distance) {
      return id < o.id;
    } else {
      return distance < o.distance;
    }
  }
  inline bool operator>(const ObjectDistance &o) const {
    if (distance == o.distance) {
      return id > o.id;
    } else {
      return distance > o.distance;
    }
  }
  void serialize(std::ofstream &os) {
    NGT::Serializer::write(os, id);
    NGT::Serializer::write(os, distance);
  }
  void deserialize(std::ifstream &is) {
    NGT::Serializer::read(is, id);
    NGT::Serializer::read(is, distance);
  }

  void serializeAsText(std::ofstream &os) {
    os.unsetf(std::ios_base::floatfield);
    os << std::setprecision(8) << id << " " << distance;
  }

  void deserializeAsText(std::ifstream &is) {
    is >> id;
    is >> distance;
  }

  friend std::ostream &operator<<(std::ostream &os, const ObjectDistance &o) {
    os << o.id << " " << o.distance;
    return os;
  }
  friend std::istream &operator>>(std::istream &is, ObjectDistance &o) {
    is >> o.id;
    is >> o.distance;
    return is;
  }
  uint32_t id;
  float distance;
};

#pragma pack()

class Object;
class ObjectDistances;

class Container {
 public:
  Container(Object &o, ObjectID i) : object(o), id(i) {}
  Container(ObjectID i) : object(emptyObject()), id(i) {}
  Container(Container &c) : object(c.object), id(c.id) {}
  bool isEmptyObject() const { return &object == &emptyObject(); }
  static Object &emptyObject() {
    auto *objptr = reinterpret_cast<Object *>(0);
    return *objptr;
  }
  Object &object;
  ObjectID id;
};

class ResultSet : public std::vector<ObjectDistance> {
 public:
  ObjectDistance &top() { return front(); }
  void emplace(const ObjectDistance &o) { push(o); }
  void push(const ObjectDistance &o) {
    emplace_back(o);
    push_heap(begin(), end());
  }
  void pop() {
    pop_heap(begin(), end());
    pop_back();
  }
  void push_pop(const ObjectDistance &o) {
    emplace_back(o);
#if !defined(__linux__)
    push_heap(begin(), end());
#endif
    pop_heap(begin(), end());
    pop_back();
  }
};

class ResultSetWithCheck : public ResultSet {
 public:
  void emplace(const ObjectDistance &o) { ResultSetWithCheck::push(o); }
  void push(const ObjectDistance &o) {
    if (ids.count(o.id)) return;
    ids.emplace(o.id);
    ResultSet::push(o);
  }
  void pop() {
    ids.erase(top().id);
    ResultSet::pop();
  }
  void push_pop(const ObjectDistance &o) {
    if (ids.count(o.id)) return;
    ids.erase(top().id);
    ResultSet::push_pop(o);
  }
  std::unordered_set<ObjectID> ids;
};

typedef ResultSet ResultPriorityQueue;

class SearchContainer : public NGT::Container {
 public:
  SearchContainer(Object &f, ObjectID i) : Container(f, i) { initialize(); }
  SearchContainer(Object &f) : Container(f, 0) { initialize(); }
  SearchContainer(SearchContainer &sc) : Container(sc) { *this = sc; }
  SearchContainer(SearchContainer &sc, Object &f) : Container(f, sc.id) { *this = sc; }
  SearchContainer() : Container(0) { initialize(); }

  SearchContainer &operator=(SearchContainer &sc) {
    size                     = sc.size;
    radius                   = sc.radius;
    explorationCoefficient   = sc.explorationCoefficient;
    result                   = sc.result;
    distanceComputationCount = sc.distanceComputationCount;
    edgeSize                 = sc.edgeSize;
    workingResult            = sc.workingResult;
    useAllNodesInLeaf        = sc.useAllNodesInLeaf;
    expectedAccuracy         = sc.expectedAccuracy;
    visitCount               = sc.visitCount;
    insertion                = sc.insertion;
#ifdef RESULT_DEFINED_RANGE
    expandedSizeByEpsilon = sc.expandedSizeByEpsilon;
#endif
    return *this;
  }
  virtual ~SearchContainer() {}
  virtual void initialize() {
    size                   = 10;
    radius                 = FLT_MAX;
    explorationCoefficient = 1.1;
    result                 = 0;
    edgeSize =
        -1; // dynamically prune the edges during search. -1 means following the index property. 0 means using all edges.
    useAllNodesInLeaf = false;
    expectedAccuracy  = -1.0;
    insertion         = false;
#ifdef RESULT_DEFINED_RANGE
    expandedSizeByEpsilon = false;
#endif
  }
  void setSize(size_t s) { size = s; }
  void setResults(ObjectDistances *r) { result = r; }
  void setRadius(Distance r) { radius = r; }
  void setEpsilon(float e) { explorationCoefficient = e + 1.0; }
  void setEdgeSize(int e) { edgeSize = e; }
  void setExpectedAccuracy(float a) { expectedAccuracy = a; }
#ifdef RESULT_DEFINED_RANGE
  void setExpandedSizeByEpsilon(bool v) { expandedSizeByEpsilon = v; }
#endif

  inline bool resultIsAvailable() { return result != 0; }
  float getEpsilon() { return explorationCoefficient - 1.0; }
  ObjectDistances &getResult() {
    if (result == 0) {
      NGTThrowException("Inner error: results is not set");
    }
    return *result;
  }

  ResultPriorityQueue &getWorkingResult() { return workingResult; }

  size_t size;
  Distance radius;
  float explorationCoefficient;
  int edgeSize;
  size_t distanceComputationCount;
  ResultPriorityQueue workingResult;
  bool useAllNodesInLeaf;
  size_t visitCount;
  float expectedAccuracy;
#ifdef RESULT_DEFINED_RANGE
  bool expandedSizeByEpsilon;
#endif
 private:
  ObjectDistances *result;

 public:
  bool insertion;
};

class QueryContainer {
 public:
  template <typename QTYPE> QueryContainer(const std::vector<QTYPE> &q) : query(0) { setQuery(q); }
  QueryContainer(const QueryContainer &qc) {
    dimension   = qc.dimension;
    queryType   = qc.queryType;
    floatVector = 0;
    if (*queryType == typeid(float)) {
      floatVector = new std::vector<float>(*(static_cast<std::vector<float> *>(qc.query)));
      query       = floatVector;
    } else if (*queryType == typeid(double)) {
      query = new std::vector<double>(*(static_cast<std::vector<double> *>(qc.query)));
    } else if (*queryType == typeid(uint8_t)) {
      query = new std::vector<uint8_t>(*(static_cast<std::vector<uint8_t> *>(qc.query)));
#ifdef NGT_HALF_FLOAT
    } else if (*queryType == typeid(float16)) {
      query = new std::vector<float16>(*(static_cast<std::vector<float16> *>(qc.query)));
#endif
    }
#ifdef NGT_REFINEMENT
    refinementExpansion = qc.refinementExpansion;
#endif
  }

  ~QueryContainer() { deleteQuery(); }

  template <typename QTYPE> void setQuery(const std::vector<QTYPE> &q) {
    if (query != 0) {
      deleteQuery();
    }
    queryType = &typeid(QTYPE);
    if (*queryType != typeid(float) &&
#ifdef NGT_HALF_FLOAT
        *queryType != typeid(float16) &&
#endif
        *queryType != typeid(double) && *queryType != typeid(uint8_t)) {
      query     = 0;
      queryType = 0;
      dimension = 0;
      std::stringstream msg;
      msg << "NGT::SearchQuery: Invalid query type!";
      NGTThrowException(msg);
    }
    query     = new std::vector<QTYPE>(q);
    dimension = q.size();
    if (typeid(QTYPE) == typeid(float)) {
      floatVector = static_cast<std::vector<float> *>(query);
    } else {
      floatVector = 0;
    }
#ifdef NGT_REFINEMENT
    refinementExpansion = 0.0;
#endif
  }
  void *getQuery() { return query; }
  std::vector<float> &getQueryVector() {
    if (floatVector == 0) {
      NGTThrowException("not set because of not float");
    }
    return *floatVector;
  }
  size_t getDimension() { return dimension; }
  const std::type_info &getQueryType() { return *queryType; }
  void pushBack(float v) {
    if (*queryType == typeid(float)) {
      static_cast<std::vector<float> *>(query)->push_back(v);
    } else if (*queryType == typeid(double)) {
      static_cast<std::vector<double> *>(query)->push_back(v);
    } else if (*queryType == typeid(uint8_t)) {
      static_cast<std::vector<uint8_t> *>(query)->push_back(v);
#ifdef NGT_HALF_FLOAT
    } else if (*queryType == typeid(float16)) {
      static_cast<std::vector<float16> *>(query)->push_back(static_cast<float16>(v));
#endif
    }
  }
#ifdef NGT_REFINEMENT
  void setRefinementExpansion(float re) { refinementExpansion = re; }
  float getRefinementExpansion() { return refinementExpansion; }
#endif
 private:
  void deleteQuery() {
    if (query == 0) {
      return;
    }
    if (*queryType == typeid(float)) {
      delete static_cast<std::vector<float> *>(query);
    } else if (*queryType == typeid(double)) {
      delete static_cast<std::vector<double> *>(query);
    } else if (*queryType == typeid(uint8_t)) {
      delete static_cast<std::vector<uint8_t> *>(query);
#ifdef NGT_HALF_FLOAT
    } else if (*queryType == typeid(float16)) {
      delete static_cast<std::vector<float16> *>(query);
#endif
    }
    query     = 0;
    queryType = 0;
  }
  void *query;
  std::vector<float> *floatVector;
  const std::type_info *queryType;
  size_t dimension;
#ifdef NGT_REFINEMENT
  float refinementExpansion;
#endif
};

class SearchQuery : public NGT::SearchContainer, public NGT::QueryContainer {
 public:
  template <typename QTYPE> SearchQuery(const std::vector<QTYPE> &q) : NGT::QueryContainer(q) {}
  SearchQuery(SearchQuery &sq, Object &o)
      : SearchContainer(static_cast<SearchContainer &>(sq), o), QueryContainer(sq) {}
};

class InsertContainer : public Container {
 public:
  InsertContainer(Object &f, ObjectID i) : Container(f, i) {}
};

template <class T, class Container = std::vector<T>, class Compare = std::greater<T>>
struct PriorityQueue : std::priority_queue<T, Container, Compare> {
  using std::priority_queue<T, Container, Compare>::c;
  void pop_back() { c.pop_back(); }
};

namespace detail {
struct CandidateObject {
  uint32_t id;
  uint16_t distance;
  bool visited;
};
} // namespace detail

template <typename T = detail::CandidateObject> class HeapCandidateObjects {
 public:
  struct CompareByDistance {
    bool operator()(const T &a, const T &b) const { return a.distance > b.distance; }
  };

  struct CompareByDistanceMax {
    bool operator()(const T &a, const T &b) const { return a.distance < b.distance; }
  };

 private:
  std::vector<T> topKResults;

  std::vector<T> expandedResults;

  std::vector<T> candidateNode;

  uint16_t k;
  uint16_t capacity;
  size_t expandedResultsCapacity;
  float resultExpansion;
  uint16_t lastPoppedDistance;
  uint16_t cachedMaxDistance;
  size_t maxExpandedIndex;

  __attribute__((always_inline)) inline void siftDownMaxHeap(std::vector<T> &heap) {
    const size_t n = heap.size();
    if (n <= 1) return;

    size_t i = 0;
    while (true) {
      const size_t left  = 2 * i + 1;
      const size_t right = 2 * i + 2;
      size_t largest     = i;

      if (left < n && heap[left].distance > heap[largest].distance) {
        largest = left;
      }
      if (right < n && heap[right].distance > heap[largest].distance) {
        largest = right;
      }

      if (largest == i) break;

      std::swap(heap[i], heap[largest]);
      i = largest;
    }
  }

 public:
  using Object = T;

  HeapCandidateObjects(uint16_t searchSize = 10, float resultExpansionValue = 2.0) {
    k                       = searchSize;
    resultExpansion         = resultExpansionValue;
    capacity                = (uint16_t)(k * resultExpansion);
    expandedResultsCapacity = (capacity > k) ? (capacity - k) : 0;
    lastPoppedDistance      = 0;
    cachedMaxDistance       = 0;
    maxExpandedIndex        = 0;
  }

  void reset(uint16_t searchSize) {
    topKResults.clear();
    candidateNode.clear();
    expandedResults.clear();

    k                       = searchSize;
    capacity                = (uint16_t)(k * resultExpansion);
    expandedResultsCapacity = (capacity > k) ? (capacity - k) : 0;
    lastPoppedDistance      = 0;
    cachedMaxDistance       = 0;
    maxExpandedIndex        = 0;

    if (topKResults.capacity() < k) {
      topKResults.reserve(k);
    }
    if (expandedResults.capacity() < expandedResultsCapacity) {
      expandedResults.reserve(expandedResultsCapacity);
    }
  }

  void reset(uint16_t searchSize, float resultExpansionValue) {
    resultExpansion = resultExpansionValue;
    reset(searchSize);
  }

  void setResultExpansion(float value) { resultExpansion = value; }

  __attribute__((always_inline)) inline bool push(T &element) {

    candidateNode.push_back(element);
    std::push_heap(candidateNode.begin(), candidateNode.end(),
                   [](const T &a, const T &b) { return a.distance > b.distance; });

    const size_t currentSize = topKResults.size();

    if (__builtin_expect(currentSize >= k, 1)) {
      if (element.distance >= cachedMaxDistance) {
        const size_t expandedSize = expandedResults.size();
        if (expandedSize < expandedResultsCapacity) {
          if (expandedSize == 0 || element.distance > expandedResults[maxExpandedIndex].distance) {
            maxExpandedIndex = expandedSize;
          }
          expandedResults.push_back(element);
        } else if (expandedSize > 0 && element.distance < expandedResults[maxExpandedIndex].distance) {
          expandedResults[maxExpandedIndex] = element;
          maxExpandedIndex                  = 0;
          for (size_t i = 1; i < expandedSize; ++i) {
            if (expandedResults[i].distance > expandedResults[maxExpandedIndex].distance) {
              maxExpandedIndex = i;
            }
          }
        }
        return false;
      }
    }

    if (currentSize < k) {
      topKResults.push_back(element);
      std::push_heap(topKResults.begin(), topKResults.end(),
                     [](const T &a, const T &b) { return a.distance < b.distance; });
      cachedMaxDistance = topKResults.front().distance;

      if (currentSize + 1 == k) {
        return true;
      }
      return false;
    } else {
      const size_t expandedSize = expandedResults.size();
      if (expandedSize < expandedResultsCapacity) {
        if (expandedSize == 0 || topKResults.front().distance > expandedResults[maxExpandedIndex].distance) {
          maxExpandedIndex = expandedSize;
        }
        expandedResults.push_back(topKResults.front());
      } else if (expandedSize > 0 &&
                 topKResults.front().distance < expandedResults[maxExpandedIndex].distance) {
        expandedResults[maxExpandedIndex] = topKResults.front();
        maxExpandedIndex                  = 0;
        for (size_t i = 1; i < expandedSize; ++i) {
          if (expandedResults[i].distance > expandedResults[maxExpandedIndex].distance) {
            maxExpandedIndex = i;
          }
        }
      }

      topKResults.front() = element;
      siftDownMaxHeap(topKResults);
      cachedMaxDistance = topKResults.front().distance;
      return true;
    }
  }
  __attribute__((always_inline)) inline bool pop(T &result) {
    if (__builtin_expect(!candidateNode.empty(), 1)) {
      result = candidateNode.front();
      std::pop_heap(candidateNode.begin(), candidateNode.end(),
                    [](const T &a, const T &b) { return a.distance > b.distance; });
      candidateNode.pop_back();
      lastPoppedDistance = result.distance;
      return true;
    }
    return false;
  }

  int getObjects(T *output, int maxCount = 0) {
    const size_t totalSize         = topKResults.size() + expandedResults.size();
    const size_t effectiveMaxCount = (maxCount == 0) ? totalSize : static_cast<size_t>(maxCount);

    const size_t topKCount = std::min(topKResults.size(), effectiveMaxCount);
    std::memcpy(output, topKResults.data(), topKCount * sizeof(T));

    const size_t remaining     = effectiveMaxCount - topKCount;
    const size_t expandedCount = std::min(expandedResults.size(), remaining);
    std::memcpy(output + topKCount, expandedResults.data(), expandedCount * sizeof(T));

    return static_cast<int>(topKCount + expandedCount);
  }

  void setK(uint16_t cap) {
    k                       = cap;
    capacity                = (uint16_t)(k * resultExpansion);
    expandedResultsCapacity = (capacity > k) ? (capacity - k) : 0;
  }

  __attribute__((always_inline)) inline uint16_t getMaxDistance() const { return cachedMaxDistance; }

  uint16_t getCurrentDistance() const { return lastPoppedDistance; }
};

} // namespace NGT

namespace std {
template <> class numeric_limits<NGT::qsint8> {
 public:
  static NGT::qsint8 max() { return NGT::qsint8(127); }
  static NGT::qsint8 min() { return NGT::qsint8(-128); }
  static bool is_specialized() { return true; }
};
} // namespace std

namespace std {
template <> class numeric_limits<NGT::quint8> {
 public:
  static NGT::quint8 max() { return NGT::quint8(255); }
  static NGT::quint8 min() { return NGT::quint8(0); }
  static bool is_specialized() { return true; }
};
} // namespace std

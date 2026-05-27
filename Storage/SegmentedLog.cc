/* Copyright (c) 2012-2014 Stanford University
 * Copyright (c) 2015 Diego Ongaro
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#define _BSD_SOURCE
#include <endian.h>

#include <algorithm>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "build/Protocol/Raft.pb.h"
#include "Core/Checksum.h"
#include "Core/Debug.h"
#include "Core/ProtoBuf.h"
#include "Core/StringUtil.h"
#include "Core/ThreadId.h"
#include "Core/Time.h"
#include "Core/Util.h"
#include "Storage/FilesystemUtil.h"
#include "Storage/SegmentedLog.h"
#include "Server/Globals.h"

namespace LogCabin {
namespace Storage {

namespace FS = FilesystemUtil;
using Core::StringUtil::format;

namespace {

/**
 * Format string for open segment filenames.
 * First param: incrementing counter.
 */
#define OPEN_SEGMENT_FORMAT "open-%lu"

/**
 * Format string for closed segment filenames.
 * First param: start index, inclusive.
 * Second param: end index, inclusive.
 */
#define CLOSED_SEGMENT_FORMAT "%020lu-%020lu"

/**
 * Return true if all the bytes in range [start, start + length) are zero.
 */
bool
isAllZeros(const void* _start, size_t length)
{
    const uint8_t* start = static_cast<const uint8_t*>(_start);
    for (size_t offset = 0; offset < length; ++offset) {
        if (start[offset] != 0)
            return false;
    }
    return true;
}

} // anonymous namespace


////////// SegmentedLog::PreparedSegments //////////


SegmentedLog::PreparedSegments::PreparedSegments(uint64_t queueSize)
    : quietForUnitTests(false)
    , mutex()
    , consumed()
    , produced()
    , exiting(false)
    , demanded(queueSize)
    , filenameCounter(0)
    , openSegments()
{
}

SegmentedLog::PreparedSegments::~PreparedSegments()
{
}

void
SegmentedLog::PreparedSegments::exit()
{
    std::lock_guard<Core::Mutex> lockGuard(mutex);
    exiting = true;
    consumed.notify_all();
    produced.notify_all();
}

void
SegmentedLog::PreparedSegments::foundFile(uint64_t fileId)
{
    std::lock_guard<Core::Mutex> lockGuard(mutex);
    if (filenameCounter < fileId)
        filenameCounter = fileId;
}

std::deque<SegmentedLog::PreparedSegments::OpenSegment>
SegmentedLog::PreparedSegments::releaseAll()
{
    std::lock_guard<Core::Mutex> lockGuard(mutex);
    std::deque<OpenSegment> ret;
    std::swap(openSegments, ret);
    return ret;
}

void
SegmentedLog::PreparedSegments::submitOpenSegment(OpenSegment segment)
{
    std::lock_guard<Core::Mutex> lockGuard(mutex);
    openSegments.push_back(std::move(segment));
    produced.notify_one();
}

uint64_t
SegmentedLog::PreparedSegments::waitForDemand()
{
    std::unique_lock<Core::Mutex> lockGuard(mutex);
    while (!exiting) {
        if (demanded > 0) {
            --demanded;
            ++filenameCounter;
            return filenameCounter;
        }
        consumed.wait(lockGuard);
    }
    throw Core::Util::ThreadInterruptedException();
}

SegmentedLog::PreparedSegments::OpenSegment
SegmentedLog::PreparedSegments::waitForOpenSegment()
{
    std::unique_lock<Core::Mutex> lockGuard(mutex);
    uint64_t numWaits = 0;
    while (true) {
        if (exiting) {
            NOTICE("Exiting");
            throw Core::Util::ThreadInterruptedException();
        }
        if (!openSegments.empty()) {
            break;
        }
        if (numWaits == 0 && !quietForUnitTests) {
            WARNING("Prepared segment not ready, having to wait on it. "
                    "This is perfectly safe but bad for performance. "
                    "Consider increasing storageOpenSegments in the config.");
        }
        ++numWaits;
        produced.wait(lockGuard);
    }
    if (numWaits > 0 && !quietForUnitTests) {
        WARNING("Done waiting: prepared segment now ready");
    }
    OpenSegment r = std::move(openSegments.front());
    openSegments.pop_front();
    consumed.notify_one();
    ++demanded;
    return r;
}


////////// SegmentedLog::Sync //////////


SegmentedLog::Sync::Sync(uint64_t lastIndex,
                         std::chrono::nanoseconds diskWriteDurationThreshold)
    : Log::Sync(lastIndex)
    , diskWriteDurationThreshold(diskWriteDurationThreshold)
    , ops()
    , waitStart(TimePoint::max())
    , waitEnd(TimePoint::max())
{
}

SegmentedLog::Sync::~Sync()
{
}

void
SegmentedLog::Sync::optimize()
{
    if (ops.size() < 3)
        return;
    auto prev = ops.begin();
    auto it = prev + 1;
    auto next = it + 1;
    while (next != ops.end()) {
        if (prev->opCode == Op::FDATASYNC &&
            it->opCode == Op::WRITE &&
            next->opCode == Op::FDATASYNC &&
            prev->fd == it->fd &&
            it->fd == next->fd) {
            prev->opCode = Op::NOOP;
        }
        prev = it;
        it = next;
        ++next;
    }
}

void
SegmentedLog::Sync::wait()
{
    optimize();

    waitStart = Clock::now();
    uint64_t writes = 0;
    uint64_t totalBytesWritten = 0;
    uint64_t truncates = 0;
    uint64_t renames = 0;
    uint64_t fdatasyncs = 0;
    uint64_t fsyncs = 0;
    uint64_t closes = 0;
    uint64_t unlinks = 0;

    while (!ops.empty()) {
        Op& op = ops.front();
        FS::File f(op.fd, "-unknown-");
        switch (op.opCode) {
            case Op::WRITE: {
                ssize_t written = FS::write(op.fd,
                        op.writeData.getData(),
                        op.writeData.getLength());
                if (written < 0) {
                    PANIC("Failed to write to fd %d: %s",
                          op.fd,
                          strerror(errno));
                }
                ++writes;
                totalBytesWritten += op.writeData.getLength();
                break;
            }
            case Op::TRUNCATE: {
                FS::truncate(f, op.size);
                ++truncates;
                break;
            }
            case Op::RENAME: {
                FS::rename(f, op.filename1,
                           f, op.filename2);
                ++renames;
                break;
            }
            case Op::FDATASYNC: {
                FS::fdatasync(f);
                ++fdatasyncs;
                break;
            }
            case Op::FSYNC: {
                FS::fsync(f);
                ++fsyncs;
                break;
            }
            case Op::CLOSE: {
                f.close();
                ++closes;
                break;
            }
            case Op::UNLINKAT: {
                FS::removeFile(f, op.filename1);
                ++unlinks;
                break;
            }
            case Op::NOOP: {
                break;
            }
        }
        f.release();
        ops.pop_front();
    }

    waitEnd = Clock::now();
    std::chrono::nanoseconds elapsed = waitEnd - waitStart;
    if (elapsed > diskWriteDurationThreshold) {
        WARNING("Executing filesystem operations took longer than expected "
                "(%s for %lu writes totaling %lu bytes, %lu truncates, "
                "%lu renames, %lu fdatasyncs, %lu fsyncs, %lu closes, and "
                "%lu unlinks)",
                Core::StringUtil::toString(elapsed).c_str(),
                writes,
                totalBytesWritten,
                truncates,
                renames,
                fdatasyncs,
                fsyncs,
                closes,
                unlinks);
    }
}

void
SegmentedLog::Sync::updateStats(Core::RollingStat& nanos) const
{
    std::chrono::nanoseconds elapsed = waitEnd - waitStart;
    nanos.push(uint64_t(elapsed.count()));
    if (elapsed > diskWriteDurationThreshold)
        nanos.noteExceptional(waitStart, uint64_t(elapsed.count()));
}


////////// SegmentedLog::Segment::Record //////////


SegmentedLog::Segment::Record::Record(uint64_t offset)
    : offset(offset)
    , entry()
{
}


////////// SegmentedLog::Segment //////////

SegmentedLog::Segment::Segment()
    : isOpen(false)
    , startIndex(~0UL)
    , endIndex(~0UL - 1)
    , bytes(0)
    , filename("--invalid--")
    , entries()
{
}

std::string
SegmentedLog::Segment::makeClosedFilename() const
{
    return format(CLOSED_SEGMENT_FORMAT,
                  startIndex, endIndex);
}

////////// SegmentedLog public functions //////////


SegmentedLog::SegmentedLog(const FS::File& parentDir,
                           Encoding encoding,
                           const Core::Config& config)
    : encoding(encoding)
    , checksumAlgorithm(config.read<std::string>("storageChecksum", "CRC32"))
    , MAX_SEGMENT_SIZE(config.read<uint64_t>("storageSegmentBytes",
                                             8 * 1024 * 1024))
    , shouldCheckInvariants(config.read<bool>("storageDebug", false))
    , diskWriteDurationThreshold(config.read<uint64_t>(
        "electionTimeoutMilliseconds", 500) / 4)
    , metadata()
    , dir(FS::openDir(parentDir,
                      (encoding == Encoding::BINARY
                        ? "Segmented-Binary"
                        : "Segmented-Text")))
    , openSegmentFile()
    , logStartIndex(1)
    , segmentsByStartIndex()
    , totalClosedSegmentBytes(0)
    , preparedSegments(
        std::max(config.read<uint64_t>("storageOpenSegments", 3),
                 1UL))
    , currentSync(new SegmentedLog::Sync(0, diskWriteDurationThreshold))
    , metadataWriteNanos()
    , filesystemOpsNanos()
    , segmentPreparer()
{
    // 1. 从所有的segment文件的文件名中解析出所有segment的一些基本元数据，但是没有真正解析文件内容
    std::vector<Segment> segments = readSegmentFilenames();

    bool quiet = config.read<bool>("unittest-quiet", false);
    preparedSegments.quietForUnitTests = quiet;
    // 2. 解析出两个metadata文件的内容，文件不存在就not ok
    SegmentedLogMetadata::Metadata metadata1;
    SegmentedLogMetadata::Metadata metadata2;
    bool ok1 = readMetadata("metadata1", metadata1, quiet);
    bool ok2 = readMetadata("metadata2", metadata2, quiet);
    // 3. 将未损坏的最新版本metadata文件内容作为初始化的metadata，如果都不可用，就将metadata默认配置
    if (ok1 && ok2) {
        if (metadata1.version() > metadata2.version())
            metadata = metadata1;
        else
            metadata = metadata2;
    } else if (ok1) {
        metadata = metadata1;
    } else if (ok2) {
        metadata = metadata2;
    } else {
        // Brand new servers won't have metadata, and that's ok.
        if (!segments.empty()) {
            PANIC("No readable metadata file but found segments in %s",
                  dir.path.c_str());
        }
        // metadata默认配置logStartIndex为1
        metadata.set_entries_start(logStartIndex);
    }

    logStartIndex = metadata.entries_start();
    Log::metadata = metadata.raft_metadata();
    // Write both metadata files
    // 4. 将解析结束后的有效metadata内容覆盖到两个metadata文件中，保证两个文件内容有效。
    updateMetadata();
    updateMetadata();
    // 5. 初始化时的updateMetadata可能会新建metadata文件，所以要额外sync dir保证目录项持久化。
    FS::fsync(dir); // in case metadata files didn't exist


    // Read data from segments, closing any open segments.
    // 6. 真正从所有segment文件中解析出实际log内容，并且将所有segment转化成closed，然后加入segmentsByStartIndex内存结构中。
    for (auto it = segments.begin(); it != segments.end(); ++it) {
        Segment& segment = *it;
        bool keep = segment.isOpen ? loadOpenSegment(segment, logStartIndex)
                                   : loadClosedSegment(segment, logStartIndex);
        if (keep) {
            assert(!segment.isOpen);
            uint64_t startIndex = segment.startIndex;
            std::string filename = segment.filename;
            auto result = segmentsByStartIndex.insert({startIndex,
                                                       std::move(segment)});
            if (!result.second) {
                Segment& other = result.first->second;
                PANIC("Two segments contain entry %lu: %s and %s",
                      startIndex,
                      other.filename.c_str(),
                      filename.c_str());
            }
        }
    }

    // Check to make sure no entry is present in more than one segment,
    // and that there's no gap in the numbering for entries we have.
    // 7. 确保所有segment是严格连续且没有空隙、重叠的。
    if (!segmentsByStartIndex.empty()) {
        uint64_t nextIndex = segmentsByStartIndex.begin()->first;
        for (auto it = segmentsByStartIndex.begin();
             it != segmentsByStartIndex.end();
             ++it) {
            Segment& segment = it->second;
            if (nextIndex < segment.startIndex) {
                PANIC("Did not find segment containing entries "
                      "%lu to %lu (inclusive)",
                      nextIndex, segment.startIndex - 1);
            } else if (segment.startIndex < nextIndex) {
                PANIC("Segment %s contains duplicate entries "
                      "%lu to %lu (inclusive)",
                      segment.filename.c_str(),
                      segment.startIndex,
                      std::min(segment.endIndex,
                               nextIndex - 1));
            }
            nextIndex = segment.endIndex + 1;
        }
    }

    // ！！！
    // 这两条 debug 模式下的assert应该提升到release模式下的PANIC，防止新版本的metadata文件由于磁盘静默损坏时，
    // release模式下读到旧的metadata数据导致logStartIndex基于一个旧的值工作，导致在后续程序在执行SegmentedLog::getEntry的时候野崩：
    // if (!segmentsByStartIndex.empty()) {
    //     const Segment& firstSegment = segmentsByStartIndex.begin()->second;
    //     if (logStartIndex < firstSegment.startIndex ||
    //         logStartIndex > firstSegment.endIndex + 1) {
    //         PANIC("Storage metadata says log starts at index %lu, but the first "
    //               "available log segment %s contains entries %lu through %lu. "
    //               "This indicates corrupt storage metadata or missing segment files.",
    //               logStartIndex,
    //               firstSegment.filename.c_str(),
    //               firstSegment.startIndex,
    //               firstSegment.endIndex);
    //     }
    // }

    // Open a segment to write new entries into.
    // 8. 新建一个文件并打开File Descriptor作为新的open segment，以接收后续的log写入。
    uint64_t fileId = preparedSegments.waitForDemand();
    preparedSegments.submitOpenSegment(
        prepareNewSegment(fileId));
    openNewSegment();

    // Launch the segment preparer thread so that we'll have a source for
    // additional new segments.
    // 9. 开一个后台线程时刻保证马上可用的openSegment数目维持在目标值，避免在raft log写入时临时创建openSegment导致性能低下。
    segmentPreparer = std::thread(&SegmentedLog::segmentPreparerMain, this);

    checkInvariants();
}

SegmentedLog::~SegmentedLog()
{
    NOTICE("Closing open segment");
    closeSegment();

    // Stop preparing segments and delete the extras.
    preparedSegments.exit();
    if (segmentPreparer.joinable())
        segmentPreparer.join();
    auto prepared = preparedSegments.releaseAll();
    while (!prepared.empty()) {
        std::string filename = prepared.front().first;
        NOTICE("Removing unused open segment: %s",
               filename.c_str());
        FS::removeFile(dir, filename);
        prepared.pop_front();
    }
    FS::fsync(dir);

    // Keep assertion in Log.h happy. No need to "take" and "complete" this
    // sync since no operations were performed.
    if (currentSync->ops.empty())
        currentSync->completed = true;
}

// 将entries中的log内容新增写进raft log中，同时append到内存的open segment结构中以及对应的持久化文件中（其中持久化的sync对于leader节点由leaderDiskThreadMain后台线程异步执行）。
std::pair<uint64_t, uint64_t>
SegmentedLog::append(const std::vector<const Entry*>& entries)
{
    Segment* openSegment = &getOpenSegment();
    uint64_t startIndex = openSegment->endIndex + 1;
    uint64_t index = startIndex;
    for (auto it = entries.begin(); it != entries.end(); ++it) {
        // 1. 使用当前的entry构造一个record，用于open segment内存结构中记录该log的信息。
        Segment::Record record(openSegment->bytes);
        // Note that record.offset may change later, if this entry doesn't fit.
        record.entry = **it;
        if (record.entry.has_index()) {
            assert(index == record.entry.index());
        } else {
            record.entry.set_index(index);
        }
        // 2. 将entry内容序列化构造出一个用于写持久化文件的buffer
        Core::Buffer buf = serializeProto(record.entry);

        // See if we need to roll over to a new head segment. If someone is
        // writing an entry that is bigger than MAX_SEGMENT_SIZE, just put it
        // in its own segment. This duplicates some code from closeSegment(),
        // but queues up the operations into 'currentSync'.
        // 3. 当open segment的文件剩余空间不足以容纳entry中的log内容时，会进入if分支:
        //    1) 将当前的open segment的文件truncate为实际内容大小然后fsync持久化到磁盘。
        //    2) 关闭当前的open segment fd，并且执行一系列close segment操作。
        //    3) 获取新的open segment用于entry的写入。
        if (openSegment->bytes > sizeof(SegmentHeader) &&
            openSegment->bytes + buf.getLength() > MAX_SEGMENT_SIZE) {
            NOTICE("Rolling over to new head segment: trying to append new "
                   "entry that is %lu bytes long, but open segment is already "
                   "%lu of %lu bytes large",
                   buf.getLength(),
                   openSegment->bytes,
                   MAX_SEGMENT_SIZE);

            // Truncate away any extra 0 bytes at the end from when
            // MAX_SEGMENT_SIZE was allocated.
            currentSync->ops.emplace_back(openSegmentFile.fd,
                                          Sync::Op::TRUNCATE);
            currentSync->ops.back().size = openSegment->bytes;
            currentSync->ops.emplace_back(openSegmentFile.fd,
                                          Sync::Op::FSYNC);
            currentSync->ops.emplace_back(openSegmentFile.release(),
                                          Sync::Op::CLOSE);

            // Rename the file.
            std::string newFilename = openSegment->makeClosedFilename();
            NOTICE("Closing full segment (was %s, renaming to %s)",
                   openSegment->filename.c_str(),
                   newFilename.c_str());
            currentSync->ops.emplace_back(dir.fd, Sync::Op::RENAME);
            currentSync->ops.back().filename1 = openSegment->filename;
            currentSync->ops.back().filename2 = newFilename;
            currentSync->ops.emplace_back(dir.fd, Sync::Op::FSYNC);
            openSegment->filename = newFilename;

            // Bookkeeping.
            openSegment->isOpen = false;
            totalClosedSegmentBytes += openSegment->bytes;

            // Open new segment.
            openNewSegment();
            openSegment = &getOpenSegment();
            record.offset = openSegment->bytes;
        }

        if (buf.getLength() > MAX_SEGMENT_SIZE) {
            WARNING("Trying to append an entry of %lu bytes when the maximum "
                    "segment size is %lu bytes. Placing this entry in its own "
                    "segment. Consider adjusting 'storageSegmentBytes' in the "
                    "config.",
                    buf.getLength(),
                    MAX_SEGMENT_SIZE);
        }

        // 4. 当前的open segment对应文件空间可以容纳entry的log写入了，写入openSegment内存结构以及持久化文件中。
        openSegment->entries.emplace_back(std::move(record));
        openSegment->bytes += buf.getLength();
        currentSync->ops.emplace_back(openSegmentFile.fd, Sync::Op::WRITE);
        currentSync->ops.back().writeData = std::move(buf);
        ++openSegment->endIndex;
        ++index;
    }

    // 本次append的所有entry被write进文件中，随后执行fdatasync真正将数据持久化到磁盘。
    currentSync->ops.emplace_back(openSegmentFile.fd, Sync::Op::FDATASYNC);
    // 这个lastIndex主要用于Leader节点在执行完一批sync操作之后，获取本地raft log的持久化进度用于推进commitIndex。
    currentSync->lastIndex = getLastLogIndex();
    checkInvariants();
    return {startIndex, getLastLogIndex()};
}

const SegmentedLog::Entry&
SegmentedLog::getEntry(uint64_t index) const
{
    if (index < getLogStartIndex() ||
        index > getLastLogIndex()) {
        PANIC("Attempted to access entry %lu outside of log "
              "(start index is %lu, last index is %lu)",
              index, getLogStartIndex(), getLastLogIndex());
    }
    auto it = segmentsByStartIndex.upper_bound(index);
    --it;
    const Segment& segment = it->second;
    assert(segment.startIndex <= index);
    assert(index <= segment.endIndex);
    return segment.entries.at(index - segment.startIndex).entry;
}

uint64_t
SegmentedLog::getLogStartIndex() const
{
    return logStartIndex;
}

uint64_t
SegmentedLog::getLastLogIndex() const
{
    // Although it's a class invariant that there's always an open segment,
    // it's convenient to be able to call this as a helper function when there
    // are no segments.
    if (segmentsByStartIndex.empty())
        return logStartIndex - 1;
    else
        return getOpenSegment().endIndex;
}

std::string
SegmentedLog::getName() const
{
    if (encoding == Encoding::BINARY)
        return "Segmented-Binary";
    else
        return "Segmented-Text";
}

uint64_t
SegmentedLog::getSizeBytes() const
{
    return totalClosedSegmentBytes + getOpenSegment().bytes;
}

std::unique_ptr<Log::Sync>
SegmentedLog::takeSync()
{
    std::unique_ptr<SegmentedLog::Sync> other(
            new SegmentedLog::Sync(getLastLogIndex(),
                                   diskWriteDurationThreshold));
    std::swap(other, currentSync);
    return std::move(other);
}

void
SegmentedLog::syncCompleteVirtual(std::unique_ptr<Log::Sync> sync)
{
    static_cast<SegmentedLog::Sync*>(sync.get())->
        updateStats(filesystemOpsNanos);
}

void
SegmentedLog::truncatePrefix(uint64_t newStartIndex)
{
    if (newStartIndex <= logStartIndex)
        return;

    NOTICE("Truncating log to start at index %lu (was %lu)",
           newStartIndex, logStartIndex);
    logStartIndex = newStartIndex;
    // update metadata before removing files in case of interruption
    // 如果在写metadata的时候发生崩溃导致其中一个metadata文件写损坏，另一个metadata文件可以补救，因为segment文件的截断删除还未发生。
    updateMetadata();

    while (!segmentsByStartIndex.empty()) {
        Segment& segment = segmentsByStartIndex.begin()->second;
        if (logStartIndex <= segment.endIndex)
            // segment文件中部分log还需要的话，整个segment文件全部保留
            break;
        NOTICE("Deleting unneeded segment %s (its end index is %lu)",
               segment.filename.c_str(),
               segment.endIndex);
        currentSync->ops.emplace_back(dir.fd, Sync::Op::UNLINKAT);
        currentSync->ops.back().filename1 = segment.filename;
        if (segment.isOpen) {
            currentSync->ops.emplace_back(openSegmentFile.release(),
                                          Sync::Op::CLOSE);
        } else {
            totalClosedSegmentBytes -= segment.bytes;
        }
        segmentsByStartIndex.erase(segmentsByStartIndex.begin());
    }

    if (segmentsByStartIndex.empty())
        openNewSegment();
    if (currentSync->lastIndex < logStartIndex - 1)
        currentSync->lastIndex = logStartIndex - 1;
    checkInvariants();
}

// 截断log中的最后一部分，有以下原因：
//   1. 与leader的log/snapshot存在冲突，需要截断最后的全部冲突部分
//   2. log被snapshot完全覆盖，需要截断全部log
// 因此该方法不可能发生在leader节点，因此所有的文件操作直接同步sync，不需要currentSync
void
SegmentedLog::truncateSuffix(uint64_t newEndIndex)
{
    if (newEndIndex >= getLastLogIndex())
        return;

    NOTICE("Truncating log to end at index %lu (was %lu)",
           newEndIndex, getLastLogIndex());
    { // Check if the open segment has some entries we need. If so,
      // just truncate that segment, open a new one, and return.
        Segment& openSegment = getOpenSegment();
        if (newEndIndex >= openSegment.startIndex) {
            // 1. openSegment的部分log仍然需要，直接对open segment执行truncate然后重新open一个
            // Update in-memory segment
            uint64_t i = newEndIndex + 1 - openSegment.startIndex;
            openSegment.bytes = openSegment.entries.at(i).offset;
            openSegment.entries.erase(
                openSegment.entries.begin() + int64_t(i),
                openSegment.entries.end());
            openSegment.endIndex = newEndIndex;
            // Truncate and close the open segment, and open a new one.
            closeSegment();
            openNewSegment();
            checkInvariants();
            return;
        }
    }

    { // Remove the open segment.
        // 2. openSegmnet不再需要，先调用closeSegment删除open segment
        Segment& openSegment = getOpenSegment();
        openSegment.endIndex = openSegment.startIndex - 1;
        openSegment.bytes = 0;
        closeSegment();
    }

    // Remove and/or truncate closed segments.
    while (!segmentsByStartIndex.empty()) {
        auto it = segmentsByStartIndex.rbegin();
        Segment& segment = it->second;
        if (segment.endIndex == newEndIndex)
            break;
        if (segment.startIndex > newEndIndex) { // remove segment
            // 3. 当前closed segment不再需要，直接删除
            NOTICE("Removing closed segment %s", segment.filename.c_str());
            FS::removeFile(dir, segment.filename);
            FS::fsync(dir);
            totalClosedSegmentBytes -= segment.bytes;
            segmentsByStartIndex.erase(segment.startIndex);
        } else if (segment.endIndex > newEndIndex) { // truncate segment
            // 4. newEndIndex落在了当前closed segment的index区间内且不等于其endIndex，先将该segment文件的后部分不需要的截断
            // Update in-memory segment
            uint64_t i = newEndIndex + 1 - segment.startIndex;
            uint64_t newBytes = segment.entries.at(i).offset;
            totalClosedSegmentBytes -= (segment.bytes - newBytes);
            segment.bytes = newBytes;
            segment.entries.erase(
                segment.entries.begin() + int64_t(i),
                segment.entries.end());
            segment.endIndex = newEndIndex;

            // 5. ！！这里的truncate+rename顺序和closeSegment方法中的相反，需要先rename在truncate，
            //    防止truncate成功但是rename失败导致后续崩溃恢复时判定数据丢失。
            // Rename the file
            std::string newFilename = segment.makeClosedFilename();
            NOTICE("Truncating closed segment (was %s, renaming to %s)",
                   segment.filename.c_str(),
                   newFilename.c_str());
            FS::rename(dir, segment.filename,
                       dir, newFilename);
            FS::fsync(dir);
            segment.filename = newFilename;

            // Truncate the file
            FS::File f = FS::openFile(dir, segment.filename, O_WRONLY);
            FS::truncate(f, segment.bytes);
            FS::fsync(f);
            
            // 6. truncate结束之后，当前segment的endIndex已经持久化为newEndIndex，回到while头判断break
        }
    }

    // Reopen a segment (so that we can write again)
    openNewSegment();
    checkInvariants();
}

// 将最新的metadata内容覆盖到一个metadata文件中。SegmentedLog实例初始化构造时，该方法可能会新建metadata文件，
// 但是其余时间不会，为了性能所以没有在内部做sync dir操作。
// JOEY_TODO: 抛弃双metadata机制，因为这种恢复其中一个metadata的方式无法应对磁盘静默损坏的场景（可能恢复到旧的元数据），改成和snapshot机制一样的
//            “write tmp file -> fsync tmp file -> rename tmp to formal ->fsync dir”，这样即使是写tmp时崩溃也不会影响
//            formal，然后崩溃恢复一律只读formal，如果formal磁盘损坏，直接PANIC。
//            如果希望进一步避免metadata单独持久化的开销，或者metadata想存一些高频变动（但是不是必须持久化）的变量，则需要将raft log设计成
//            混合WAL，这时候这部分高频非必须metadata的sync就可以被其他log的sync顺带着一起执行。
void
SegmentedLog::updateMetadata()
{
    if (Log::metadata.ByteSize() == 0)
        metadata.clear_raft_metadata();
    else
        // Log::metadata为raft算法关心的元数据，为term和vote_for
        *metadata.mutable_raft_metadata() = Log::metadata;
    // SegmentedLog在raft算法关心的元数据之外，还需要关心自身持久化机制的一些额外元数据，最主要的是logStartIndex
    metadata.set_format_version(1);
    metadata.set_entries_start(logStartIndex);
    metadata.set_version(metadata.version() + 1);
    std::string filename;
    // 两个metadata文件交替写防止损坏后metadata完全不可用。主要是防止写时崩溃导致的损坏，对于磁盘静默损坏防御较差。
    if (metadata.version() % 2 == 1) {
        filename = "metadata1";
    } else {
        filename = "metadata2";
    }

    TimePoint start = Clock::now();

    NOTICE("Writing new storage metadata (version %lu) to %s",
           metadata.version(),
           filename.c_str());
    FS::File file = FS::openFile(dir, filename, O_CREAT|O_WRONLY|O_TRUNC);
    Core::Buffer record = serializeProto(metadata);
    ssize_t written = FS::write(file.fd,
                                record.getData(),
                                record.getLength());
    if (written == -1) {
        PANIC("Failed to write to %s: %s",
              file.path.c_str(), strerror(errno));
    }
    FS::fsync(file);

    TimePoint end = Clock::now();
    std::chrono::nanoseconds elapsed = end - start;
    metadataWriteNanos.push(uint64_t(elapsed.count()));
    if (elapsed > diskWriteDurationThreshold) {
        WARNING("Writing metadata file took longer than expected "
                "(%s for %lu bytes)",
                Core::StringUtil::toString(elapsed).c_str(),
                record.getLength());
        metadataWriteNanos.noteExceptional(start, uint64_t(elapsed.count()));
    }
}

void
SegmentedLog::updateServerStats(Protocol::ServerStats& serverStats) const
{
    Protocol::ServerStats::Storage& stats = *serverStats.mutable_storage();
    stats.set_num_segments(segmentsByStartIndex.size());
    stats.set_open_segment_bytes(getOpenSegment().bytes);
    stats.set_metadata_version(metadata.version());
    metadataWriteNanos.updateProtoBuf(*stats.mutable_metadata_write_nanos());
    filesystemOpsNanos.updateProtoBuf(*stats.mutable_filesystem_ops_nanos());
}


////////// SegmentedLog initialization helper functions //////////


std::vector<SegmentedLog::Segment>
SegmentedLog::readSegmentFilenames()
{
    std::vector<Segment> segments;
    std::vector<std::string> filenames = FS::ls(dir);
    // sorting isn't strictly necessary, but it helps with unit tests
    std::sort(filenames.begin(), filenames.end());
    for (auto it = filenames.begin(); it != filenames.end(); ++it) {
        const std::string& filename = *it;
        if (filename == "metadata1" ||
            filename == "metadata2") {
            continue;
        }
        Segment segment;
        segment.filename = filename;
        segment.bytes = 0;
        { // Closed segment: xxx-yyy
            uint64_t startIndex = 1;
            uint64_t endIndex = 0;
            unsigned bytesConsumed;
            int matched = sscanf(filename.c_str(),
                                 CLOSED_SEGMENT_FORMAT "%n",
                                 &startIndex, &endIndex,
                                 &bytesConsumed);
            if (matched == 2 && bytesConsumed == filename.length()) {
                segment.isOpen = false;
                segment.startIndex = startIndex;
                segment.endIndex = endIndex;
                segments.push_back(segment);
                continue;
            }
        }

        { // Open segment: open-xxx
            uint64_t counter;
            unsigned bytesConsumed;
            int matched = sscanf(filename.c_str(),
                                 OPEN_SEGMENT_FORMAT "%n",
                                 &counter,
                                 &bytesConsumed);
            if (matched == 1 && bytesConsumed == filename.length()) {
                segment.isOpen = true;
                segment.startIndex = ~0UL;
                segment.endIndex = ~0UL - 1;
                segments.push_back(segment);
                preparedSegments.foundFile(counter);
                continue;
            }
        }

        // Neither
        WARNING("%s doesn't look like a valid segment filename (from %s)",
                filename.c_str(),
                (dir.path + "/" + filename).c_str());
    }
    return segments;
}

bool
SegmentedLog::readMetadata(const std::string& filename,
                           SegmentedLogMetadata::Metadata& metadata,
                           bool quiet) const
{
    std::string error;
    FS::File file = FS::tryOpenFile(dir, filename, O_RDONLY);
    if (file.fd == -1) {
        error = format("Could not open %s/%s: %s",
                       dir.path.c_str(), filename.c_str(), strerror(errno));
    } else {
        FS::FileContents reader(file);
        uint64_t offset = 0;
        error = readProtoFromFile(file, reader, &offset, &metadata);
    }
    if (error.empty()) {
        if (metadata.format_version() > 1) {
            PANIC("The format version found in %s is %lu but this code "
                  "only understands version 1",
                  filename.c_str(),
                  metadata.format_version());
        }
        NOTICE("Read metadata version %lu from %s",
               metadata.version(), filename.c_str());
        return true;
    } else {
        if (!quiet) {
            WARNING("Error reading metadata from %s: %s",
                    filename.c_str(), error.c_str());
        }
        return false;
    }
}

bool
SegmentedLog::loadClosedSegment(Segment& segment, uint64_t logStartIndex)
{
    assert(!segment.isOpen);
    FS::File file = FS::openFile(dir, segment.filename, O_RDWR);
    FS::FileContents reader(file);
    uint64_t offset = 0;

    if (reader.getFileLength() < 1) {
        // 1. 已经closed的segment文件不应该是空的，直接PANIC
        PANIC("Found completely empty segment file %s (it doesn't even have "
              "a version field)",
              segment.filename.c_str());
    } else {
        uint8_t version = *reader.get<uint8_t>(0, 1);
        offset += 1;
        if (version != 1) {
            // 2. 当前版本代码不认识非version=1的存储格式，且已closed的segment文件不应该version=0未初始化，直接PANIC
            PANIC("Segment version read from %s was %u, but this code can "
                  "only read version 1",
                  segment.filename.c_str(),
                  version);
        }
        // 3. version校验成功，说明文件的entry存储格式类型正确性校验通过，接下来继续解析校验单条entry。
    }

    if (segment.endIndex < logStartIndex) {
        // 该closed segment文件不含任何有效的entry log，直接删除
        NOTICE("Removing closed segment whose entries are no longer "
               "needed (last index is %lu but log start index is %lu): %s",
               segment.endIndex,
               logStartIndex,
               segment.filename.c_str());
        FS::removeFile(dir, segment.filename);
        FS::fsync(dir);
        return false;
    }

    for (uint64_t index = segment.startIndex;
         index <= segment.endIndex;
         ++index) {
        std::string error;
        if (offset >= reader.getFileLength()) {
            error = "File too short";
        } else {
            segment.entries.emplace_back(offset);
            error = readProtoFromFile(file, reader, &offset,
                                      &segment.entries.back().entry);
        }
        if (!error.empty()) {
            // 4. closed segment文件的entry log解析严格按照filename上的index区间解析，有任何解析不出
            //    就说明文件存在损坏（包括sync失败、磁盘静默损坏），直接PANIC
            PANIC("Could not read entry %lu in log segment %s "
                  "(offset %lu bytes). This indicates the file was "
                  "somehow corrupted. Error was: %s",
                  index,
                  segment.filename.c_str(),
                  offset,
                  error.c_str());
        }
    }
    if (offset < reader.getFileLength()) {
        WARNING("Found an extra %lu bytes at the end of closed segment "
                "%s. This can happen if the server crashed while "
                "truncating the segment. Truncating these now.",
                reader.getFileLength() - offset,
                segment.filename.c_str());
        // TODO(ongaro): do we want to save these bytes somewhere?
        FS::truncate(file, offset);
        FS::fsync(file);
    }
    segment.bytes = offset;
    totalClosedSegmentBytes += segment.bytes;
    return true;
}

bool
SegmentedLog::loadOpenSegment(Segment& segment, uint64_t logStartIndex)
{
    assert(segment.isOpen);
    FS::File file = FS::openFile(dir, segment.filename, O_RDWR);
    FS::FileContents reader(file);
    uint64_t offset = 0;

    if (reader.getFileLength() < 1) {
        // 1. 这种情况对应备用open segment已经创建但是还没被初始化MAX_SEGMENT_SIZE的情况。
        WARNING("Found completely empty segment file %s (it doesn't even have "
                "a version field)",
                segment.filename.c_str());
    } else {
        uint8_t version = *reader.get<uint8_t>(0, 1);
        offset += 1;
        if (version != 1) {
            uint64_t remainingBytes = reader.getFileLength() - offset;
            if (version == 0 &&
                isAllZeros(reader.get(
                               offset, remainingBytes), remainingBytes)) {
                // 2. 这种情况对应备用open segment已经创建并且已经成功初始化MAX_SEGMENT_SIZE，但是还没写入header的情况。
                // move the offset to the end of the file. allow the
                // existing cleanup mechanism to remove this file.
                offset = reader.getFileLength();
            } else {
                // 3. 这种情况对应segment文件的version在本版本代码中不认识，因此不知道entry存储格式也就解析不出来后续的entry log。
                PANIC("Segment version read from %s was %u, "
                      "but this code can only read version 1",
                      segment.filename.c_str(),
                      version);
            }
        }
        // 4. version校验成功，说明文件的entry存储格式类型正确性校验通过，接下来继续解析校验单条entry。
    }

    uint64_t lastIndex = 0;
    while (offset < reader.getFileLength()) {
        // 从segment文件中解析出下一条entry的log内容，并且填充Record内存结构
        segment.entries.emplace_back(offset);
        std::string error = readProtoFromFile(
                file,
                reader,
                &offset,
                &segment.entries.back().entry);
        if (!error.empty()) {
            // 下一条entry log解析失败，先将其Record从内存结构中删除
            segment.entries.pop_back();
            uint64_t remainingBytes = reader.getFileLength() - offset;
            if (isAllZeros(reader.get(offset, remainingBytes),
                           remainingBytes)) {
                // 5. 这种情况对应此前该open segment正在正常等待接收写入，但是程序突然崩溃，但是所有已写入entry都是已持久化且正确的。
                WARNING("Truncating %lu zero bytes at the end of log "
                        "segment %s (%lu bytes into the segment, "
                        "following  entry %lu). This is most likely "
                        "because the server shutdown uncleanly.",
                        remainingBytes,
                        segment.filename.c_str(),
                        offset,
                        lastIndex);
            } else {
                // 6. 这种情况对应下一条entry log由于此前sync时崩溃或者磁盘静默损坏等原因导致后续的entry都不可用了。
                // TODO(ongaro): do we want to save these bytes somewhere?
                WARNING("Could not read entry in log segment %s "
                        "(%lu bytes into the segment, following "
                        "entry %lu), probably because it was being "
                        "written when the server crashed. Discarding the "
                        "remainder of the file (%lu bytes). Error was: %s",
                        segment.filename.c_str(),
                        offset,
                        lastIndex,
                        remainingBytes,
                        error.c_str());
            }
            // ！！！注意，对于open segment文件解析过程中遇到损坏的entry，本设计选择直接截断丢弃后续的所有entry。这
            // ！！！可能会导致本机的部分log丢失，但是在Raft共识协议下，丢失的log后续必然可以通过集群其他正确的多数派恢复。
            FS::truncate(file, offset);
            FS::fsync(file);
            break;
        }
        lastIndex = segment.entries.back().entry.index();
    }

    bool remove = false;
    if (segment.entries.empty()) {
        NOTICE("Removing empty segment: %s", segment.filename.c_str());
        remove = true;
    } else if (segment.entries.back().entry.index() < logStartIndex) {
        // 这里可以看出从metadata文件中恢复出的logStartIndex才是权威数据。
        NOTICE("Removing open segment whose entries are no longer "
               "needed (last index is %lu but log start index is %lu): %s",
               segment.entries.back().entry.index(),
               logStartIndex,
               segment.filename.c_str());
        remove = true;
    }
    if (remove) {
        // 该open segment文件不含任何有效的entry log，直接删除
        FS::removeFile(dir, segment.filename);
        FS::fsync(dir);
        return false;
    } else {
        // 经过以上解析，该open segment文件已经被truncate为所有有效entry log的紧凑size，可以直接将其closed。
        // 当然，对于损坏的log直接截断丢弃了，但是后续可通过集群多数派恢复。
        segment.bytes = offset;
        totalClosedSegmentBytes += segment.bytes;
        segment.isOpen = false;
        segment.startIndex = segment.entries.front().entry.index();
        segment.endIndex = segment.entries.back().entry.index();
        std::string newFilename = segment.makeClosedFilename();
        NOTICE("Closing open segment %s, renaming to %s",
                segment.filename.c_str(),
                newFilename.c_str());
        FS::rename(dir, segment.filename,
                   dir, newFilename);
        FS::fsync(dir);
        segment.filename = newFilename;
        return true;
    }
}


////////// SegmentedLog normal operation helper functions //////////


void
SegmentedLog::checkInvariants()
{
    if (!shouldCheckInvariants)
        return;
#if DEBUG
    assert(openSegmentFile.fd >= 0);
    assert(!segmentsByStartIndex.empty());
    assert(logStartIndex >= segmentsByStartIndex.begin()->second.startIndex);
    assert(logStartIndex <= segmentsByStartIndex.begin()->second.endIndex + 1);
    assert(currentSync.get() != NULL);
    uint64_t closedBytes = 0;
    for (auto it = segmentsByStartIndex.begin();
         it != segmentsByStartIndex.end();
         ++it) {
        auto next = it;
        ++next;
        Segment& segment = it->second;
        assert(it->first == segment.startIndex);
        assert(segment.startIndex > 0);
        assert(segment.entries.size() ==
               segment.endIndex + 1 - segment.startIndex);
        uint64_t lastOffset = 0;
        for (uint64_t i = 0; i < segment.entries.size(); ++i) {
            assert(segment.entries.at(i).entry.index() ==
                   segment.startIndex + i);
            if (i == 0)
                assert(segment.entries.at(0).offset == sizeof(SegmentHeader));
            else
                assert(segment.entries.at(i).offset > lastOffset);
            lastOffset = segment.entries.at(i).offset;
        }
        if (next == segmentsByStartIndex.end()) {
            assert(segment.isOpen);
            assert(segment.endIndex >= segment.startIndex - 1);
            assert(Core::StringUtil::startsWith(segment.filename, "open-"));
            assert(segment.bytes >= sizeof(SegmentHeader));
        } else {
            assert(!segment.isOpen);
            assert(segment.endIndex >= segment.startIndex);
            assert(next->second.startIndex == segment.endIndex + 1);
            assert(segment.bytes > sizeof(SegmentHeader));
            closedBytes += segment.bytes;
            assert(segment.filename == segment.makeClosedFilename());
        }
    }
    assert(closedBytes == totalClosedSegmentBytes);
#endif /* DEBUG */
}

// 标准的close一个open segment的流程，根据openSegment内存结构的start、end、bytes信息，
// 确定是否直接删除segment文件，或者trunctate截断到哪个byte偏移。（所以调用closeSegment前需要先Update in-memory segment）
// 由于rename发生在truncate+fsync结束之后，因此通过这种方式close segment，只要后续能够在dir中读到closed文件名，那文件的截断就肯定是正确生效的。
void
SegmentedLog::closeSegment()
{
    if (openSegmentFile.fd < 0)
        return;
    Segment& openSegment = getOpenSegment();
    if (openSegment.startIndex > openSegment.endIndex) {
        // Segment is empty; just remove it.
        NOTICE("Removing empty open segment (start index %lu): %s",
               openSegment.startIndex,
               openSegment.filename.c_str());
        openSegmentFile.close();
        FS::removeFile(dir, openSegment.filename);
        FS::fsync(dir);
        segmentsByStartIndex.erase(openSegment.startIndex);
        return;
    }

    // Truncate away any extra 0 bytes at the end from when
    // MAX_SEGMENT_SIZE was allocated, or in the case of truncateSuffix,
    // truncate away actual entries that are no longer desired.
    FS::truncate(openSegmentFile, openSegment.bytes);
    FS::fsync(openSegmentFile);
    openSegmentFile.close();

    // Rename the file.
    std::string newFilename = openSegment.makeClosedFilename();
    NOTICE("Closing segment (was %s, renaming to %s)",
           openSegment.filename.c_str(),
           newFilename.c_str());
    FS::rename(dir, openSegment.filename,
               dir, newFilename);
    FS::fsync(dir);
    openSegment.filename = newFilename;

    openSegment.isOpen = false;
    totalClosedSegmentBytes += openSegment.bytes;
}

SegmentedLog::Segment&
SegmentedLog::getOpenSegment()
{
    assert(!segmentsByStartIndex.empty());
    return segmentsByStartIndex.rbegin()->second;
}

// 该方法命名不是特别严谨，在openSegment被close之后到重新openNewSegment之前，其实segmentsByStartIndex中的最后一个成员也是closed的。
// 但是在openNewSegment后对外正常使用的过程中，这个命名方式都是合理的。
const SegmentedLog::Segment&
SegmentedLog::getOpenSegment() const
{
    assert(!segmentsByStartIndex.empty());
    return segmentsByStartIndex.rbegin()->second;
}

// 获取一个新的open segment用于后续新的raft log的追加写入，需要先保证旧的已写满open segment已经被close且对应的fd已经触发（异步）truncate和sync。
void
SegmentedLog::openNewSegment()
{
    assert(openSegmentFile.fd < 0);
    assert(segmentsByStartIndex.empty() ||
           !segmentsByStartIndex.rbegin()->second.isOpen);

    Segment newSegment;
    newSegment.isOpen = true;
    newSegment.startIndex = getLastLogIndex() + 1;
    // 新的openSegment为empty，将其endIndex设置为startIndex - 1，刚好num = endIndex - startIndex + 1 = 0。
    // Log整体的StartIndex和LastLogIndex也是相同的道理。
    newSegment.endIndex = newSegment.startIndex - 1;
    newSegment.bytes = sizeof(SegmentHeader);
    // This can throw ThreadInterruptedException, but it shouldn't ever, since
    // this class shouldn't have been destroyed yet.
    // 获取一个已打开可直接写入内容的File Descriptor，由一个专门的segmentPreparerMain线程在后台准备好以节省写入耗时。
    auto s = preparedSegments.waitForOpenSegment();
    newSegment.filename = s.first;
    // 将获取到的File Descriptor记录到Log实例的全局openSegmentFile成员，也就是说Log实例的全局可写open segment fd只有一个。
    openSegmentFile = std::move(s.second);
    // 将新的openSegment压进segmentsByStartIndex，之后其最后一个segment就是open的
    segmentsByStartIndex.insert({newSegment.startIndex, newSegment});
}

std::string
SegmentedLog::readProtoFromFile(const FS::File& file,
                                FS::FileContents& reader,
                                uint64_t* offset,
                                google::protobuf::Message* out) const
{
    uint64_t loffset = *offset;
    char checksum[Core::Checksum::MAX_LENGTH];
    uint64_t bytesRead = reader.copyPartial(loffset, checksum,
                                            sizeof(checksum));
    uint32_t checksumBytes = Core::Checksum::length(checksum,
                                                    uint32_t(bytesRead));
    if (checksumBytes == 0)
        return format("Missing checksum in file %s", file.path.c_str());
    loffset += checksumBytes;

    uint64_t dataLen;
    if (reader.copyPartial(loffset, &dataLen, sizeof(dataLen)) <
        sizeof(dataLen)) {
        return format("Record length truncated in file %s", file.path.c_str());
    }
    dataLen = be64toh(dataLen);
    if (reader.getFileLength() < loffset + sizeof(dataLen) + dataLen) {
        return format("ProtoBuf truncated in file %s", file.path.c_str());
    }

    const void* checksumCoverage = reader.get(loffset,
                                              sizeof(dataLen) + dataLen);
    std::string error = Core::Checksum::verify(checksum, checksumCoverage,
                                               sizeof(dataLen) + dataLen);
    if (!error.empty()) {
        return format("Checksum verification failure on %s: %s",
                      file.path.c_str(), error.c_str());
    }
    loffset += sizeof(dataLen);
    const void* data = reader.get(loffset, dataLen);
    loffset += dataLen;

    switch (encoding) {
        case SegmentedLog::Encoding::BINARY: {
            Core::Buffer contents(const_cast<void*>(data),
                                  dataLen,
                                  NULL);
            if (!Core::ProtoBuf::parse(contents, *out)) {
                return format("Failed to parse protobuf in %s",
                              file.path.c_str());
            }
            break;
        }
        case SegmentedLog::Encoding::TEXT: {
            std::string contents(static_cast<const char*>(data), dataLen);
            Core::ProtoBuf::Internal::fromString(contents, *out);
            break;
        }
    }
    *offset = loffset;
    return "";
}

Core::Buffer
SegmentedLog::serializeProto(const google::protobuf::Message& in) const
{
    // TODO(ongaro): can the intermediate buffer be avoided?
    const void* data = NULL;
    uint64_t len = 0;
    Core::Buffer binaryContents;
    std::string asciiContents;
    switch (encoding) {
        case SegmentedLog::Encoding::BINARY: {
            Core::ProtoBuf::serialize(in, binaryContents);
            data = binaryContents.getData();
            len = binaryContents.getLength();
            break;
        }
        case SegmentedLog::Encoding::TEXT: {
            asciiContents = Core::ProtoBuf::dumpString(in);
            data = asciiContents.data();
            len = asciiContents.length();
            break;
        }
    }
    uint64_t netLen = htobe64(len);
    char checksum[Core::Checksum::MAX_LENGTH];
    uint32_t checksumLen = Core::Checksum::calculate(
        checksumAlgorithm.c_str(), {
            {&netLen, sizeof(netLen)},
            {data, len},
        },
        checksum);

    uint64_t totalLen = checksumLen + sizeof(netLen) + len;
    char* buf = new char[totalLen];
    Core::Buffer record(
        buf,
        totalLen,
        Core::Buffer::deleteArrayFn<char>);
    Core::Util::memcpy(buf, {
        {checksum, checksumLen},
        {&netLen, sizeof(netLen)},
        {data, len},
    });
    return record;
}


////////// SegmentedLog segment preparer thread functions //////////

// 执行这个方法过后创建的备用open segment已经存在于目录中，“version = 1” + “其余全0”，并且size等于MAX_SEGMENT_SIZE。
// 如果不被使用，那么在后续的崩溃恢复中将被删除。
std::pair<std::string, FS::File>
SegmentedLog::prepareNewSegment(uint64_t id)
{
    TimePoint start = Clock::now();

    std::string filename = format(OPEN_SEGMENT_FORMAT, id);
    FS::File file = FS::openFile(dir, filename,
                                 O_CREAT|O_EXCL|O_RDWR);
    FS::allocate(file, 0, MAX_SEGMENT_SIZE);
    // SegmentHeader以及version的存在主要是为了兼容后续的代码迭代，假如后续entry存储格式发生改变，可以设置新格式的文件version为2，
    // 这时新代码对于version为1的旧文件仍然可以按照旧格式继续正常解析，达到向前兼容的效果。假如没有version字段，将无法区分旧文件格式和新文件格式，导致无法解析。
    SegmentHeader header;
    header.version = 1;
    ssize_t written = FS::write(file.fd,
                                &header,
                                sizeof(header));
    if (written == -1) {
        PANIC("Failed to write header to %s: %s",
              file.path.c_str(), strerror(errno));
    }
    FS::fsync(file);
    FS::fsync(dir);

    TimePoint end = Clock::now();
    std::chrono::nanoseconds elapsed = end - start;
    // TODO(ongaro): record elapsed times into RollingStat in a thread-safe way
    if (elapsed > diskWriteDurationThreshold) {
        WARNING("Preparing open segment file took longer than expected (%s)",
                Core::StringUtil::toString(elapsed).c_str());
    }
    return {std::move(filename), std::move(file)};
}


void
SegmentedLog::segmentPreparerMain()
{
    Core::ThreadId::setName("SegmentPreparer");
    while (true) {
        uint64_t fileId = 0;
        try {
            fileId = preparedSegments.waitForDemand();
        } catch (const Core::Util::ThreadInterruptedException&) {
            VERBOSE("Exiting");
            break;
        }
        preparedSegments.submitOpenSegment(
            prepareNewSegment(fileId));
    }
}

} // namespace LogCabin::Storage
} // namespace LogCabin

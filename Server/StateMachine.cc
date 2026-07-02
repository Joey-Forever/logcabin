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

#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "Core/Debug.h"
#include "Core/Mutex.h"
#include "Core/ProtoBuf.h"
#include "Core/Random.h"
#include "Core/ThreadId.h"
#include "Core/Util.h"
#include "Server/Globals.h"
#include "Server/RaftConsensus.h"
#include "Server/StateMachine.h"
#include "Storage/SnapshotFile.h"
#include "Tree/ProtoBuf.h"

namespace LogCabin {
namespace Server {

namespace PC = LogCabin::Protocol::Client;


// for testing purposes
bool stateMachineSuppressThreads = false;
uint32_t stateMachineChildSleepMs = 0;

StateMachine::StateMachine(std::shared_ptr<RaftConsensus> consensus,
                           Core::Config& config,
                           Globals& globals)
    // StateMachine实例需要持有RaftConsensus实例的引用，保证raft实例在StateMachine析构之后再析构，因为state machine实例会调用raft实例。
    : consensus(consensus)
    , globals(globals)
      // This configuration option isn't advertised as part of the public API:
      // it's only useful for testing.
    , snapshotBlockPercentage(
            config.read<uint64_t>("snapshotBlockPercentage", 0))
    , snapshotMinLogSize(
            config.read<uint64_t>("snapshotMinLogSize", 64UL * 1024 * 1024))
    , snapshotRatio(
            config.read<uint64_t>("snapshotRatio", 4))
    , snapshotWatchdogInterval(std::chrono::milliseconds(
            config.read<uint64_t>("snapshotWatchdogMilliseconds", 10000)))
      // TODO(ongaro): This should be configurable, but it must be the same for
      // every server, so it's dangerous to put it in the config file. Need to
      // use the Raft log to agree on this value. Also need to inform clients
      // of the value and its changes, so that they can send keep-alives at
      // appropriate intervals. For now, servers time out after about an hour,
      // and clients send keep-alives every minute.
      // 这个东西涉及到state machine中sessions表的数据正确性，如果每个server节点的配置不一致的话，
      // 会导致集群的共识出现偏差，出现有的server apply到了某个index时一个session失效了但是其他
      // server apply到这个index时却没有失效该session。所以这个东西放在config中可配置就会比较危险。
      // 并且在可配置的情况下，必须将该值加入raft log做共识，并且必须告知client端做keepalive调整，所以
      // 总的看来，作者选择直接在代码中写死该数值了。
      // 这也能看出，其实sessions去重表并不适合在raft state machine中内建，更合适的是把command幂等控制权完全交给业务层。
    , sessionTimeoutNanos(1000UL * 1000 * 1000 * 60 * 60)
    , unknownRequestMessageBackoff(std::chrono::milliseconds(
            config.read<uint64_t>("stateMachineUnknownRequestMessage"
                                  "BackoffMilliseconds", 10000)))
    , mutex()
    , entriesApplied()
    , snapshotSuggested()
    , snapshotStarted()
    , snapshotCompleted()
    , exiting(false)
    , childPid(0)
    , lastApplied(0)
    , lastUnknownRequestMessage(TimePoint::min())
    , numUnknownRequests(0)
    , numUnknownRequestsSinceLastMessage(0)
    , numSnapshotsAttempted(0)
    , numSnapshotsFailed(0)
    , numRedundantAdvanceVersionEntries(0)
    , numRejectedAdvanceVersionEntries(0)
    , numSuccessfulAdvanceVersionEntries(0)
    , numTotalAdvanceVersionEntries(0)
    , isSnapshotRequested(false)
    , maySnapshotAt(TimePoint::min())
    , sessions()
    , tree()
    , versionHistory()
    , writer()
    , applyThread()
    , snapshotThread()
    , snapshotWatchdogThread()
{
    // 1. state machine初始的versionHistory只有1，runningVersion也只到1
    versionHistory.insert({0, 1});
    // 2. 将当前运行代码的state machine版本区间设置进localServer中，用于集群leader
    //    节点获取后进行集群滚动升级
    consensus->setSupportedStateMachineVersions(MIN_SUPPORTED_VERSION,
                                                MAX_SUPPORTED_VERSION);
    // 3. state machine初始化时先创建必要的几个线程
    if (!stateMachineSuppressThreads) {
    //  1）用于将raft log中已经commit的log entry apply到state machine中
        applyThread = std::thread(&StateMachine::applyThreadMain, this);
    //  2）用于触发snapshot的生成（通过fork一个子进程的方式，COW）
        snapshotThread = std::thread(&StateMachine::snapshotThreadMain, this);
    //  3）用于主进程监听子进程的snapshot生成进展
        snapshotWatchdogThread = std::thread(
                &StateMachine::snapshotWatchdogThreadMain, this);
    }
}

// 实际上，当主线程的析构流程执行到这里的时候，所有service的worker线程都已经安全退出了，
// 后续需要处理的只有state machine实例和raft实例内部创建的后台线程以及所有的peer线程的安全退出了。
// 由于StateMachine持有raft实例的引用，所以state machine析构时raft实例必然还存在
StateMachine::~StateMachine()
{
    NOTICE("Shutting down");
    // StateMachine析构时raft实例必然还存在，先调用raft实例的exit，引发raft实例成员方法下工作的后台线程以及所有的peer线程退出
    if (consensus) // sometimes missing for testing
        consensus->exit();

    // ！！！
    // 必须先在此等待所有state machine内部创建的后台线程都退出了，才可以进入raft实例的析构，因为这些后台线程会使用到raft实例方法。

    // applyThreadMain线程会在执行raft的getNextEntry方法时看到raft exiting而退出
    if (applyThread.joinable())
        applyThread.join();
    // applyThreadMain线程退出时会设置state machine exiting，随后触发statemachine相关的后台线程退出
    if (snapshotThread.joinable())
        snapshotThread.join();
    if (snapshotWatchdogThread.joinable())
        snapshotWatchdogThread.join();
    NOTICE("Joined with threads");
}

bool
StateMachine::query(const Query::Request& request,
                    Query::Response& response) const
{
    std::lock_guard<Core::Mutex> lockGuard(mutex);
    if (request.has_tree()) {
        Tree::ProtoBuf::readOnlyTreeRPC(tree,
                                        request.tree(),
                                        *response.mutable_tree());
        return true;
    }
    warnUnknownRequest(request, "does not understand the given request");
    return false;
}

void
StateMachine::updateServerStats(Protocol::ServerStats& serverStats) const
{
    std::lock_guard<Core::Mutex> lockGuard(mutex);
    Core::Time::SteadyTimeConverter time;
    serverStats.clear_state_machine();
    Protocol::ServerStats::StateMachine& smStats =
        *serverStats.mutable_state_machine();
    smStats.set_snapshotting(childPid != 0);
    smStats.set_last_applied(lastApplied);
    smStats.set_num_sessions(sessions.size());
    smStats.set_num_unknown_requests(numUnknownRequests);
    smStats.set_num_snapshots_attempted(numSnapshotsAttempted);
    smStats.set_num_snapshots_failed(numSnapshotsFailed);
    smStats.set_num_redundant_advance_version_entries(
        numRedundantAdvanceVersionEntries);
    smStats.set_num_rejected_advance_version_entries(
        numRejectedAdvanceVersionEntries);
    smStats.set_num_successful_advance_version_entries(
        numSuccessfulAdvanceVersionEntries);
    smStats.set_num_total_advance_version_entries(
        numTotalAdvanceVersionEntries);
    smStats.set_min_supported_version(MIN_SUPPORTED_VERSION);
    smStats.set_max_supported_version(MAX_SUPPORTED_VERSION);
    smStats.set_running_version(getVersion(lastApplied));
    smStats.set_may_snapshot_at(time.unixNanos(maySnapshotAt));
    tree.updateServerStats(*smStats.mutable_tree());
}

void
StateMachine::wait(uint64_t index) const
{
    std::unique_lock<Core::Mutex> lockGuard(mutex);
    while (lastApplied < index)
        entriesApplied.wait(lockGuard);
}

bool
StateMachine::waitForResponse(uint64_t logIndex,
                              const Command::Request& command,
                              Command::Response& response) const
{
    std::unique_lock<Core::Mutex> lockGuard(mutex);
    // read-write类状态机命令rpc service 最后会在这里等待对应的 Raft log 最终被apply到state machine，
    // 然后再构造出response回复给client。
    while (lastApplied < logIndex)
        entriesApplied.wait(lockGuard);

    // Need to check whether we understood the request at the time it
    // was applied using getVersion(logIndex), then reply and return true/false
    // based on that. Existing commands have been around since version 1, so we
    // skip this check for now.
    uint16_t versionThen = getVersion(logIndex);

    if (command.has_tree()) {
        const PC::ExactlyOnceRPCInfo& rpcInfo = command.tree().exactly_once();
        auto sessionIt = sessions.find(rpcInfo.client_id());
        if (sessionIt == sessions.end()) {
            WARNING("Client %lu session expired but client still active",
                    rpcInfo.client_id());
            response.mutable_tree()->
                set_status(PC::Status::SESSION_EXPIRED);
            return true;
        }
        const Session& session = sessionIt->second;
        auto responseIt = session.responses.find(rpcInfo.rpc_number());
        if (responseIt == session.responses.end()) {
            // The response for this RPC has already been removed: the client
            // is not waiting for it. This request is just a duplicate that is
            // safe to drop.
            WARNING("Client %lu asking for discarded response to RPC %lu",
                    rpcInfo.client_id(), rpcInfo.rpc_number());
            response.mutable_tree()->
                set_status(PC::Status::SESSION_EXPIRED);
            return true;
        }
        response = responseIt->second;
        return true;
    } else if (command.has_open_session()) {
        response.mutable_open_session()->
            set_client_id(logIndex);
        return true;
    } else if (versionThen >= 2 && command.has_close_session()) {
        response.mutable_close_session(); // no fields to set
        return true;
    } else if (command.has_advance_version()) {
        response.mutable_advance_version()->
            set_running_version(versionThen);
        return true;
    }
    // don't warnUnknownRequest here, since we already did so in apply()
    return false;
}

bool
StateMachine::isTakingSnapshot() const
{
    std::lock_guard<Core::Mutex> lockGuard(mutex);
    return childPid != 0;
}

void
StateMachine::startTakingSnapshot()
{
    std::unique_lock<Core::Mutex> lockGuard(mutex);
    if (childPid == 0) {
        NOTICE("Administrator requested snapshot");
        isSnapshotRequested = true;
        snapshotSuggested.notify_all();
        // This waits on numSnapshotsAttempted to change, since waiting on
        // childPid != 0 would risk missing an entire snapshot that started and
        // completed before this thread was scheduled.
        uint64_t nextSnapshot = numSnapshotsAttempted + 1;
        while (!exiting && numSnapshotsAttempted < nextSnapshot) {
            snapshotStarted.wait(lockGuard);
        }
    }
}

void
StateMachine::stopTakingSnapshot()
{
    std::unique_lock<Core::Mutex> lockGuard(mutex);
    int pid = childPid;
    if (pid != 0) {
        NOTICE("Administrator aborted snapshot");
        killSnapshotProcess(Core::HoldingMutex(lockGuard), SIGTERM);
        while (!exiting && pid == childPid) {
            snapshotCompleted.wait(lockGuard);
        }
    }
}

std::chrono::nanoseconds
StateMachine::getInhibit() const
{
    std::lock_guard<Core::Mutex> lockGuard(mutex);
    TimePoint now = Clock::now();
    if (maySnapshotAt <= now) {
        return std::chrono::nanoseconds::zero();
    } else {
        return maySnapshotAt - now;
    }
}

void
StateMachine::setInhibit(std::chrono::nanoseconds duration)
{
    std::lock_guard<Core::Mutex> lockGuard(mutex);
    if (duration <= std::chrono::nanoseconds::zero()) {
        maySnapshotAt = TimePoint::min();
        NOTICE("Administrator permitted snapshotting");
    } else {
        TimePoint now = Clock::now();
        maySnapshotAt = now + duration;
        if (maySnapshotAt < now) { // overflow
            maySnapshotAt = TimePoint::max();
        }
        NOTICE("Administrator inhibited snapshotting for the next %s",
               Core::StringUtil::toString(maySnapshotAt - now).c_str());
    }
    snapshotSuggested.notify_all();
}


////////// StateMachine private methods //////////

// client在请求的时候raft log中会给他开一个session，并且对于每次该session的请求entry都会在raft log中记录exactly_once，用于标记唯一的{client_id, rpc_number},
// 但是raft log本身不会对这个pair进行去重，也就是raft log是可能存在重复的{client_id，rpc_number}的，但是raft log只需要顺序apply即可，state machine中会维护
// 真正的内存sessions表，在state machine执行apply的时候会对照内存sessions表再执行去重，避免state machine的重复apply。也就意味着，在每次生成snapshot对state machine
// 进行持久化的时候，内存sessions表也需要被持久化。
// ！！这里的 exactly-once 不是“exactly-once appended to log”，而是“exactly-once applied to state machine”。
void
StateMachine::apply(const RaftConsensus::Entry& entry)
{
    Command::Request command;
    // 1. 从 entry中解析出实际的command
    if (!Core::ProtoBuf::parse(entry.command, command)) {
        PANIC("Failed to parse protobuf for entry %lu",
              entry.index);
    }
    // 2. 获取当前command在apply时对应的最新的集群state machine version，可用于判断该条命令需不需要实际执行，
    //    这是对版本落后server的保护，因为有的落后版本server代码无法识别某条command。由于version update是
    //    进入raft log共识的，所以可以保持集群一致，即当前apply的command要么所有节点都执行要么所有都不执行。
    uint16_t runningVersion = getVersion(entry.index - 1);
    if (command.has_tree()) {
        PC::ExactlyOnceRPCInfo rpcInfo = command.tree().exactly_once();
        auto it = sessions.find(rpcInfo.client_id());
        if (it == sessions.end()) {
            // session does not exist
        } else {
            // session exists
            Session& session = it->second;
            expireResponses(session, rpcInfo.first_outstanding_rpc());
            if (rpcInfo.rpc_number() < session.firstOutstandingRPC) {
                // response already discarded, do not re-apply
            } else {
                auto inserted = session.responses.insert(
                                                {rpcInfo.rpc_number(), {}});
                if (inserted.second) {
                    // response not found, apply and save it
                    Tree::ProtoBuf::readWriteTreeRPC(
                        tree,
                        command.tree(),
                        *inserted.first->second.mutable_tree());
                    session.lastModified = entry.clusterTime;
                } else {
                    // response exists, do not re-apply
                }
            }
        }
    } else if (command.has_open_session()) {
        uint64_t clientId = entry.index;
        Session& session = sessions.insert({clientId, {}}).first->second;
        session.lastModified = entry.clusterTime;
    } else if (command.has_close_session()) {
        if (runningVersion >= 2) {
            sessions.erase(command.close_session().client_id());
        } else {
            // Command is ignored in version < 2.
            // 3. 虽然当前代码能够识别该command，但是由于apply到该entry时集群的runningVersion还未升级到该新版本，
            //    所以必须忽略，对旧版本节点保持兼容。
            warnUnknownRequest(command, "may not process the given request, "
                               "which was introduced in version 2");
        }
    } else if (command.has_advance_version()) {
        // 4. 该command是一条集群state machine版本升级命令
        uint16_t requested = Core::Util::downCast<uint16_t>(
                command.advance_version(). requested_version());
        if (requested < runningVersion) {
            // 5. 升级的版本小于runningVersion，拒绝降级。这种情况一般是由于有旧版本server加入了集群，
            //    导致leader发起了低版本升级，新版本server直接拒绝即可，旧版本server会自行PANIC。
            WARNING("Rejecting downgrade of state machine version "
                    "(running version %u but command at log index %lu wants "
                    "to switch to version %u)",
                    runningVersion,
                    entry.index,
                    requested);
            ++numRejectedAdvanceVersionEntries;
        } else if (requested > runningVersion) {
            if (requested > MAX_SUPPORTED_VERSION) {
                // 6. 升级的版本虽然比runningVersion大，但是比本机当前运行代码的support max版本还要大，
                //    直接崩溃。这一般发生在本机作为新加入集群的server节点运行了旧版本代码，但是其实集群此前已经
                //    升级到了该更新的版本，如果允许这类server节点加入集群，将会导致集群版本降级，所以直接崩了本机。
                PANIC("Cannot upgrade state machine to version %u (from %u) "
                      "because this code only supports up to version %u",
                      requested,
                      runningVersion,
                      MAX_SUPPORTED_VERSION);
            } else {
                NOTICE("Upgrading state machine to version %u (from %u)",
                       requested,
                       runningVersion);
                // 7. 成功升级，将新版本insert进versionHistory中生效，后续的command apply将以该最新version为runningVersion
                //    决定是否apply。
                versionHistory.insert({entry.index, requested});
            }
            ++numSuccessfulAdvanceVersionEntries;
        } else { // requested == runningVersion
            // nothing to do
            // If this stat is high, see note in RaftConsensus.cc.
            ++numRedundantAdvanceVersionEntries;
        }
        ++numTotalAdvanceVersionEntries;
    } else { // unknown command
        // This is (deterministically) ignored by all state machines running
        // the current version.
        // 8. 本机当前运行的代码版本无法识别该command因此无法apply，说明本机运行的代码版本较旧，其他
        //    新版本server即使能够识别该command也不会执行，滚动升级照顾的正是这类旧版本server。
        warnUnknownRequest(command, "does not understand the given request");
    }
}

// 该方法是state machine的用于将commit index apply到state machine中的后台线程执行函数，
// 在commitIndex未被推进到next apply entry的绝大多数时候，该线程都是在Raftconsensus实例的getNextEntry
// 方法内sleep wait stateChanged，等待commitIndex被推进后被唤醒。
void
StateMachine::applyThreadMain()
{
    Core::ThreadId::setName("StateMachine");
    try {
        while (true) {
            // 1. 先进入Raftconsensus实例的genNextEntry方法中阻塞性获取next apply entry，
            //    这里先不会持有state machine mutex，因为进入getNextEntry后会持有raft mutex
            //    执行相关操作或sleep wait stateChanged。
            RaftConsensus::Entry entry = consensus->getNextEntry(lastApplied);
            // 2. 成功拿到了next apply entry，开始持有state machine mutex进入真正的apply等操作。
            std::lock_guard<Core::Mutex> lockGuard(mutex);
            switch (entry.type) {
                case RaftConsensus::Entry::SKIP:
                    // 3. 该entry不是state machine关心的DATA类型entry log，直接跳过
                    break;
                case RaftConsensus::Entry::DATA:
                    // 4. 该entry是DATA类型entry log，真正执行apply state machine操作
                    apply(entry);
                    break;
                case RaftConsensus::Entry::SNAPSHOT:
                    // 5. 该entry是snaoshot类型，说明本机的raft log已经无法提供单条的next apply entry log，
                    //    直接将整个state machine用snapshot内容替代。
                    NOTICE("Loading snapshot through entry %lu into state "
                           "machine", entry.index);
                    // JOEY_TODO: 看到这里
                    loadSnapshot(*entry.snapshotReader);
                    NOTICE("Done loading snapshot");
                    break;
            }
            // 6. state machine已经apply到了一个新的index，用对应的clusterTime对
            //    过期client session进行清理。
            expireSessions(entry.clusterTime);
            lastApplied = entry.index;
            // 7.有新的raft log被apply到了state machine，lastApplied已经被更新，
            //   唤醒所有在wait entriesApplied的rpc service.
            entriesApplied.notify_all();
            // 8. 判断当前是否适合进行打新snapshot的操作，然后唤醒snapshotThreadMain后台线程
            if (shouldTakeSnapshot(lastApplied) &&
                maySnapshotAt <= Clock::now()) {
                snapshotSuggested.notify_all();
            }
        }
    } catch (const Core::Util::ThreadInterruptedException&) {
        // 9. applyThreadMain线程在getNextEntry方法内获知了raft实例已经设置了exiting，直接throw到了这里，
        //    说明当前的Globals实例正在执行析构流程。
        NOTICE("exiting");
        std::lock_guard<Core::Mutex> lockGuard(mutex);
        // 10.在applyThreadMain线程退出之前，先设置state machine的exiting，然后通知所有state machine相关的后台线程进行退出。
        //    由于当Globals实例的析构流程走到这里的时候，所有service的worker线程都已经安全退出，所以这里主要是state machine内部创建的后台线程。
        exiting = true;
        entriesApplied.notify_all();
        snapshotSuggested.notify_all();
        snapshotStarted.notify_all();
        snapshotCompleted.notify_all();
        // 11. 如果snapshot生成子进程存在，就发送SIGTERM信号杀死。
        killSnapshotProcess(Core::HoldingMutex(lockGuard), SIGTERM);
    }
}

void
StateMachine::serializeSessions(SnapshotStateMachine::Header& header) const
{
    for (auto it = sessions.begin(); it != sessions.end(); ++it) {
        SnapshotStateMachine::Session& session = *header.add_session();
        session.set_client_id(it->first);
        session.set_last_modified(it->second.lastModified);
        session.set_first_outstanding_rpc(it->second.firstOutstandingRPC);
        for (auto it2 = it->second.responses.begin();
             it2 != it->second.responses.end();
             ++it2) {
            SnapshotStateMachine::Response& response =
                *session.add_rpc_response();
            response.set_rpc_number(it2->first);
            *response.mutable_response() = it2->second;
        }
    }
}

void
StateMachine::expireResponses(Session& session, uint64_t firstOutstandingRPC)
{
    if (session.firstOutstandingRPC >= firstOutstandingRPC)
        return;
    session.firstOutstandingRPC = firstOutstandingRPC;
    auto it = session.responses.begin();
    while (it != session.responses.end()) {
        if (it->first < session.firstOutstandingRPC)
            it = session.responses.erase(it);
        else
            ++it;
    }
}

// 在state machine成功apply/skip一条entry log后，需要已最新的entry clusterTime为基准
// 删除所有已过时的session，由于集群所有节点apply到同一个index时的clusterTime都相同，所以
// 过时的session也保持一致。
void
StateMachine::expireSessions(uint64_t clusterTime)
{
    auto it = sessions.begin();
    while (it != sessions.end()) {
        Session& session = it->second;
        uint64_t expireTime = session.lastModified + sessionTimeoutNanos;
        if (expireTime < clusterTime) {
            uint64_t diffNanos = clusterTime - session.lastModified;
            NOTICE("Expiring client %lu's session after %lu.%09lu seconds "
                   "of cluster time due to inactivity",
                   it->first,
                   diffNanos / (1000 * 1000 * 1000UL),
                   diffNanos % (1000 * 1000 * 1000UL));
            it = sessions.erase(it);
        } else {
            ++it;
        }
    }
}

uint16_t
StateMachine::getVersion(uint64_t logIndex) const
{
    auto it = versionHistory.upper_bound(logIndex);
    --it;
    return it->second;
}

// 父进程使用该方法向snapshot子进程发送SIGTERM信号直接将其杀死，由于子进程生成snapshot时
// 只会对state machine进行序列化和写临时partial文件，partial文件是否被rename成有效snapshot文件
// 由父进程决定，所以直接kill子进程只会损坏临时的partial文件，后续父进程再进行清理即可。
// ！！！
// 由于该方法并不是一个单独的原子方法，而是一个helper方法使得这块逻辑可被复用，所以不应该在内部单独加锁，
// 通过强制caller传递HoldingMutex参数的方式，让caller确保在调用该方法前已经获得了锁。
void
StateMachine::killSnapshotProcess(Core::HoldingMutex holdingMutex,
                                  int signum)
{
    if (childPid != 0) {
        int r = kill(childPid, signum);
        if (r != 0) {
            WARNING("Could not send %s to child process (%d): %s",
                    strsignal(signum),
                    childPid,
                    strerror(errno));
        }
    }
}

void
StateMachine::loadSessions(const SnapshotStateMachine::Header& header)
{
    sessions.clear();
    for (auto it = header.session().begin();
         it != header.session().end();
         ++it) {
        Session& session = sessions.insert({it->client_id(), {}})
                                                        .first->second;
        session.lastModified = it->last_modified();
        session.firstOutstandingRPC = it->first_outstanding_rpc();
        for (auto it2 = it->rpc_response().begin();
             it2 != it->rpc_response().end();
             ++it2) {
            session.responses.insert({it2->rpc_number(), it2->response()});
        }
    }
}

void
StateMachine::loadSnapshot(Core::ProtoBuf::InputStream& stream)
{
    // Check that this snapshot uses format version 1
    uint8_t formatVersion = 0;
    uint64_t bytesRead = stream.readRaw(&formatVersion, sizeof(formatVersion));
    if (bytesRead < sizeof(formatVersion)) {
        PANIC("Snapshot contents are empty (no format version field)");
    }
    if (formatVersion != 1) {
        PANIC("Snapshot contents format version read was %u, but this "
              "code can only read version 1",
              formatVersion);
    }

    // Load snapshot header
    {
        SnapshotStateMachine::Header header;
        std::string error = stream.readMessage(header);
        if (!error.empty()) {
            PANIC("Couldn't read state machine header from snapshot: %s",
                  error.c_str());
        }
        loadVersionHistory(header);
        loadSessions(header);
    }

    // Load the tree's state
    tree.loadSnapshot(stream);
}

void
StateMachine::loadVersionHistory(const SnapshotStateMachine::Header& header)
{
    versionHistory.clear();
    versionHistory.insert({0, 1});
    for (auto it = header.version_update().begin();
         it != header.version_update().end();
         ++it) {
        versionHistory.insert({it->log_index(),
                               Core::Util::downCast<uint16_t>(it->version())});
    }

    // The version of the current state machine behavior.
    uint16_t running = versionHistory.rbegin()->second;
    if (running < MIN_SUPPORTED_VERSION ||
        running > MAX_SUPPORTED_VERSION) {
        PANIC("State machine version read from snapshot was %u, but this "
              "code only supports %u through %u (inclusive)",
              running,
              MIN_SUPPORTED_VERSION,
              MAX_SUPPORTED_VERSION);
    }
}

void
StateMachine::serializeVersionHistory(
        SnapshotStateMachine::Header& header) const
{
    for (auto it = versionHistory.begin();
         it != versionHistory.end();
         ++it) {
        SnapshotStateMachine::VersionUpdate& update =
            *header.add_version_update();
        update.set_log_index(it->first);
        update.set_version(it->second);
    }
}

bool
StateMachine::shouldTakeSnapshot(uint64_t lastIncludedIndex) const
{
    SnapshotStats::SnapshotStats stats = consensus->getSnapshotStats();

    // print every 10% but not at 100% because then we'd be printing all the
    // time
    uint64_t curr = 0;
    if (lastIncludedIndex > stats.last_snapshot_index())
        curr = lastIncludedIndex - stats.last_snapshot_index();
    uint64_t prev = curr - 1;
    uint64_t logEntries = stats.last_log_index() - stats.last_snapshot_index();
    if (curr != logEntries &&
        10 * prev / logEntries != 10 * curr / logEntries) {
        NOTICE("Have applied %lu%% of the %lu total log entries",
               100 * curr / logEntries,
               logEntries);
    }

    if (stats.log_bytes() < snapshotMinLogSize)
        return false;
    if (stats.log_bytes() < stats.last_snapshot_bytes() * snapshotRatio)
        return false;
    if (lastIncludedIndex < stats.last_snapshot_index())
        return false;
    if (lastIncludedIndex < stats.last_log_index() * 3 / 4)
        return false;
    return true;
}

void
StateMachine::snapshotThreadMain()
{
    Core::ThreadId::setName("SnapshotStateMachine");
    std::unique_lock<Core::Mutex> lockGuard(mutex);
    bool wasInhibited = false;
    while (!exiting) {
        bool inhibited = (maySnapshotAt > Clock::now());

        TimePoint waitUntil = TimePoint::max();
        if (inhibited)
            waitUntil = maySnapshotAt;

        if (wasInhibited && !inhibited)
            NOTICE("Now permitted to take snapshots");
        wasInhibited = inhibited;

        if (isSnapshotRequested ||
            (!inhibited && shouldTakeSnapshot(lastApplied))) {

            isSnapshotRequested = false;
            takeSnapshot(lastApplied, lockGuard);
            continue;
        }

        snapshotSuggested.wait_until(lockGuard, waitUntil);
    }
}

void
StateMachine::snapshotWatchdogThreadMain()
{
    using Core::StringUtil::toString;
    Core::ThreadId::setName("SnapshotStateMachineWatchdog");
    std::unique_lock<Core::Mutex> lockGuard(mutex);

    // The snapshot process that this thread is currently tracking, based on
    // numSnapshotsAttempted. If set to ~0UL, this thread is not currently
    // tracking a snapshot process.
    uint64_t tracking = ~0UL;
    // The value of writer->sharedBytesWritten at the "start" time.
    uint64_t startProgress = 0;
    // The time at the "start" time.
    TimePoint startTime = TimePoint::min();
    // Special value for infinite interval.
    const std::chrono::nanoseconds zero = std::chrono::nanoseconds::zero();

    while (!exiting) {
        TimePoint waitUntil = TimePoint::max();
        TimePoint now = Clock::now();

        if (childPid > 0) { // there is some child process
            uint64_t currentProgress = *writer->sharedBytesWritten.value;
            if (tracking == numSnapshotsAttempted) { // tracking current child
                if (snapshotWatchdogInterval != zero &&
                    now >= startTime + snapshotWatchdogInterval) { // check
                    if (currentProgress == startProgress) {
                        ERROR("Snapshot process (counter %lu, pid %u) made no "
                              "progress for %s. Killing it. If this happens "
                              "at all often, you should file a bug to "
                              "understand the root cause.",
                              numSnapshotsAttempted,
                              childPid,
                              toString(snapshotWatchdogInterval).c_str());
                        killSnapshotProcess(Core::HoldingMutex(lockGuard),
                                            SIGKILL);
                        // Don't kill for another interval,
                        // hopefully child will be reaped by then.
                    }
                    startProgress = currentProgress;
                    startTime = now;
                } else {
                    // woke up too early, nothing to do
                }
            } else { // not yet tracking this child
                VERBOSE("Beginning to track snapshot process "
                        "(counter %lu, pid %u)",
                        numSnapshotsAttempted,
                        childPid);
                tracking = numSnapshotsAttempted;
                startProgress = currentProgress;
                startTime = now;
            }
            if (snapshotWatchdogInterval != zero)
                waitUntil = startTime + snapshotWatchdogInterval;
        } else { // no child process
            if (tracking != ~0UL) {
                VERBOSE("Snapshot ended: no longer tracking (counter %lu)",
                        tracking);
                tracking = ~0UL;
            }
        }
        snapshotStarted.wait_until(lockGuard, waitUntil);
    }
}


void
StateMachine::takeSnapshot(uint64_t lastIncludedIndex,
                           std::unique_lock<Core::Mutex>& lockGuard)
{
    // Open a snapshot file, then fork a child to write a consistent view of
    // the state machine to the snapshot file while this process continues
    // accepting requests.
    writer = consensus->beginSnapshot(lastIncludedIndex);
    // Flush the outstanding changes to the snapshot now so that they
    // aren't somehow double-flushed later.
    writer->flushToOS();

    ++numSnapshotsAttempted;
    snapshotStarted.notify_all();

    pid_t pid = fork();
    if (pid == -1) { // error
        PANIC("Couldn't fork: %s", strerror(errno));
    } else if (pid == 0) { // child
        Core::Debug::processName += "-child";
        globals.unblockAllSignals();
        usleep(stateMachineChildSleepMs * 1000); // for testing purposes
        if (snapshotBlockPercentage > 0) { // for testing purposes
            if (Core::Random::randomRange(0, 100) < snapshotBlockPercentage) {
                WARNING("Purposely deadlocking child (probability is %lu%%)",
                        snapshotBlockPercentage);
                std::mutex mutex;
                mutex.lock();
                mutex.lock(); // intentional deadlock
            }
        }

        // Format version of snapshot contents is 1.
        uint8_t formatVersion = 1;
        writer->writeRaw(&formatVersion, sizeof(formatVersion));
        // StateMachine state comes next
        {
            SnapshotStateMachine::Header header;
            serializeVersionHistory(header);
            serializeSessions(header);
            writer->writeMessage(header);
        }
        // Then the Tree itself (this one is potentially large)
        tree.dumpSnapshot(*writer);

        // Flush the changes to the snapshot file before exiting.
        writer->flushToOS();
        _exit(0);
    } else { // parent
        assert(childPid == 0);
        childPid = pid;
        int status = 0;
        {
            // release the lock while blocking on the child to allow
            // parallelism
            Core::MutexUnlock<Core::Mutex> unlockGuard(lockGuard);
            pid = waitpid(pid, &status, 0);
        }
        childPid = 0;
        if (pid == -1)
            PANIC("Couldn't waitpid: %s", strerror(errno));
        if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
            NOTICE("Child completed writing state machine contents to "
                   "snapshot staging file");
            writer->seekToEnd();
            consensus->snapshotDone(lastIncludedIndex, std::move(writer));
        } else if (exiting &&
                   WIFSIGNALED(status) && WTERMSIG(status) == SIGTERM) {
            writer->discard();
            writer.reset();
            NOTICE("Child exited from SIGTERM since this process is "
                   "exiting");
        } else {
            writer->discard();
            writer.reset();
            ++numSnapshotsFailed;
            ERROR("Snapshot creation failed with status %d. This server will "
                  "try again, but something might be terribly wrong. "
                  "%lu of %lu snapshots have failed in total.",
                  status,
                  numSnapshotsFailed,
                  numSnapshotsAttempted);
        }
        snapshotCompleted.notify_all();
    }
}

void
StateMachine::warnUnknownRequest(
        const google::protobuf::Message& request,
        const char* reason) const
{
    ++numUnknownRequests;
    TimePoint now = Clock::now();
    if (lastUnknownRequestMessage + unknownRequestMessageBackoff < now) {
        lastUnknownRequestMessage = now;
        if (numUnknownRequestsSinceLastMessage > 0) {
            WARNING("This version of the state machine (%u) %s "
                    "(and %lu similar warnings "
                    "were suppressed since the last message): %s",
                    getVersion(~0UL),
                    reason,
                    numUnknownRequestsSinceLastMessage,
                    Core::ProtoBuf::dumpString(request).c_str());
        } else {
            WARNING("This version of the state machine (%u) %s: %s",
                    getVersion(~0UL),
                    reason,
                    Core::ProtoBuf::dumpString(request).c_str());
        }
        numUnknownRequestsSinceLastMessage = 0;
    } else {
        ++numUnknownRequestsSinceLastMessage;
    }
}


} // namespace LogCabin::Server
} // namespace LogCabin

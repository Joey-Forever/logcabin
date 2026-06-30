/* Copyright (c) 2012 Stanford University
 * Copyright (c) 2015 Diego Ongaro
 * Copyright (c) 2015 Scale Computing
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

#include <algorithm>
#include <fcntl.h>
#include <limits>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "build/Protocol/Raft.pb.h"
#include "build/Server/SnapshotMetadata.pb.h"
#include "Core/Buffer.h"
#include "Core/Debug.h"
#include "Core/ProtoBuf.h"
#include "Core/Random.h"
#include "Core/StringUtil.h"
#include "Core/ThreadId.h"
#include "Core/Util.h"
#include "Protocol/Common.h"
#include "RPC/ClientRPC.h"
#include "RPC/ClientSession.h"
#include "RPC/ServerRPC.h"
#include "Server/RaftConsensus.h"
#include "Server/Globals.h"
#include "Storage/LogFactory.h"

namespace LogCabin {
namespace Server {

typedef Storage::Log Log;

namespace RaftConsensusInternal {

bool startThreads = true;

////////// Server //////////

Server::Server(uint64_t serverId)
    : serverId(serverId)
    , addresses()
    , haveStateMachineSupportedVersions(false)
    , minStateMachineVersion(std::numeric_limits<uint16_t>::max())
    , maxStateMachineVersion(0)
    , gcFlag(false)
{
}

Server::~Server()
{
}

std::ostream&
operator<<(std::ostream& os, const Server& server)
{
    return server.dumpToStream(os);
}

////////// LocalServer //////////

LocalServer::LocalServer(uint64_t serverId, RaftConsensus& consensus)
    : Server(serverId)
    , consensus(consensus)
    , lastSyncedIndex(0)
{
}

LocalServer::~LocalServer()
{
}

void
LocalServer::beginRequestVote()
{
}

void
LocalServer::beginLeadership()
{
    // raft保证Follower节点的所有append log在对外可知的情况下都已经是持久化了的，所以当一个Follower节点从Candidate成功成为Leader时，可以直接
    // 设置本机的lastSyncedIndex为last log index。
    lastSyncedIndex = consensus.log->getLastLogIndex();
}

void
LocalServer::exit()
{
}

uint64_t
LocalServer::getLastAckEpoch() const
{
    return consensus.currentEpoch;
}

uint64_t
LocalServer::getMatchIndex() const
{
    return lastSyncedIndex;
}

bool
LocalServer::haveVote() const
{
    return (consensus.votedFor == serverId);
}

void
LocalServer::interrupt()
{
}

bool
LocalServer::isCaughtUp() const
{
    return true;
}

void
LocalServer::scheduleHeartbeat()
{
}

std::ostream&
LocalServer::dumpToStream(std::ostream& os) const
{
    // Nothing interesting to dump.
    return os;
}

void
LocalServer::updatePeerStats(Protocol::ServerStats::Raft::Peer& peerStats,
                             Core::Time::SteadyTimeConverter& time) const
{
    switch (consensus.state) {
        case RaftConsensus::State::FOLLOWER:
            break;
        case RaftConsensus::State::CANDIDATE:
            break;
        case RaftConsensus::State::LEADER:
            peerStats.set_last_synced_index(lastSyncedIndex);
            break;
    }
}

////////// Peer //////////

Peer::Peer(uint64_t serverId, RaftConsensus& consensus)
    : Server(serverId)
    , consensus(consensus)
    , eventLoop(consensus.globals.eventLoop)
    , exiting(false)
    , requestVoteDone(false)
    , haveVote_(false)
    // 新加入configuration的Peer在实例构造时需要保证suppressBulkData、nextIndex、matchIndex三个值的设置和beginLeadership方法中的一致
    , suppressBulkData(true)
      // It's somewhat important to set nextIndex correctly here, since peers
      // that are added to the configuration won't go through beginLeadership()
      // on the current leader. I say somewhat important because, if nextIndex
      // is set incorrectly, it's self-correcting, so it's just a potential
      // performance issue.
    , nextIndex(consensus.log->getLastLogIndex() + 1)
    , matchIndex(0)
    , lastAckEpoch(0)
    , nextHeartbeatTime(TimePoint::min())
    , backoffUntil(TimePoint::min())
    , rpcFailuresSinceLastWarning(0)
    , lastCatchUpIterationMs(~0UL)
    , thisCatchUpIterationStart(Clock::now())
    , thisCatchUpIterationGoalId(~0UL)
    , isCaughtUp_(false)
    , snapshotFile()
    , snapshotFileOffset(0)
    , lastSnapshotIndex(0)
    , session()
    , rpc()
{
}

Peer::~Peer()
{
}

void
Peer::beginRequestVote()
{
    requestVoteDone = false;
    haveVote_ = false;
}

// 当一个server成为新Leader时就会调用已知所有Peer的该方法，主要设置三个关键值：
// 1. nextIndex设置到新Leader自身的last log index + 1，其实是在乐观猜测peer server的log同步状态，如果猜测不正确，
//    首次发送appendEnties时会fail然后会得到peer返回的其本地log真正的last log index用来纠正。
// 2. matchIndex设置为0，因为新leader并不知道peer节点的本地log match到哪里了，而且保证peerThreadMain后台线程马上就可以触发appendEnties。
// 3. suppressBulkData设置为true，保证首次发送的appendEntries是不携带任何log数据的heartBeat request。
void
Peer::beginLeadership()
{
    nextIndex = consensus.log->getLastLogIndex() + 1;
    matchIndex = 0;
    suppressBulkData = true;
    snapshotFile.reset();
    snapshotFileOffset = 0;
    lastSnapshotIndex = 0;
}

void
Peer::exit()
{
    NOTICE("Flagging peer %lu to exit", serverId);
    exiting = true;
    // Usually telling peers to exit is paired with an interruptAll(). That can
    // be error-prone, however, when you're removing servers from the
    // configuration (if the code removes servers and then calls
    // interruptAll(), it won't interrupt() the removed servers). So it's
    // better to just interrupt() here as well. See
    // https://github.com/logcabin/logcabin/issues/183
    interrupt();
}

uint64_t
Peer::getLastAckEpoch() const
{
    return lastAckEpoch;
}

uint64_t
Peer::getMatchIndex() const
{
    return matchIndex;
}

bool
Peer::haveVote() const
{
    return haveVote_;
}

void
Peer::interrupt()
{
    rpc.cancel();
}

bool
Peer::isCaughtUp() const
{
    return isCaughtUp_;
}

void
Peer::scheduleHeartbeat()
{
    nextHeartbeatTime = Clock::now();
}

Peer::CallStatus
Peer::callRPC(Protocol::Raft::OpCode opCode,
              const google::protobuf::Message& request,
              google::protobuf::Message& response,
              std::unique_lock<Mutex>& lockGuard)
{
    typedef RPC::ClientRPC::Status RPCStatus;
    rpc = RPC::ClientRPC(getSession(lockGuard),
                         Protocol::Common::ServiceId::RAFT_SERVICE,
                         /* serviceSpecificErrorVersion = */ 0,
                         opCode,
                         request);
    // release lock for concurrency
    Core::MutexUnlock<Mutex> unlockGuard(lockGuard);
    switch (rpc.waitForReply(&response, NULL, TimePoint::max())) {
        case RPCStatus::OK:
            if (rpcFailuresSinceLastWarning > 0) {
                WARNING("RPC to server succeeded after %lu failures",
                        rpcFailuresSinceLastWarning);
                rpcFailuresSinceLastWarning = 0;
            }
            return CallStatus::OK;
        case RPCStatus::SERVICE_SPECIFIC_ERROR:
            PANIC("unexpected service-specific error");
        case RPCStatus::TIMEOUT:
            PANIC("unexpected RPC timeout");
        case RPCStatus::RPC_FAILED:
            ++rpcFailuresSinceLastWarning;
            if (rpcFailuresSinceLastWarning == 1) {
                WARNING("RPC to server failed: %s",
                        rpc.getErrorMessage().c_str());
            } else if (rpcFailuresSinceLastWarning % 100 == 0) {
                WARNING("Last %lu RPCs to server failed. This failure: %s",
                        rpcFailuresSinceLastWarning,
                        rpc.getErrorMessage().c_str());
            }
            return CallStatus::FAILED;
        case RPCStatus::RPC_CANCELED:
            return CallStatus::FAILED;
        case RPCStatus::INVALID_SERVICE:
            PANIC("The server isn't running the RaftService");
        case RPCStatus::INVALID_REQUEST:
            return CallStatus::INVALID_REQUEST;
    }
    PANIC("Unexpected RPC status");
}

void
Peer::startThread(std::shared_ptr<Peer> self)
{
    thisCatchUpIterationStart = Clock::now();
    thisCatchUpIterationGoalId = consensus.log->getLastLogIndex();
    ++consensus.numPeerThreads;
    NOTICE("Starting peer thread for server %lu", serverId);
    std::thread(&RaftConsensus::peerThreadMain, &consensus, self).detach();
}

std::shared_ptr<RPC::ClientSession>
Peer::getSession(std::unique_lock<Mutex>& lockGuard)
{
    if (!session || !session->getErrorMessage().empty()) {
        // Unfortunately, creating a session isn't currently interruptible, so
        // we use a timeout to prevent the server from hanging forever if some
        // peer thread happens to be creating a session when it's told to exit.
        // See https://github.com/logcabin/logcabin/issues/183 for more detail.
        TimePoint timeout = Clock::now() + consensus.ELECTION_TIMEOUT;
        // release lock for concurrency
        Core::MutexUnlock<Mutex> unlockGuard(lockGuard);
        RPC::Address target(addresses, Protocol::Common::DEFAULT_PORT);
        target.refresh(timeout);
        Client::SessionManager::ServerId peerId(serverId);
        session = consensus.sessionManager.createSession(
            target,
            timeout,
            &consensus.globals.clusterUUID,
            &peerId);
    }
    return session;
}

std::ostream&
Peer::dumpToStream(std::ostream& os) const
{
    os << "Peer " << serverId << std::endl;
    os << "addresses: " << addresses << std::endl;
    switch (consensus.state) {
        case RaftConsensus::State::FOLLOWER:
            break;
        case RaftConsensus::State::CANDIDATE:
            os << "vote: ";
            if (requestVoteDone) {
                if (haveVote_)
                    os << "granted";
                else
                    os << "not granted";
            } else {
                os << "no response";
            }
            os << std::endl;
            break;
        case RaftConsensus::State::LEADER:
            os << "suppressBulkData: " << suppressBulkData << std::endl;
            os << "nextIndex: " << nextIndex << std::endl;
            os << "matchIndex: " << matchIndex << std::endl;
            break;
    }
    return os;
}

void
Peer::updatePeerStats(Protocol::ServerStats::Raft::Peer& peerStats,
                      Core::Time::SteadyTimeConverter& time) const
{
    switch (consensus.state) {
        case RaftConsensus::State::FOLLOWER:
            break;
        case RaftConsensus::State::CANDIDATE:
            break;
        case RaftConsensus::State::LEADER:
            peerStats.set_suppress_bulk_data(suppressBulkData);
            peerStats.set_next_index(nextIndex);
            peerStats.set_last_agree_index(matchIndex);
            peerStats.set_is_caught_up(isCaughtUp_);
            peerStats.set_next_heartbeat_at(time.unixNanos(nextHeartbeatTime));
            break;
    }

    switch (consensus.state) {
        case RaftConsensus::State::FOLLOWER:
            break;
        case RaftConsensus::State::CANDIDATE: // fallthrough
        case RaftConsensus::State::LEADER:
            peerStats.set_request_vote_done(requestVoteDone);
            peerStats.set_have_vote(haveVote_);
            peerStats.set_backoff_until(time.unixNanos(backoffUntil));
            break;
    }
}

////////// Configuration::SimpleConfiguration //////////

Configuration::SimpleConfiguration::SimpleConfiguration()
    : servers()
{
}

Configuration::SimpleConfiguration::~SimpleConfiguration()
{
}

bool
Configuration::SimpleConfiguration::all(const Predicate& predicate) const
{
    for (auto it = servers.begin(); it != servers.end(); ++it) {
        if (!predicate(**it))
            return false;
    }
    return true;
}

bool
Configuration::SimpleConfiguration::contains(std::shared_ptr<Server> server)
                                                                          const
{
    for (auto it = servers.begin(); it != servers.end(); ++it) {
        if (*it == server)
            return true;
    }
    return false;
}

void
Configuration::SimpleConfiguration::forEach(const SideEffect& sideEffect)
{
    for (auto it = servers.begin(); it != servers.end(); ++it)
        sideEffect(**it);
}

uint64_t
Configuration::SimpleConfiguration::min(const GetValue& getValue) const
{
    if (servers.empty())
        return 0;
    uint64_t smallest = ~0UL;
    for (auto it = servers.begin(); it != servers.end(); ++it)
        smallest = std::min(smallest, getValue(**it));
    return smallest;
}

// 只有当多数派都是true时才返回true
bool
Configuration::SimpleConfiguration::quorumAll(const Predicate& predicate) const
{
    if (servers.empty())
        return true;
    uint64_t count = 0;
    for (auto it = servers.begin(); it != servers.end(); ++it)
        if (predicate(**it))
            ++count;
    return (count >= servers.size() / 2 + 1);
}

// 返回value数值最大的多数派中最小的那个值
uint64_t
Configuration::SimpleConfiguration::quorumMin(const GetValue& getValue) const
{
    if (servers.empty())
        return 0;
    std::vector<uint64_t> values;
    for (auto it = servers.begin(); it != servers.end(); ++it)
        values.push_back(getValue(**it));
    std::sort(values.begin(), values.end());
    return values.at((values.size() - 1)/ 2);
}

////////// Configuration //////////

Configuration::Configuration(uint64_t serverId, RaftConsensus& consensus)
    : consensus(consensus)
    , knownServers()
    , localServer()
    , state(State::BLANK)
    , id(0)
    , description()
    , oldServers()
    , newServers()
{
    localServer.reset(new LocalServer(serverId, consensus));
    // 还没从raft log中读取任何configuration，初始KnownServers只有自己，即LocalServer
    knownServers[serverId] = localServer;
}

Configuration::~Configuration()
{
}

void
Configuration::forEach(const SideEffect& sideEffect)
{
    for (auto it = knownServers.begin(); it != knownServers.end(); ++it)
        sideEffect(*it->second);
}

bool
Configuration::hasVote(std::shared_ptr<Server> server) const
{
    if (state == State::TRANSITIONAL) {
        return (oldServers.contains(server) ||
                newServers.contains(server));
    } else {
        return oldServers.contains(server);
    }
}

std::string
Configuration::lookupAddress(uint64_t serverId) const
{
    auto it = knownServers.find(serverId);
    if (it != knownServers.end())
        return it->second->addresses;
    return "";
}

bool
Configuration::quorumAll(const Predicate& predicate) const
{
    if (state == State::TRANSITIONAL) {
        return (oldServers.quorumAll(predicate) &&
                newServers.quorumAll(predicate));
    } else {
        return oldServers.quorumAll(predicate);
    }
}

uint64_t
Configuration::quorumMin(const GetValue& getValue) const
{
    if (state == State::TRANSITIONAL) {
        return std::min(oldServers.quorumMin(getValue),
                        newServers.quorumMin(getValue));
    } else {
        return oldServers.quorumMin(getValue);
    }
}

void
Configuration::resetStagingServers()
{
    if (state == State::STAGING) {
        // staging servers could have changed other servers' addresses, so roll
        // back to old description with old addresses
        // staging state下的configuration的id和description仍然保持此前的stable状态，这里直接可以回退
        setConfiguration(id, description);
    }
}

namespace {
void setGCFlag(Server& server)
{
    server.gcFlag = true;
}
} // anonymous namespace

void
Configuration::reset()
{
    NOTICE("Resetting to blank configuration");
    state = State::BLANK;
    id = 0;
    description = {};
    oldServers.servers.clear();
    newServers.servers.clear();
    for (auto it = knownServers.begin(); it != knownServers.end(); ++it)
        it->second->exit();
    knownServers.clear();
    knownServers[localServer->serverId] = localServer;
}

// 将一个从raft log中读取到的new configuration（newId对应raft log中的entry id）设置为本地已知的集群membership config：
//  1. 将new configuration中涉及到的所有server加入KnownServers列表，并分别启动一个peerThreadMain线程
//  2. 为KnownServers列表中不存在于new configuration中的peerThreadMain线程执行exit
void
Configuration::setConfiguration(
        uint64_t newId,
        const Protocol::Raft::Configuration& newDescription)
{
    NOTICE("Activating configuration %lu:\n%s", newId,
           Core::ProtoBuf::dumpString(newDescription).c_str());

    if (newDescription.next_configuration().servers().size() == 0)
        state = State::STABLE;
    else
        state = State::TRANSITIONAL;
    id = newId;
    description = newDescription;
    oldServers.servers.clear();
    newServers.servers.clear();

    // Build up the list of old servers
    for (auto confIt = description.prev_configuration().servers().begin();
         confIt != description.prev_configuration().servers().end();
         ++confIt) {
        std::shared_ptr<Server> server = getServer(confIt->server_id());
        server->addresses = confIt->addresses();
        oldServers.servers.push_back(server);
    }

    // Build up the list of new servers
    for (auto confIt = description.next_configuration().servers().begin();
         confIt != description.next_configuration().servers().end();
         ++confIt) {
        std::shared_ptr<Server> server = getServer(confIt->server_id());
        server->addresses = confIt->addresses();
        newServers.servers.push_back(server);
    }

    // Servers not in the current configuration need to be told to exit
    setGCFlag(*localServer);
    oldServers.forEach(setGCFlag);
    newServers.forEach(setGCFlag);
    auto it = knownServers.begin();
    while (it != knownServers.end()) {
        std::shared_ptr<Server> server = it->second;
        if (!server->gcFlag) {
            server->exit();
            it = knownServers.erase(it);
        } else {
            server->gcFlag = false; // clear flag for next time
            ++it;
        }
    }
}

// leader节点将本地的stable configuration设置成staging state，该方法只会将configuration实例的
// 的new_servers列表进行填充为传入servers，不会修改id和description，new_servers后续也暂时不会参与quorum。
void
Configuration::setStagingServers(
        const Protocol::Raft::SimpleConfiguration& stagingServers)
{
    // configuration此前的state必须得是stable
    assert(state == State::STABLE);
    state = State::STAGING;
    for (auto it = stagingServers.servers().begin();
         it != stagingServers.servers().end();
         ++it) {
        // getServer方法会为每个新来的server节点新建一个peer线程，用于后续与该server节点的各类网络通信活动。
        std::shared_ptr<Server> server = getServer(it->server_id());
        server->addresses = it->addresses();
        // 此前stable state的newServers列表为空，这里会将传入的stagingServers列表填充进去。
        newServers.servers.push_back(server);
    }
}

bool
Configuration::stagingAll(const Predicate& predicate) const
{
    if (state == State::STAGING)
        return newServers.all(predicate);
    else
        return true;
}

uint64_t
Configuration::stagingMin(const GetValue& getValue) const
{
    if (state == State::STAGING)
        return newServers.min(getValue);
    else
        return 0;
}

void
Configuration::updateServerStats(Protocol::ServerStats& serverStats,
                                 Core::Time::SteadyTimeConverter& time) const
{
    for (auto it = knownServers.begin();
         it != knownServers.end();
         ++it) {
        Protocol::ServerStats::Raft::Peer& peerStats =
            *serverStats.mutable_raft()->add_peer();
        peerStats.set_server_id(it->first);
        const ServerRef& peer = it->second;
        peerStats.set_addresses(peer->addresses);
        peerStats.set_old_member(oldServers.contains(peer));
        peerStats.set_new_member(state == State::TRANSITIONAL &&
                                 newServers.contains(peer));
        peerStats.set_staging_member(state == State::STAGING &&
                                     newServers.contains(peer));
        peer->updatePeerStats(peerStats, time);
    }
}

std::ostream&
operator<<(std::ostream& os, Configuration::State state)
{
    typedef Configuration::State State;
    switch (state) {
        case State::BLANK:
            os << "State::BLANK";
            break;
        case State::STABLE:
            os << "State::STABLE";
            break;
        case State::STAGING:
            os << "State::STAGING";
            break;
        case State::TRANSITIONAL:
            os << "State::TRANSITIONAL";
            break;
    }
    return os;
}

std::ostream&
operator<<(std::ostream& os, const Configuration& configuration)
{
    os << "Configuration: {" << std::endl;
    os << "  state: " << configuration.state << std::endl;
    os << "  id: " << configuration.id << std::endl;
    os << "  description: " << std::endl;
    os << Core::ProtoBuf::dumpString(configuration.description);
    os << "}" << std::endl;
    for (auto it = configuration.knownServers.begin();
         it != configuration.knownServers.end();
         ++it) {
        os << *it->second;
    }
    return os;
}


////////// Configuration private methods //////////

// 从config中每读取一个knownServers中没有的peer server节点，就会为该peer server启动一个peerThreadMain。
std::shared_ptr<Server>
Configuration::getServer(uint64_t newServerId)
{
    auto it = knownServers.find(newServerId);
    if (it != knownServers.end()) {
        return it->second;
    } else {
        std::shared_ptr<Peer> peer(new Peer(newServerId, consensus));
        if (startThreads)
            peer->startThread(peer);
        knownServers[newServerId] = peer;
        return peer;
    }
}

////////// ConfigurationManager //////////

ConfigurationManager::ConfigurationManager(Configuration& configuration)
    : configuration(configuration)
    , descriptions()
    , snapshot(0, {})
{
}

ConfigurationManager::~ConfigurationManager()
{
}

void
ConfigurationManager::add(
    uint64_t index,
    const Protocol::Raft::Configuration& description)
{
    descriptions[index] = description;
    restoreInvariants();
}

void
ConfigurationManager::truncatePrefix(uint64_t firstIndexKept)
{
    descriptions.erase(descriptions.begin(),
                       descriptions.lower_bound(firstIndexKept));
    restoreInvariants();
}

void
ConfigurationManager::truncateSuffix(uint64_t lastIndexKept)
{
    descriptions.erase(descriptions.upper_bound(lastIndexKept),
                       descriptions.end());
    restoreInvariants();
}

void
ConfigurationManager::setSnapshot(
    uint64_t index,
    const Protocol::Raft::Configuration& description)
{
    assert(index >= snapshot.first);
    snapshot = {index, description};
    restoreInvariants();
}

std::pair<uint64_t, Protocol::Raft::Configuration>
ConfigurationManager::getLatestConfigurationAsOf(
                                        uint64_t lastIncludedIndex) const
{
    if (descriptions.empty())
        return {0, {}};
    auto it = descriptions.upper_bound(lastIncludedIndex);
    // 'it' is either an element or end()
    if (it == descriptions.begin())
        return {0, {}};
    --it;
    return *it;
}

////////// ConfigurationManager private methods //////////

void
ConfigurationManager::restoreInvariants()
{
    if (snapshot.first != 0)
        descriptions.insert(snapshot);
    if (descriptions.empty()) {
        configuration.reset();
    } else {
        auto it = descriptions.rbegin();
        if (configuration.id != it->first)
            configuration.setConfiguration(it->first, it->second);
    }
}

////////// ClusterClock //////////

ClusterClock::ClusterClock()
    : clusterTimeAtEpoch(0)
    , localTimeAtEpoch(Core::Time::SteadyClock::now())
{
}

void
ClusterClock::newEpoch(uint64_t clusterTime)
{
    clusterTimeAtEpoch = clusterTime;
    localTimeAtEpoch = Core::Time::SteadyClock::now();
}

uint64_t
ClusterClock::leaderStamp()
{
    auto localTime = Core::Time::SteadyClock::now();
    uint64_t nanosSinceEpoch =
        Core::Util::downCast<uint64_t>(std::chrono::nanoseconds(
            localTime - localTimeAtEpoch).count());
    clusterTimeAtEpoch += nanosSinceEpoch;
    localTimeAtEpoch = localTime;
    return clusterTimeAtEpoch;
}

uint64_t
ClusterClock::interpolate() const
{
    auto localTime = Core::Time::SteadyClock::now();
    uint64_t nanosSinceEpoch =
        Core::Util::downCast<uint64_t>(std::chrono::nanoseconds(
            localTime - localTimeAtEpoch).count());
    return clusterTimeAtEpoch + nanosSinceEpoch;
}

namespace {

struct StagingProgressing {
    StagingProgressing(uint64_t epoch,
                       Protocol::Client::SetConfiguration::Response& response)
        : epoch(epoch)
        , response(response)
    {
    }
    bool operator()(Server& server) {
        uint64_t serverEpoch = server.getLastAckEpoch();
        if (serverEpoch < epoch) {
            auto& s = *response.mutable_configuration_bad()->add_bad_servers();
            s.set_server_id(server.serverId);
            s.set_addresses(server.addresses);
            return false;
        }
        return true;
    }
    const uint64_t epoch;
    Protocol::Client::SetConfiguration::Response& response;
};

struct StateMachineVersionIntersection {
    StateMachineVersionIntersection()
        : missingCount(0)
        , allCount(0)
        , minVersion(0)
        , maxVersion(std::numeric_limits<uint16_t>::max()) {
    }
    void operator()(Server& server) {
        ++allCount;
        if (server.haveStateMachineSupportedVersions) {
            minVersion = std::max(server.minStateMachineVersion,
                                  minVersion);
            maxVersion = std::min(server.maxStateMachineVersion,
                                  maxVersion);
        } else {
            ++missingCount;
        }
    }
    uint64_t missingCount;
    uint64_t allCount;
    uint16_t minVersion;
    uint16_t maxVersion;
};

} // anonymous namespace

} // namespace RaftConsensusInternal

////////// RaftConsensus::Entry //////////

RaftConsensus::Entry::Entry()
    : index(0)
    , type(SKIP)
    , command()
    , snapshotReader()
    , clusterTime(0)
{
}

RaftConsensus::Entry::Entry(Entry&& other)
    : index(other.index)
    , type(other.type)
    , command(std::move(other.command))
    , snapshotReader(std::move(other.snapshotReader))
    , clusterTime(other.clusterTime)
{
}

RaftConsensus::Entry::~Entry()
{
}

////////// RaftConsensus //////////

// JOEY_TODO: 做multi-raft，做batching/pipelining
RaftConsensus::RaftConsensus(Globals& globals)
    : ELECTION_TIMEOUT(
        std::chrono::milliseconds(
            globals.config.read<uint64_t>(
                "electionTimeoutMilliseconds",
                500)))
    , HEARTBEAT_PERIOD(
        globals.config.keyExists("heartbeatPeriodMilliseconds")
            ? std::chrono::nanoseconds(
                std::chrono::milliseconds(
                    globals.config.read<uint64_t>(
                        "heartbeatPeriodMilliseconds")))
            : ELECTION_TIMEOUT / 2)
    , MAX_LOG_ENTRIES_PER_REQUEST(
        globals.config.read<uint64_t>(
            "maxLogEntriesPerRequest",
            5000))
    , RPC_FAILURE_BACKOFF(
        globals.config.keyExists("rpcFailureBackoffMilliseconds")
            ? std::chrono::nanoseconds(
                std::chrono::milliseconds(
                    globals.config.read<uint64_t>(
                        "rpcFailureBackoffMilliseconds")))
            : (ELECTION_TIMEOUT / 2))
    , STATE_MACHINE_UPDATER_BACKOFF(
        std::chrono::milliseconds(
            globals.config.read<uint64_t>(
                "stateMachineUpdaterBackoffMilliseconds",
                10000)))
    , SOFT_RPC_SIZE_LIMIT(Protocol::Common::MAX_MESSAGE_LENGTH - 1024)
    , serverId(0)
    , serverAddresses()
    , globals(globals)
    , storageLayout()
    , sessionManager(globals.eventLoop,
                     globals.config)
    , mutex()
    , stateChanged()
    , exiting(false)
    , numPeerThreads(0)
    , log()
    , logSyncQueued(false)
    , leaderDiskThreadWorking(false)
    , configuration()
    , configurationManager()
    , currentTerm(0)
    , state(State::FOLLOWER)
    , lastSnapshotIndex(0)
    , lastSnapshotTerm(0)
    , lastSnapshotClusterTime(0)
    , lastSnapshotBytes(0)
    , snapshotReader()
    , snapshotWriter()
    , commitIndex(0)
    , leaderId(0)
    , votedFor(0)
    , currentEpoch(0)
    , clusterClock()
    , startElectionAt(TimePoint::max())
    , withholdVotesUntil(TimePoint::min())
    , numEntriesTruncated(0)
    , leaderDiskThread()
    , timerThread()
    , stateMachineUpdaterThread()
    , stepDownThread()
    , invariants(*this)
{
}

// StateMachine实例析构之后，会引起raft实例引用减一，最终导致raft实例析构
RaftConsensus::~RaftConsensus()
{
    // 理论上，在StateMachine实例析构时已经触发过了raft实例的exit
    if (!exiting)
        exit();
    // raft exiting已经设置，无锁等待所有raft持有的后台线程退出
    if (leaderDiskThread.joinable())
        leaderDiskThread.join();
    if (timerThread.joinable())
        timerThread.join();
    if (stateMachineUpdaterThread.joinable())
        stateMachineUpdaterThread.join();
    if (stepDownThread.joinable())
        stepDownThread.join();
    NOTICE("Joined with disk and timer threads");
    std::unique_lock<Mutex> lockGuard(mutex);
    // while循环等待peer线程数减为0，由于需要访问numPeerThreads，所以需要持锁
    if (numPeerThreads > 0) {
        NOTICE("Waiting for %u peer threads to exit", numPeerThreads);
        while (numPeerThreads > 0)
            // 放锁sleep
            stateChanged.wait(lockGuard);
    }
    NOTICE("Peer threads have exited");
    // issue any outstanding disk flushes
    // leaderDiskThread线程退出后，可能还有文件操作未来得及执行，在raft实例析构之前，需要先完成剩余的文件操作，
    // follower节点的文件操作都是持raft mutex同步执行的，只有leader节点可能存在未来得及执行。
    if (logSyncQueued) {
        std::unique_ptr<Log::Sync> sync = log->takeSync();
        sync->wait();
        log->syncComplete(std::move(sync));
    }
    NOTICE("Completed disk writes");
}

// RaftConsensus实例初始化，主要做这几件事：
// 1. 从磁盘中恢复raft log到内存中，包括term、voteFor、logStartIndex这些metatdata
// 2. 将raft log中的历史configuration entry逐一存储到descriptions然后进行覆盖式激活，
//    激活时会为对应的membership servers创建独立的peer线程
// 3. 解析磁盘中最新的snapshot header，以该snapshot为基准对raft log进行截断甚至全部抛弃、
//    用snapshot configuration对本地配置进行覆盖式激活，保存snapshotReader提供给后续state machine的apply线程进行内容读取
// 4. 将本机stepDown为currentTerm Follower，包括设置选举计时器，超时发起新选举
// 5. 创建并启动一些必要的后台线程
void
RaftConsensus::init()
{
    std::lock_guard<Mutex> lockGuard(mutex);
#if DEBUG
    if (globals.config.read<bool>("raftDebug", false)) {
        mutex.callback = std::bind(&Invariants::checkAll, &invariants);
    }
#endif

    NOTICE("My server ID is %lu", serverId);

    if (storageLayout.topDir.fd == -1) {
        if (globals.config.read("use-temporary-storage", false))
            storageLayout.initTemporary(serverId); // unit tests
        else
            storageLayout.init(globals.config, serverId);
    }

    configuration.reset(new Configuration(serverId, *this));
    configurationManager.reset(new ConfigurationManager(*configuration));

    NOTICE("Reading the log");
    if (!log) { // some unit tests pre-set the log; don't overwrite it
        // 1. 构造SegmentLog实例，触发raft log的启动恢复，解析本机磁盘中的metadata文件元数据（主要是logStartIndex、term、voted_for）
        //    以及segment文件的所有log entry到内存结构中
        log = Storage::LogFactory::makeLog(globals.config, storageLayout);
    }
    for (uint64_t index = log->getLogStartIndex();
         index <= log->getLastLogIndex();
         ++index) {
        const Log::Entry& entry = log->getEntry(index);
        if (entry.type() == Protocol::Raft::EntryType::UNKNOWN) {
            PANIC("Don't understand the entry type for index %lu (term %lu) "
                  "found on disk",
                  index, entry.term());
        }
        // 2. 遍历segment log中的所有entry，挑出其中的所有configuration类型的entry，保存到ConfigurationManager中，然后不断将本地configuration设置到Index最新的那个
        if (entry.type() == Protocol::Raft::EntryType::CONFIGURATION) {
            configurationManager->add(index, entry.configuration());
        }
    }

    // Restore cluster time epoch from last log entry, if any
    if (log->getLastLogIndex() >= log->getLogStartIndex()) {
        // 3. 根据恢复出来的raft log，将最后的log entry的cluster time更新clusterClock
        clusterClock.newEpoch(
            log->getEntry(log->getLastLogIndex()).cluster_time());
    }

    NOTICE("The log contains indexes %lu through %lu (inclusive)",
           log->getLogStartIndex(), log->getLastLogIndex());

    // 4. 根据恢复出来的metadata信息，将term和voted_for更新到内存状态，然后再持久化一次metadata文件
    if (log->metadata.has_current_term())
        currentTerm = log->metadata.current_term();
    if (log->metadata.has_voted_for())
        votedFor = log->metadata.voted_for();
    updateLogMetadata();

    // Read snapshot after reading log, since readSnapshot() will get rid of
    // conflicting log entries
    // 5. 解析本机最新的snapshot header，该操作会以snapshot作为基准，可能会
    //    对raft log进行截断、更新logStartIndex、更新commitedIndex、更新clusterClock、更新内存configuration信息等等。
    readSnapshot();

    // Clean up incomplete snapshots left by prior runs. This could be done
    // earlier, but maybe it's nicer to make sure we can get to this point
    // without PANICing before deleting these files.
    // 6. 清除所有没有完整生成的partial snapshot文件
    Storage::SnapshotFile::discardPartialSnapshots(storageLayout);

    if (configuration->id == 0)
        // 7. 本机经过恢复raft log和snapshot步骤之后，连configuration都没有，说明本机
        //    可能是个新加入集群的空server节点，需要等待leader节点后续走配置变更流程复制过来。
        NOTICE("No configuration, waiting to receive one.");

    // 8. 节点重启后，无条件stepDown为metadata中记录的currentTerm，转为Follower，选举超时也会在这里重置，
    //    保证后续如果收不到leader心跳可以主动发起选举。
    stepDown(currentTerm);
    // 9. 创建并启动一些必须的后台线程
    if (RaftConsensusInternal::startThreads) {
        // 1）用于Leader节点异步执行文件操作
        leaderDiskThread = std::thread(
            &RaftConsensus::leaderDiskThreadMain, this);
        // 2）用于Follower/Candidate节点超时发起新选举
        timerThread = std::thread(
            &RaftConsensus::timerThreadMain, this);
        // 3) 用于Leader节点定时检查集群所有节点共同支持的最新state machine版本然后对runningVersion进行升级
        if (globals.config.read<bool>("disableStateMachineUpdates", false)) {
            NOTICE("Not starting state machine updater thread (state machine "
                   "updates are disabled in config)");
        } else {
            stateMachineUpdaterThread = std::thread(
                &RaftConsensus::stateMachineUpdaterThreadMain, this);
        }
        // 4）用于Leader节点在向多数派心跳quorum得不到ok时主动stepDown
        stepDownThread = std::thread(
            &RaftConsensus::stepDownThreadMain, this);
    }
    // log->path = ""; // hack to disable disk
    // 10. RaftConsensus实例初始化结束了，唤醒一下等待stateChanged的线程
    stateChanged.notify_all();
    printElectionState();
}

void
RaftConsensus::exit()
{
    NOTICE("Shutting down");
    std::lock_guard<Mutex> lockGuard(mutex);
    exiting = true;
    if (configuration)
        configuration->forEach(&Server::exit);
    interruptAll();
}

void
RaftConsensus::bootstrapConfiguration()
{
    std::lock_guard<Mutex> lockGuard(mutex);

    if (currentTerm != 0 ||
        log->getLogStartIndex() != 1 ||
        log->getLastLogIndex() != 0 ||
        lastSnapshotIndex != 0) {
        PANIC("Refusing to bootstrap configuration: it looks like a log or "
              "snapshot already exists.");
    }
    stepDown(1); // satisfies invariants assertions

    // Append the configuration entry to the log
    Log::Entry entry;
    entry.set_term(1);
    entry.set_type(Protocol::Raft::EntryType::CONFIGURATION);
    entry.set_cluster_time(0);
    Protocol::Raft::Configuration& configuration =
        *entry.mutable_configuration();
    Protocol::Raft::Server& server =
        *configuration.mutable_prev_configuration()->add_servers();
    server.set_server_id(serverId);
    server.set_addresses(serverAddresses);
    append({&entry});
}

RaftConsensus::ClientResult
RaftConsensus::getConfiguration(
        Protocol::Raft::SimpleConfiguration& currentConfiguration,
        uint64_t& id) const
{
    std::unique_lock<Mutex> lockGuard(mutex);
    if (!upToDateLeader(lockGuard))
        return ClientResult::NOT_LEADER;
    // 只有当当前leader本地configuration已经stable并且已commit，才会返回id号给client
    if (configuration->state != Configuration::State::STABLE ||
        commitIndex < configuration->id) {
        return ClientResult::RETRY;
    }
    currentConfiguration = configuration->description.prev_configuration();
    id = configuration->id;
    return ClientResult::SUCCESS;
}

std::pair<RaftConsensus::ClientResult, uint64_t>
RaftConsensus::getLastCommitIndex() const
{
    std::unique_lock<Mutex> lockGuard(mutex);
    if (!upToDateLeader(lockGuard))
        return {ClientResult::NOT_LEADER, 0};
    else
        return {ClientResult::SUCCESS, commitIndex};
}

std::string
RaftConsensus::getLeaderHint() const
{
    std::lock_guard<Mutex> lockGuard(mutex);
    return configuration->lookupAddress(leaderId);
}

// 该方法用于state machine的applyThreadMain线程从raft log中获取下一条用于apply到state machine的entry，
// 只有当nextIndex entry能够被成功获取时该函数才会返回上层，否则applyThreadMain线程会sleep在RaftConsensus stateChanged上。
RaftConsensus::Entry
RaftConsensus::getNextEntry(uint64_t lastIndex) const
{
    std::unique_lock<Mutex> lockGuard(mutex);
    // 1. nextIndex是applyThreadMain线程下一步希望进行apply的entry log
    uint64_t nextIndex = lastIndex + 1;
    while (true) {
        if (exiting)
            // 2. RaftConsensus实例exiting了，直接throw回上层，引发state machine的exit
            throw Core::Util::ThreadInterruptedException();
        if (commitIndex >= nextIndex) {
            // 3.下一个希望apply的index entry log已经commit了，可以被apply，
            //   需要构造个entry返回给上层state machine执行
            RaftConsensus::Entry entry;

            // Make the state machine load a snapshot if we don't have the next
            // entry it needs in the log.
            if (log->getLogStartIndex() > nextIndex) {
                // 4. raft log中已经不包括nextIndex了，只能直接apply snapshot了，可能是因为：
                //    1）该节点当前处于崩溃恢复阶段，需要从index 1开始apply state machine
                //    2）此前从leader处installSnapshot时本地raft log被截断了
                entry.type = Entry::SNAPSHOT;
                // For well-behaved state machines, we expect 'snapshotReader'
                // to contain a SnapshotFile::Reader that we can return
                // directly to the state machine. In the case that a State
                // Machine asks for the snapshot again, we have to build a new
                // SnapshotFile::Reader again.
                // 5. 直接将上次readSnapshot时保存的snapshotReader移动给entry，该snapshotReader是一次性使用。
                entry.snapshotReader = std::move(snapshotReader);
                if (!entry.snapshotReader) {
                    // 6. snapshotReader不存在了，可能是之前已经被消费了，需要调用readSnapshot重新打开一个snapshotReader
                    WARNING("State machine asked for same snapshot twice; "
                            "this shouldn't happen in normal operation. "
                            "Having to re-read it from disk.");
                    // readSnapshot() shouldn't have any side effects since the
                    // snapshot should have already been read, so const_cast
                    // should be ok (though ugly).
                    // 因为getNextEntry是const方法，所以这里必须把this强转成非const this才能调用
                    const_cast<RaftConsensus*>(this)->readSnapshot();
                    entry.snapshotReader = std::move(snapshotReader);
                }
                entry.index = lastSnapshotIndex;
                entry.clusterTime = lastSnapshotClusterTime;
            } else {
                // not a snapshot
                // 7. nextIndex还在raft log中，直接从raft log中获取目标log entry
                const Log::Entry& logEntry = log->getEntry(nextIndex);
                entry.index = nextIndex;
                if (logEntry.type() == Protocol::Raft::EntryType::DATA) {
                    // 8. 将DATA类型的entry log的data拷贝到entry.command中，因为后续的实际apply是不持有RaftConsensus mutex的，
                    //    防止直接引用失效。
                    entry.type = Entry::DATA;
                    const std::string& s = logEntry.data();
                    entry.command = Core::Buffer(
                        memcpy(new char[s.length()], s.data(), s.length()),
                        s.length(),
                        Core::Buffer::deleteArrayFn<char>);
                } else {
                    // 9. 非DATA类型的entry log不需要apply到state machine中，直接skip
                    entry.type = Entry::SKIP;
                }
                entry.clusterTime = logEntry.cluster_time();
            }
            return entry;
        }
        // 10. nextIndex对应的entry log还没有commit，applyThreadMain线程直接sleep在stateChanged，
        //     等待后面commitIndex被推进后被唤醒重试。
        stateChanged.wait(lockGuard);
    }
}

SnapshotStats::SnapshotStats
RaftConsensus::getSnapshotStats() const
{
    std::lock_guard<Mutex> lockGuard(mutex);

    SnapshotStats::SnapshotStats s;
    s.set_last_snapshot_index(lastSnapshotIndex);
    s.set_last_snapshot_bytes(lastSnapshotBytes);
    s.set_log_start_index(log->getLogStartIndex());
    s.set_last_log_index(log->getLastLogIndex());
    s.set_log_bytes(log->getSizeBytes());
    s.set_is_leader(state == State::LEADER);
    return s;
}

// 需要考虑rpc request重复发送时的写幂等
// 处理来自leader(可能是旧leader，需要判断term)的appendEntries请求.
// !!!
// folower的该方法与leader的appendEntries方法相配合，达到了一个效果：
//  1）如果一条entry冲突，那么其后续的所有entry都冲突
//  2）如果一条entry正确，那么其前面的所有entry都正确
//  由于冲突的entries必然在某一时刻处于follower的log的suffix中，而leader给follower发送正确的entries前会去猜测follower的最后正确log同步点，
//  由于同步点和实际的lastLogIndex间不能留gap，且leader会从follower的lastLogIndex逐一回退来找正确的同步点，因此最后的正确同步点必然是
//  落在follower的尾部冲突entries之前的某个正确entry中，而trancateSuffix机制保证了在append正确entry之前follower尾部冲突entries必然已被全部截断，
//  也就保证了follower不会出现正确的entry之间夹着冲突entry的情况了。（当然这依赖leader本身发送过来的entries都是正确的）
void
RaftConsensus::handleAppendEntries(
                    const Protocol::Raft::AppendEntries::Request& request,
                    Protocol::Raft::AppendEntries::Response& response)
{
    std::lock_guard<Mutex> lockGuard(mutex);
    assert(!exiting);

    // Set response to a rejection. We'll overwrite these later if we end up
    // accepting the request.
    // 1. 先构造一个reject的response内容，带上自己所认的leader term以及本地raft log的lastLogIndex，
    //    如果后续真的需要拒绝时会返回给leader协助其做一些判断依据。
    response.set_term(currentTerm);
    response.set_success(false);
    response.set_last_log_index(log->getLastLogIndex());

    // Piggy-back server capabilities.
    // 2. 顺便带上自己本地的state machine版本区间。
    {
        auto& cap = *response.mutable_server_capabilities();
        auto& s = *configuration->localServer;
        if (s.haveStateMachineSupportedVersions) {
            cap.set_min_supported_state_machine_version(
                    s.minStateMachineVersion);
            cap.set_max_supported_state_machine_version(
                    s.maxStateMachineVersion);
        }
    }

    // If the caller's term is stale, just return our term to it.
    // 3. 自己本地的leader term比request中的大，说明发request的为旧leader，直接reject。
    if (request.term() < currentTerm) {
        VERBOSE("Caller(%lu) is stale. Our term is %lu, theirs is %lu",
                 request.server_id(), currentTerm, request.term());
        return; // response was set to a rejection above
    }
    if (request.term() > currentTerm) {
        NOTICE("Received AppendEntries request from server %lu in term %lu "
               "(this server's term was %lu)",
                request.server_id(), request.term(), currentTerm);
        // We're about to bump our term in the stepDown below: update
        // 'response' accordingly.
        response.set_term(request.term());
    }
    // ！！！
    // 4. 发request的leader的term >= 本地的term，说明本机认可该leader的身份了，该请求将视为该leader的一个保活信号，然后做两件事：
    //     1）本机stepDown为Follower然后更新本机的term为request中的更新term值
    //     2）leader心跳有效，重置本地的选举发起时间以及重置拒绝其余server选举请求的时间窗口
    // This request is a sign of life from the current leader. Update
    // our term and convert to follower if necessary; reset the
    // election timer. set it here in case request we exit the
    // function early, we will set it again after the disk write.
    // 值得注意的是，这里只要对方的term不小于currentTerm，本机就无条件承认对方的leader身份，即使对方可能在事实上已经没有了leader身份。
    // 但是即使如此，这个假leader也只能暂时污染少数派节点，如果多数派已经选出新leader，假leader的任何操作都得不到quorum，最终自己主动退位，
    // 而那些被污染的少数派节点也会在与新leader取得联系时被纠正。
    stepDown(request.term());
    setElectionTimer();
    withholdVotesUntil = Clock::now() + ELECTION_TIMEOUT;

    // Record the leader ID as a hint for clients.
    // 5. 记录认可的该leader的serverId，可以作为后续给请求到本机的client的重定向提示。
    if (leaderId == 0) {
        leaderId = request.server_id();
        NOTICE("All hail leader %lu for term %lu", leaderId, currentTerm);
        printElectionState();
    } else {
        assert(leaderId == request.server_id());
    }

    // For an entry to fit into our log, it must not leave a gap.
    // 6. leader猜测本机的本地log同步点错误了，会留gap，直接reject
    if (request.prev_log_index() > log->getLastLogIndex()) {
        VERBOSE("Rejecting AppendEntries RPC: would leave gap");
        return; // response was set to a rejection above
    }
    // It must also agree with the previous entry in the log (and, inductively
    // all prior entries).
    // Always match on index 0, and always match on any discarded indexes:
    // since we know those were committed, the leader must agree with them.
    // We could truncate the log here, but there's no real advantage to doing
    // that.
    // 7. leader猜测本机的本地log同步点错误了，猜测的Index仍有冲突，直接reject
    if (request.prev_log_index() >= log->getLogStartIndex() &&
        log->getEntry(request.prev_log_index()).term() !=
            request.prev_log_term()) {
        VERBOSE("Rejecting AppendEntries RPC: terms don't agree");
        return; // response was set to a rejection above
    }

    // If we got this far, we're accepting the request.
    // ！！！
    // 8. leader成功猜对了本机的本地log同步点（同步点落在了必然正确的snaoshot内，或对应index的entry log term一致），可以respnse success
    response.set_success(true);

    // This needs to be able to handle duplicated RPC requests. We compare the
    // entries' terms to know if we need to do the operation; otherwise,
    // reapplying requests can result in data loss.
    //
    // The first problem this solves is that an old AppendEntries request may be
    // duplicated and received after a newer request, which could cause
    // undesirable data loss. For example, suppose the leader appends entry 4
    // and then entry 5, but the follower receives 4, then 5, then 4 again.
    // Without this extra guard, the follower would truncate 5 out of its
    // log.
    //
    // The second problem is more subtle: if the same request is duplicated but
    // the leader processes an earlier response, it will assume the
    // acknowledged data is safe. However, there is a window of vulnerability
    // on the follower's disk between the truncate and append operations (which
    // are not done atomically) when the follower processes the later request.
    // 9. 然后将request中携带的log entry追加到本地的raft log中。
    //    ！！！需要注意的是，leader发送的appendEntries request和client发送的statemachine command使用的是相同的rpc机制，因此handle函数在处理写请求时需要保证幂等
    //    ！！！（不能假设request不会重复到达，或者不能假设两个request的entry内容完全没有重复），在client request中，这种重试在server端有state machine中的session表
    //    ！！！来保证写操作的exactly once语义，而对于appendEnries request，raft log的持久化充当了该语义的保证机制，而follower在执行entry追加时需要做详细的去重检测以保证append操作的exactly once。
    uint64_t index = request.prev_log_index();
    for (auto it = request.entries().begin();
         it != request.entries().end();
         ++it) {
        ++index;
        const Protocol::Raft::Entry& entry = *it;
        if (entry.has_index()) {
            // This precaution was added after #160: "Packing entries into
            // AppendEntries requests is broken (critical)".
            assert(entry.index() == index);
        }
        if (index < log->getLogStartIndex()) {
            // We already snapshotted and discarded this index, so presumably
            // we've received a committed entry we once already had.
            // 10. entry已经被包含在本机的snapshot中，直接跳过。
            continue;
        }
        if (log->getLastLogIndex() >= index) {
            if (log->getEntry(index).term() == entry.term())
                // 12. entry在本地raft log的范围内，且与对应index的entry重复，直接跳过
                continue;
            // 13. entry在本地raft log的范围内，但是对应index的entry与其冲突，直接truncate本地log的suffix部分，
            //     !!!需要注意的是，必须要找到精确的冲突index再执行截断，否则对已经持久化的正确log进行截断的话，一旦机器在截断之后append之前发生崩溃，将导致作为多数派的本机数据丢失，
            //     !!!虽然本机是follower，但是这会破坏多数派的备份假设，一旦leader突然不可用，选出来的新leader恰好是发生了这种崩溃的follower的话，将导致集群的数据丢失。
            // should never truncate committed entries:
            assert(commitIndex < index);
            uint64_t lastIndexKept = index - 1;
            uint64_t numTruncating = log->getLastLogIndex() - lastIndexKept;
            NOTICE("Truncating %lu entries after %lu from the log",
                   numTruncating,
                   lastIndexKept);
            numEntriesTruncated += numTruncating;
            log->truncateSuffix(lastIndexKept);
            configurationManager->truncateSuffix(lastIndexKept);
        }

        assert(index == (log->getLastLogIndex() + 1));
        // 14. 经历了前面的去重以及截断后，有效的request entry index来到了本地raft log的lastLogIndex + 1位置，将request中当前及后续的所有entry追加进本地raft log中。
        // Append this and all following entries.
        std::vector<const Protocol::Raft::Entry*> entries;
        do {
            const Protocol::Raft::Entry& entry = *it;
            if (entry.type() == Protocol::Raft::EntryType::UNKNOWN) {
                PANIC("Leader %lu is trying to send us an unknown log entry "
                      "type for index %lu (term %lu). It shouldn't do that, "
                      "and there's not a good way forward. There's some hope "
                      "that if this server reboots, it'll come back up with a "
                      "newer version of the code that understands the entry.",
                      index,
                      entry.term(),
                      leaderId);
            }
            entries.push_back(&entry);
            ++it;
            ++index;
        } while (it != request.entries().end());
        append(entries);
        clusterClock.newEpoch(entries.back()->cluster_time());
        break;
    }
    // 15. 新entry可能已经append进本地raft log，response中的lastLogIndex也要更新。
    response.set_last_log_index(log->getLastLogIndex());

    // Set our committed ID from the request's. In rare cases, this would make
    // our committed ID decrease. For example, this could happen with a new
    // leader who has not yet replicated one of its own entries. While that'd
    // be perfectly safe, guarding against it with an if statement lets us
    // make stronger assertions.
    // 16. 如果request中的commitIndex大于本地值，则推进本地Index，然后唤醒其他后台线程例如apply statemachine线程。
    //     ！！！需要注意的是，我们为了保持commitIndex的单增性质，需要保证request中的值确实大于本地值才更新，考虑一种时序：
    //     ！！！旧leader把entry复制到多数派commit到10之后，给follower a发心跳，a也commit到了10，但是在给follower b发
    //     ！！！心跳之前旧leader就挂了，b的commit index只到了8，但是b本地确实作为多数派有了完整的log的，然后后面b首先发起
    //     ！！！选举并且顺利当选新leader，就出现了新leader b的commitIndex暂时落后follower a的情况，新leader b在发给a的首个
    //     ！！！心跳请求中request commideIndex就会小于a的本地值。
    if (commitIndex < request.commit_index()) {
        commitIndex = request.commit_index();
        assert(commitIndex <= log->getLastLogIndex());
        stateChanged.notify_all();
        VERBOSE("New commitIndex: %lu", commitIndex);
    }

    // reset election timer to avoid punishing the leader for our own
    // long disk writes
    // 17. 执行log append的磁盘操作前已经重置过一次选举计时器，但是这里需要再重置一次，因为leader的心跳需要等到前一个appendRequest回来才能发，如果
    //     follower自己由于长时间disk操作耽误了leader的及时心跳导致自己本地提前触发了选举，会造成对leader的不利。 
    setElectionTimer();
    withholdVotesUntil = Clock::now() + ELECTION_TIMEOUT;
}

// 需要考虑rpc request重复发送时的写幂等
// 处理leader发送过来的installSnapshot请求。
// ！！！在snapshot被完整接收之前，partial临时文件都是随时可以被删除的，leader的peer matchIndex也不会被更新。
void
RaftConsensus::handleInstallSnapshot(
        const Protocol::Raft::InstallSnapshot::Request& request,
        Protocol::Raft::InstallSnapshot::Response& response)
{
    std::lock_guard<Mutex> lockGuard(mutex);
    assert(!exiting);

    response.set_term(currentTerm);

    // If the caller's term is stale, just return our term to it.
    if (request.term() < currentTerm) {
        VERBOSE("Caller(%lu) is stale. Our term is %lu, theirs is %lu",
                 request.server_id(), currentTerm, request.term());
        return;
    }
    if (request.term() > currentTerm) {
        NOTICE("Received InstallSnapshot request from server %lu in "
               "term %lu (this server's term was %lu)",
                request.server_id(), request.term(), currentTerm);
        // We're about to bump our term in the stepDown below: update
        // 'response' accordingly.
        response.set_term(request.term());
    }
    // This request is a sign of life from the current leader. Update our term
    // and convert to follower if necessary; reset the election timer.
    stepDown(request.term());
    setElectionTimer();
    withholdVotesUntil = Clock::now() + ELECTION_TIMEOUT;

    // Record the leader ID as a hint for clients.
    if (leaderId == 0) {
        leaderId = request.server_id();
        NOTICE("All hail leader %lu for term %lu", leaderId, currentTerm);
        printElectionState();
    } else {
        assert(leaderId == request.server_id());
    }

    if (!snapshotWriter) {
        // 1. snapshotWriter实例构造时会创建一个name带有partial字段的临时写入文件，然后持有该文件的一个file descriptor，用于接收后续的snapshot chunk写入。
        //    该实例是和某一指定term leader的snapshot发送流程绑定的，一旦term发生改变，这个实例会在stepdown的时候被discard。
        snapshotWriter.reset(
            new Storage::SnapshotFile::Writer(storageLayout));
    }
    response.set_bytes_stored(snapshotWriter->getBytesWritten());

    if (request.byte_offset() < snapshotWriter->getBytesWritten()) {
        // 2. request chunk旧了，本地已有，直接抛弃
        WARNING("Ignoring stale snapshot chunk for byte offset %lu when the "
                "next byte needed is %lu",
                request.byte_offset(),
                snapshotWriter->getBytesWritten());
        return;
    }
    if (request.byte_offset() > snapshotWriter->getBytesWritten()) {
        // 3. request chunk和follower已经写入的部分有gap，先不写入
        WARNING("Leader tried to send snapshot chunk at byte offset %lu but "
                "the next byte needed is %lu. Discarding the chunk.",
                request.byte_offset(),
                snapshotWriter->getBytesWritten());
        if (!request.has_version() || request.version() < 2) {
            // For compatibility with InstallSnapshot version 1 leader: such a
            // leader assumes the InstallSnapshot RPC succeeded if the terms
            // match (it ignores the 'bytes_stored' field). InstallSnapshot
            // hasn't succeeded here, so we can't respond ok.
            WARNING("Incrementing our term (to %lu) to force the leader "
                    "(of %lu) to step down and forget about the partial "
                    "snapshot it's sending",
                    currentTerm + 1,
                    currentTerm);
            stepDown(currentTerm + 1);
            // stepDown() changed currentTerm to currentTerm + 1
            response.set_term(currentTerm);
        }
        return;
    }
    // 4. request chunck恰好接在follower已经写入的部分后，可以写入。
    //    ！！！需要注意的是，这里写入并不会触发fsync磁盘操作，因为在snapshot完整接收到之前，
    //    ！！！partial文件都是可抛弃的，没有必要sync，leader也不会在snapshot完整被接收
    //    ！！！之前更新peer matchIndex。
    snapshotWriter->writeRaw(request.data().data(), request.data().length());
    response.set_bytes_stored(snapshotWriter->getBytesWritten());

    if (request.done()) {
        if (request.last_snapshot_index() < lastSnapshotIndex) {
            // 5. snapshot文件完整被接收了，但是比follower本地已有的snapshot还旧，直接删除
            WARNING("The leader sent us a snapshot, but it's stale: it only "
                    "covers up through index %lu and we already have one "
                    "through %lu. A well-behaved leader shouldn't do that. "
                    "Discarding the snapshot.",
                    request.last_snapshot_index(),
                    lastSnapshotIndex);
            snapshotWriter->discard();
            snapshotWriter.reset();
            return;
        }
        NOTICE("Loading in new snapshot from leader");
        // 6. snapshot文件完整被接收了，并且比follower本地的要新，save取代旧snapshot
        snapshotWriter->save();
        snapshotWriter.reset();
        // 7. 执行readSnapshot，将新snapshot的信息加载进follower本地
        readSnapshot();
        // 8. 唤醒某些线程执行任务
        stateChanged.notify_all();
        // 9. 和handleAppendEntries同理，save操作和readSnapshot操作都涉及磁盘同步操作，需要
        //    重置本地的选举计时器防止对leader不利。
        setElectionTimer();
        withholdVotesUntil = Clock::now() + ELECTION_TIMEOUT;
    }
}

// 需要考虑rpc request重复发送时的写幂等
// 处理来自Candidate节点的拉票request
void
RaftConsensus::handleRequestVote(
                    const Protocol::Raft::RequestVote::Request& request,
                    Protocol::Raft::RequestVote::Response& response)
{
    std::lock_guard<Mutex> lockGuard(mutex);
    assert(!exiting);

    // If the caller has a less complete log, we can't give it our vote.
    uint64_t lastLogIndex = log->getLastLogIndex();
    uint64_t lastLogTerm = getLastLogTerm();
    // 1. 比较candidate的log和本机本地log谁更新，last term越大越新，term相同，last index越大越新
    bool logIsOk = (request.last_log_term() > lastLogTerm ||
                    (request.last_log_term() == lastLogTerm &&
                     request.last_log_index() >= lastLogIndex));

    // ！！！
    // 2. 最近一段时间刚刚收到leader的heartbeat，说明现任leader健康，不管candidate的拉票term值是多少，都直接拒绝投票。
    //    该判断可以避免健康的leader被异常节点的新选举拉下台，但是无法阻止异常节点在选举失败后仍然单增其本地的term值。
    if (withholdVotesUntil > Clock::now()) {
        NOTICE("Rejecting RequestVote for term %lu from server %lu, since "
               "this server (which is in term %lu) recently heard from a "
               "leader (%lu). Should server %lu be shut down?",
               request.term(), request.server_id(), currentTerm,
               leaderId, request.server_id());
        response.set_term(currentTerm);
        response.set_granted(false);
        response.set_log_ok(logIsOk);
        return;
    }

    // 3. 如果candidate的拉票term大于本机的term，本机无条件stepdown为follower，并且如果本机自己也处于candidate状态，
    //    stepdown时也会终止自身的选举流程。
    if (request.term() > currentTerm) {
        NOTICE("Received RequestVote request from server %lu in term %lu "
               "(this server's term was %lu)",
                request.server_id(), request.term(), currentTerm);
        stepDown(request.term());
    }

    // At this point, if leaderId != 0, we could tell the caller to step down.
    // However, this is just an optimization that does not affect correctness
    // or really even efficiency, so it's not worth the trouble.

    // 4. 这里有个需要注意的点，如果candidate的拉票term在本机进行stepdown前就相等，那么本机的当前状态可能有两种：
    //   1）candidate状态，那么此时本机的votefor必然是自己，也就不会投票给其他server
    //   2）follower状态，即使本机的voteFor为0，投给了这个candidate，由于每个term下只有一个candidate能够拿到多数派，最终该term下也只有一个leader，但是本机的votefor可能和实际leader不一致，这是raft允许的
    if (request.term() == currentTerm) {
        if (logIsOk && votedFor == 0) {
            // Give caller our vote
            // 5. candidate的log足够新并且本机在该term下没有voteFor，可以投票给该candidate
            NOTICE("Voting for %lu in term %lu",
                   request.server_id(), currentTerm);
            // 6. 确认投票时再次stepdown一下保证本机为follower了，顺便interruptAll正在进行的peer rpc活动，当前更多是防御措施，如果后面引入pre vote的话，这个stepdown就有实际意义。
            stepDown(currentTerm);
            // 7. 重置本机选举计时器，给voteFor节点一点时间收集多数派选票成为leader后发heartbeat，避免自己刚投完票马上又发起新选举。
            setElectionTimer();
            votedFor = request.server_id();
            // ！！！
            // 8. 在确认投票的rpc response发送回去给candidate之前，必须先将本机的term+voteFor持久化到metadata文件，这里有两种崩溃情况：
            //    1）如果本机在持久化后发送response前崩溃，最多就是集群的投票损失一票，最差情况就是谁也拿不到多数派选票，后续重新发起新选举即可
            //    2）如果本机在response后崩溃，由于本机已经持久化投票信息，后续也不会出现重复投票的情况
            updateLogMetadata();
            printElectionState();
        }
    }

    // Fill in response.
    response.set_term(currentTerm);
    // don't strictly need the first condition
    // 9. 本机的term为candidate的拉票term并且本机voteFor为该candidate的serverId，则投票成功
    response.set_granted(request.term() == currentTerm &&
                         votedFor == request.server_id());
    response.set_log_ok(logIsOk);
}

std::pair<RaftConsensus::ClientResult, uint64_t>
RaftConsensus::replicate(const Core::Buffer& operation)
{
    std::unique_lock<Mutex> lockGuard(mutex);
    Log::Entry entry;
    entry.set_type(Protocol::Raft::EntryType::DATA);
    entry.set_data(operation.getData(), operation.getLength());
    return replicateEntry(entry, lockGuard);
}

// 该方法是leader节点处理client的集群配置变更请求。
// ！！！
// 配置变更是最容易造成脑裂的，如果直接将stable new configuration复制到新集群而不考虑旧集群的话，考虑以下情况：
// 当前集群的leader作为新集群的临时leader负责向新集群多数派写入stable new configuration，在将stable new configuration复制到
// 新集群的时候，leader突然断线，这时候旧集群节点和新集群节点会分别按照自己本地的configuration发起选举拉票，
// 这时候新集群的已经有stable new configuration的节点会获得新集群的leader，而旧集群的节点还是根据旧配置选出旧集群的leader，
// 这样就导致了一个raft组产生了两个leader，即脑裂。
// 因此需要一个机制，保证旧集群能够根据旧配置独立选出leader的情况下，新集群选出的合法leader必然还要经过旧集群的多数派投票，否则选不出leader，新集群
// 能够根据新配置独立选出leader的情况下，旧集群选出的合法leader必然还要经过新集群的多数派投票，否则选不出leader。这就是联合共识。
// 为了实现联合共识，raft在配置变更时，先后引入了三种configuration state：
//   1）staging state：该配置状态不会进入raft log。该状态下，新集群节点作为leader的follower不断接收来自leader的log entry，直到其复制追赶速率达到了
//                     leader的append新增速率，leader就认为该follower成功caught up。这个过程中的所有新增append log都不需要来自新集群的共识。
//   2）transitional state：该配置状态会同时记录旧集群(prev)和新集群(next)的membership信息进入一条raft log。当新集群节点全部都caught up之后，
//                          就会进入该状态。该状态下，所有的append log（包括transitional state log）不仅会复制到新旧集群，而且commit也需要同
//                          时获得来自新旧集群的共识。
//   3）stable state：该配置状态只记录新集群的membership信息进入一条raft log。当leader本地advanceCommitIndex（需要新旧集群联合共识）到
//                    transitional state log index的时候，就会进入该状态。该状态下，所有的append log（包括stable state log）只会复制到新集群，
//                    commit也只需要来自新集群的共识。当该stable state log被leader确认commit后，如果leader不属于新集群，leader就会stepDown，
//                    随后新集群节点根据新配置选出自己的独立leader。至此集群配置就算成功变更了。
/////////////////////////////////////////////////////////////////////////////////////////
// 因此，raft解决脑裂的联合共识机制中发挥主要作用的就是transitional state这个中间状态。只有当transitional state log被同时写进新集群和旧集群的多数派
// 后，真正的stable state log才会被写入新集群节点。考虑三种leader的崩溃时机：
//   1）如果stable state log成功commit，新集群节点可以单独根据新配置选出新集群的独立leader，而旧集群节点发起选举时要么是连transitional state log
//      都没有的少数派落后节点，要么是含有transitional state log的多数派发起选举时必须同时获得来自新集群的多数派共识（事实上，由于新集群的多数派已经
//      含有stable state log，而旧集群所有节点都不含有stable state log，所以新集群多数派的log必然比旧集群的任意一个节点都要新，因此旧集群节点发起的
//      新选举不可能获得来自新集群的多数派共识）。
//   2）如果只有transactional state log成功commit而stable state log没有成功commit，那么后续新旧集群的任意胜选新leader都必然同时获得了两边的共识，
//      然后新的leader作为两边的共同leader继续配置变更操作。（或者新集群的含有stable state log的少数派节点直接发起选举并胜选，这就回到1）的情况了，只是
//      新集群并没有stable state log的多数派复制，所以旧集群节点还是有可能获得来自新集群的共识并胜选，但是也会和新集群的独立leader选举冲突，导致最后
//      两边只能选出一个leader）
//   3）如果transitional state log也没有commit成功，旧集群节点可能可以单独根据旧配置选出旧集群的独立leader，而新集群不含有该log的节点会在发起新选
//      举时被本地挡住（历史membership不含有新节点id），含有该log的新集群节点在发起新选举时必然需要同时获取来自旧集群的共识。
RaftConsensus::ClientResult
RaftConsensus::setConfiguration(
        const Protocol::Client::SetConfiguration::Request& request,
        Protocol::Client::SetConfiguration::Response& response)
{
    std::unique_lock<Mutex> lockGuard(mutex);

    if (exiting || state != State::LEADER) {
        // caller fills out response
        // 2. configuration变更需要append log进入raft log，所以只能通过leader节点执行
        return ClientResult::NOT_LEADER;
    }
    if (configuration->id != request.old_id()) {
        // configurations has changed in the meantime
        // JOEY_TODO: 是否可以启蒙下state machine写请求的CAS？
        // 3. 由于server允许并发处理client的任何请求，所以会出现一个配置变更还在执行中，又有另一个请求过来，这里直接根据old_id做CAS，
        //    client在发起setConfiguration前需要先通过getConfiguration获取当前的stable configuration id。
        response.mutable_configuration_changed()->set_error(
            Core::StringUtil::format(
                "The current configuration has ID %lu (no longer %lu) "
                "and it's %s",
                configuration->id,
                request.old_id(),
                Core::StringUtil::toString(configuration->state).c_str()));
        return ClientResult::FAIL;
    }
    if (configuration->state != Configuration::State::STABLE) {
        // 4. 只有当前的配置已经是stable state了（getConfiguration逻辑保证，如果client拿到了当前stable config id，说明当前stable必然是commited的），
        //    才允许新的配置变更，进一步排除有其他配置变更流程正在执行的可能性。
        response.mutable_configuration_changed()->set_error(
            Core::StringUtil::format(
                "The current configuration (%lu) is not stable (it's %s)",
                configuration->id,
                Core::StringUtil::toString(configuration->state).c_str()));
        return ClientResult::FAIL;
    }

    NOTICE("Attempting to change the configuration from %lu",
           configuration->id);

    // Set the staging servers in the configuration.
    Protocol::Raft::SimpleConfiguration nextConfiguration;
    for (auto it = request.new_servers().begin();
         it != request.new_servers().end();
         ++it) {
        NOTICE("Adding server %lu at %s to staging servers",
               it->server_id(), it->addresses().c_str());
        Protocol::Raft::Server* s = nextConfiguration.add_servers();
        s->set_server_id(it->server_id());
        s->set_addresses(it->addresses());
    }
    // 5. 将configuration state设置成staging state，为每个新来的server节点新建一个peer线程，并填充configuration new_servers列表,
    //    staging state不会进raft log。
    configuration->setStagingServers(nextConfiguration);
    stateChanged.notify_all();

    // Wait for new servers to be caught up. This will abort if not every
    // server makes progress in a ELECTION_TIMEOUT period.
    // 6. 新配置中的节点已经通过各自的peer线程开始同步leader的log，当前leader心跳至新节点时会促使其stepdown为follower，
    //    然后leader将作为新集群节点的临时leader负责后续的完整配置变更流程
    uint64_t term = currentTerm;
    // 7. 开新epoch，设置checkProgressAt超时检测，用于定时检查是否所有新节点都没有断线以及持续和leader保持进展。直到所有新节点都caught up。
    ++currentEpoch;
    uint64_t epoch = currentEpoch;
    TimePoint checkProgressAt = Clock::now() + ELECTION_TIMEOUT;
    while (true) {
        if (exiting || term != currentTerm) {
            // 8. 等待新节点caught up期间leader失去了身份，直接终止配置变更（resetStagingServers在之前stepdown的时候就做了）
            NOTICE("Lost leadership, aborting configuration change");
            // caller will fill in response
            return ClientResult::NOT_LEADER;
        }
        if (configuration->stagingAll(&Server::isCaughtUp)) {
            // 9. 所有新节点都已经caught up，可以进入配置变更的下一步了
            NOTICE("Done catching up servers");
            break;
        }
        if (Clock::now() >= checkProgressAt) {
            // 10. 线程被超时唤醒了，直接检查是否这段时间内所有新节点都保持了进展
            using RaftConsensusInternal::StagingProgressing;
            StagingProgressing progressing(epoch, response);
            if (!configuration->stagingAll(progressing)) {
                // 11. 通过检查lastAckEpoch得知，有的新节点在这段时间内没有获得任何进展，直接终止配置变更
                NOTICE("Failed to catch up new servers, aborting "
                       "configuration change");
                // 12. 将本地configuration回滚回此前的stable state（id和description未改变）
                configuration->resetStagingServers();
                stateChanged.notify_all();
                // progressing filled in response
                return ClientResult::FAIL;
            } else {
                // 13. 所有新节点都在顺利保持进展，开新epoch并重置超时检测，继续等待caught up
                ++currentEpoch;
                epoch = currentEpoch;
                checkProgressAt = Clock::now() + ELECTION_TIMEOUT;
            }
        }
        // 14. sleep放锁等待新节点caught up，或者超时起来检查进展。
        stateChanged.wait_until(lockGuard, checkProgressAt);
    }

    // 15. staging state下所有新节点都已经成功caught up了，可以进行下一步transitional state。
    // Write and commit transitional configuration
    NOTICE("Writing transitional configuration entry");
    Protocol::Raft::Configuration newConfiguration;
    *newConfiguration.mutable_prev_configuration() =
        configuration->description.prev_configuration();
    *newConfiguration.mutable_next_configuration() = nextConfiguration;
    Log::Entry entry;
    entry.set_type(Protocol::Raft::EntryType::CONFIGURATION);
    *entry.mutable_configuration() = newConfiguration;
    // 16. 将transitional state log追加至leader本地raft log，等待本地fsync并复制给多数派quorum。leader本地配置会首先激活为transitional state，
    //     由于transitional state是联合共识，所以该state下所有log（包括transitional state log本身）都需要同时获得来自新集群和旧集群的共识。
    std::pair<ClientResult, uint64_t> result =
        replicateEntry(entry, lockGuard);
    if (result.first != ClientResult::SUCCESS) {
        // 17. 线程结束等待时，transitional state log没有获得多数派quorum，直接终止配置变更
        NOTICE("Failed to commit transitional configuration entry, aborting "
               "configuration change (%s)",
               Core::StringUtil::toString(result.first).c_str());
        if (result.first == ClientResult::NOT_LEADER) {
            // caller will fill in response
        } else {
            response.mutable_configuration_changed()->set_error(
                Core::StringUtil::format(
                    "Couldn't successfully replicate the transitional "
                    "configuration (%s)",
                    Core::StringUtil::toString(result.first).c_str()));
        }
        return result.first;
    }
    uint64_t transitionalId = result.second;

    // Wait until the configuration that removes the old servers has been
    // committed. This is the first configuration with ID greater than
    // transitionalId.
    // 18. transitional state log被成功复制到了多数派quorum然后被leader成功推进commitIndex，
    //     并且此前在advanceCommitIndex时leader已经通过append方法触发将stable state log追加到了本地log
    //     以及激活了本地configuration为stable state，并触发了本地异步fsync以及复制到多数派quorum（stable状态
    //     下quorum只会需要新集群共识，log也只会复制到新集群）。
    //     当前线程只需要继续等待stable state log quorum ok并commit，就完成了配置变更流程。
    NOTICE("Waiting for stable configuration to commit");
    while (true) {
        // Check this first: if the new configuration excludes us so we've
        // stepped down upon committing it, we still want to return success.
        // 19. 如果本机configuration已经至少激活到了transitionalId之后，并且本机的commitIndex至少推进到了当前configuration id，
        //     说明当前配置变更流程的stable state已经被成功commit。此时无论本地是否还是leader，都应该返回配置变更成功。
        if (configuration->id > transitionalId &&
            commitIndex >= configuration->id) {
            response.mutable_ok();
            NOTICE("Stable configuration committed. Configuration change "
                   "completed successfully");
            return ClientResult::SUCCESS;
        }
        // 20. 如果stable state log尚未commit但本机却丢失了leader身份，直接终止配置变更，返回操作结果不确定的“Not Leader” response给client。
        if (exiting || term != currentTerm) {
            NOTICE("Lost leadership");
            // caller fills in response
            return ClientResult::NOT_LEADER;
        }
        stateChanged.wait(lockGuard);
    }
}

void
RaftConsensus::setSupportedStateMachineVersions(uint16_t minSupported,
                                                uint16_t maxSupported)
{
    std::lock_guard<Mutex> lockGuard(mutex);
    auto& s = *configuration->localServer;
    if (!s.haveStateMachineSupportedVersions ||
        s.minStateMachineVersion != minSupported ||
        s.maxStateMachineVersion != maxSupported) {

        s.haveStateMachineSupportedVersions = true;
        s.minStateMachineVersion = minSupported;
        s.maxStateMachineVersion = maxSupported;
        stateChanged.notify_all();
    }
}

std::unique_ptr<Storage::SnapshotFile::Writer>
RaftConsensus::beginSnapshot(uint64_t lastIncludedIndex)
{
    std::lock_guard<Mutex> lockGuard(mutex);

    NOTICE("Creating new snapshot through log index %lu (inclusive)",
           lastIncludedIndex);
    std::unique_ptr<Storage::SnapshotFile::Writer> writer(
                new Storage::SnapshotFile::Writer(storageLayout));

    // Only committed entries may be snapshotted.
    // (This check relies on commitIndex monotonically increasing.)
    if (lastIncludedIndex > commitIndex) {
        PANIC("Attempted to snapshot uncommitted entries (%lu requested but "
              "%lu is last committed entry)", lastIncludedIndex, commitIndex);
    }

    // Format version of snapshot file is 1.
    uint8_t version = 1;
    writer->writeRaw(&version, sizeof(version));

    // set header fields
    SnapshotMetadata::Header header;
    header.set_last_included_index(lastIncludedIndex);
    // Set last_included_term and last_cluster_time:
    if (lastIncludedIndex >= log->getLogStartIndex() &&
        lastIncludedIndex <= log->getLastLogIndex()) {
        const Log::Entry& entry = log->getEntry(lastIncludedIndex);
        header.set_last_included_term(entry.term());
        header.set_last_cluster_time(entry.cluster_time());
    } else if (lastIncludedIndex == 0) {
        WARNING("Taking a snapshot covering no log entries");
        header.set_last_included_term(0);
        header.set_last_cluster_time(0);
    } else if (lastIncludedIndex == lastSnapshotIndex) {
        WARNING("Taking a snapshot where we already have one, covering "
                "entries 1 through %lu (inclusive)", lastIncludedIndex);
        header.set_last_included_term(lastSnapshotTerm);
        header.set_last_cluster_time(lastSnapshotClusterTime);
    } else {
        WARNING("We've already discarded the entries that the state machine "
                "wants to snapshot. This can happen in rare cases if the "
                "leader already sent us a newer snapshot. We'll go ahead and "
                "compute the snapshot, but it'll be discarded later in "
                "snapshotDone(). Setting the last included term in the "
                "snapshot header to 0 (a bogus value).");
        // If this turns out to be common, we should return NULL instead and
        // change the state machines to deal with that.
        header.set_last_included_term(0);
        header.set_last_cluster_time(0);
    }

    // Copy the configuration as of lastIncludedIndex to the header.
    std::pair<uint64_t, Protocol::Raft::Configuration> c =
        configurationManager->getLatestConfigurationAsOf(lastIncludedIndex);
    if (c.first == 0) {
        WARNING("Taking snapshot with no configuration. "
                "This should have been the first thing in the log.");
    } else {
        header.set_configuration_index(c.first);
        *header.mutable_configuration() = c.second;
    }

    // write header to file
    writer->writeMessage(header);
    return writer;
}

void
RaftConsensus::snapshotDone(
        uint64_t lastIncludedIndex,
        std::unique_ptr<Storage::SnapshotFile::Writer> writer)
{
    std::lock_guard<Mutex> lockGuard(mutex);
    if (lastIncludedIndex <= lastSnapshotIndex) {
        NOTICE("Discarding snapshot through %lu since we already have one "
               "(presumably from another server) through %lu",
               lastIncludedIndex, lastSnapshotIndex);
        writer->discard();
        return;
    }

    // log->getEntry(lastIncludedIndex) is safe:
    // If the log prefix for this snapshot was truncated, that means we have a
    // newer snapshot (handled above).
    assert(lastIncludedIndex >= log->getLogStartIndex());
    // We never truncate committed entries from the end of our log, and
    // beginSnapshot() made sure that lastIncludedIndex covers only committed
    // entries.
    assert(lastIncludedIndex <= log->getLastLogIndex());

    lastSnapshotBytes = writer->save();
    lastSnapshotIndex = lastIncludedIndex;
    const Log::Entry& lastEntry = log->getEntry(lastIncludedIndex);
    lastSnapshotTerm = lastEntry.term();
    lastSnapshotClusterTime = lastEntry.cluster_time();

    // It's easier to grab this configuration out of the manager again than to
    // carry it around after writing the header.
    std::pair<uint64_t, Protocol::Raft::Configuration> c =
        configurationManager->getLatestConfigurationAsOf(lastIncludedIndex);
    if (c.first == 0) {
        WARNING("Could not find the latest configuration as of index %lu "
                "(inclusive). This shouldn't happen if the snapshot was "
                "created with a configuration, as they should be.",
                lastIncludedIndex);
    } else {
        configurationManager->setSnapshot(c.first, c.second);
    }

    NOTICE("Completed snapshot through log index %lu (inclusive)",
           lastSnapshotIndex);

    // It may be beneficial to defer discarding entries if some followers are
    // a little bit slow, to avoid having to send them a snapshot when a few
    // entries would do the trick. Best to avoid premature optimization though.
    discardUnneededEntries();
}

void
RaftConsensus::updateServerStats(Protocol::ServerStats& serverStats) const
{
    std::lock_guard<Mutex> lockGuard(mutex);
    Core::Time::SteadyTimeConverter time;
    serverStats.clear_raft();
    Protocol::ServerStats::Raft& raftStats = *serverStats.mutable_raft();

    raftStats.set_current_term(currentTerm);
    switch (state) {
        case State::FOLLOWER:
            raftStats.set_state(Protocol::ServerStats::Raft::FOLLOWER);
            break;
        case State::CANDIDATE:
            raftStats.set_state(Protocol::ServerStats::Raft::CANDIDATE);
            break;
        case State::LEADER:
            raftStats.set_state(Protocol::ServerStats::Raft::LEADER);
            break;
    }
    raftStats.set_commit_index(commitIndex);
    raftStats.set_last_log_index(log->getLastLogIndex());
    raftStats.set_leader_id(leaderId);
    raftStats.set_voted_for(votedFor);
    raftStats.set_start_election_at(time.unixNanos(startElectionAt));
    raftStats.set_withhold_votes_until(time.unixNanos(withholdVotesUntil));
    raftStats.set_cluster_time_epoch(clusterClock.clusterTimeAtEpoch);
    raftStats.set_cluster_time(clusterClock.interpolate());

    raftStats.set_last_snapshot_index(lastSnapshotIndex);
    raftStats.set_last_snapshot_term(lastSnapshotTerm);
    raftStats.set_last_snapshot_cluster_time(lastSnapshotClusterTime);
    raftStats.set_last_snapshot_bytes(lastSnapshotBytes);
    raftStats.set_num_entries_truncated(numEntriesTruncated);
    raftStats.set_log_start_index(log->getLogStartIndex());
    raftStats.set_log_bytes(log->getSizeBytes());
    configuration->updateServerStats(serverStats, time);
    log->updateServerStats(serverStats);
}

std::ostream&
operator<<(std::ostream& os, const RaftConsensus& raft)
{
    std::lock_guard<RaftConsensus::Mutex> lockGuard(raft.mutex);
    typedef RaftConsensus::State State;
    os << "server id: " << raft.serverId << std::endl;
    os << "term: " << raft.currentTerm << std::endl;
    os << "state: " << raft.state << std::endl;
    os << "leader: " << raft.leaderId << std::endl;
    os << "lastSnapshotIndex: " << raft.lastSnapshotIndex << std::endl;
    os << "lastSnapshotTerm: " << raft.lastSnapshotTerm << std::endl;
    os << "lastSnapshotClusterTime: " << raft.lastSnapshotClusterTime
       << std::endl;
    os << "commitIndex: " << raft.commitIndex << std::endl;
    switch (raft.state) {
        case State::FOLLOWER:
            os << "vote: ";
            if (raft.votedFor == 0)
                os << "available";
            else
                os << "given to " << raft.votedFor;
            os << std::endl;
            break;
        case State::CANDIDATE:
            break;
        case State::LEADER:
            break;
    }
    os << *raft.log;
    os << *raft.configuration;
    return os;
}


//// RaftConsensus private methods that MUST acquire the lock
//// 以下private方法都是某一个后台线程的执行函数，会和从public方法进来的外界线程互斥访问成员变量，所以必须加锁。

// 每个server节点运行代码版本的state machine都支持一套自己的commands，不同版本的state machine可以识别执行的apply命令不同，
// 如果节点强行apply某条自身不支持的命令，会导致崩溃。
// 该方法对应的后台线程就是用于leader节点定时更新升级集群的state machine版本，保证集群的state machine版本升级到最新的其中的apply命令能够被所有节点的代码识别的版本。
// ！！！
// 之所以要专门一个后台线程负责随时能够醒来升级，是出于集群代码版本 “滚动升级” 需要，也就是，
// 逐个节点进行停/升/启的流程，在一个节点离线升级过程中，集群多数派节点仍然能够保持在线，
// 保证集群对外服务不中断，当一个节点升级完重启上线之后，leader节点收到的该节点support版本区间
// 会发生变化，这时候就需要stateMachineUpdaterThreadMain醒来检查是否需要进行集群代码版本升级。
// 由于该过程是逐个节点轮流进行，所以leader需要一个随时oncall的后台线程。
// JOEY_TODO: 做transfer leader，leader自己需要升级时主动让位，避免直接离线导致集群重选举导致服务短暂不可用。
void
RaftConsensus::stateMachineUpdaterThreadMain()
{
    // This implementation might create many spurious entries, since this
    // process will append a state machine version if it hasn't appended that
    // same version before during this boot. That should be fine for most use
    // cases. If the state machine's num_redundant_advance_version_entries
    // server stat gets to be large, this may need to be revisited.
    std::unique_lock<Mutex> lockGuard(mutex);
    Core::ThreadId::setName("StateMachineUpdater");
    // 1. lastVersionCommitted是线程局部变量，且不会持久化，所以leader重启后可能会往raft log中append一个<=上一个已commit version的版本，
    //    因此这个变量并不是一个安全状态，只是一个本地优化。真正的安全保证在statemachine执行apply的时候：
    //       1）节点的statemachine runningVersion如果已经是新版本，在apply到一个更旧的version时会直接拒绝，也就是state machine running version是单增的，不会降级。
    //       2）节点的代码版本如果比较旧，那么他在apply到前面的更新version时会直接PANIC
    uint64_t lastVersionCommitted = 0;
    TimePoint backoffUntil = TimePoint::min();
    while (!exiting) {
        TimePoint now = Clock::now();
        // 2. 本质上还是往raft log中append一条state machine升级的log，所以也是只能在leader节点中执行
        if (backoffUntil <= now && state == State::LEADER) {
            using RaftConsensusInternal::StateMachineVersionIntersection;
            StateMachineVersionIntersection s;
            // 3. 统计本机已知的所有server节点支持的state machine版本区间（对方server节点支持的区间由其handleAppendEntries时一起
            //    response回本机leader）交集（minVersion是所有区间min的最大值，maxVersion是所有区间max的最小值）。
            //    ！！！
            //    注意，这里是统计knownServers的所有节点，包括staging servers。避免某个state machine代码版本比较旧的节点突然网络分区
            //    和集群断联时集群偷偷升级版本，导致该节点重连集群时出问题。只有当一个节点被stable移出集群了，knownServers发生改变后，后续计算出的
            //    升级版本号才不会考虑该节点。这也就意味着，如果一个被移出集群的server节点如果后续要重新加入集群，新运行的state machine代码版本必须至少不比当前集群已提交的最大state machine runningVersion旧，否则
            //    在apply日志的时候一旦apply到不支持的新版本号，就会PANIC。如果是在配置变更成功之后，新节点才apply到不支持的新版本号，严重时可能导致整个集群多数派全部崩溃导致集群不可用。
            // JOEY_TODO: leader本机在配置变更时，在进入staging state前后，必须分别执行一次configuration for each StateMachineVersionIntersection计算，如果进入staging state后
            //            找不到交集或者交集的maxVersion变小，说明新配置节点state machine代码存在版本问题/版本过旧，应该拒绝变更配置。
            configuration->forEach(std::ref(s));
            if (s.missingCount == 0) {
                // 4. 所有已知server都有对应的version区间了
                if (s.minVersion > s.maxVersion) {
                    // 5. 没有找到一个所有server都重叠的有效交集，设置backoffUntil sleep时间后续再尝试
                    ERROR("The state machines on the %lu servers do not "
                          "currently support a common version "
                          "(max of mins=%u, min of maxes=%u). Will wait to "
                          "change the state machine version for at least "
                          "another backoff period",
                          s.allCount,
                          s.minVersion,
                          s.maxVersion);
                    backoffUntil = now + STATE_MACHINE_UPDATER_BACKOFF;
                } else { // s.maxVersion is the one we want
                    // 6. 找到了所有已知server节点的区间的重叠有效交集，取交集的maxVersion作为本次升级的版本
                    if (s.maxVersion > lastVersionCommitted) {
                        // 7. maxVersion大于上次commit的升级版本，可以执行升级
                        NOTICE("Appending log entry to advance state machine "
                               "version to %u (it may be set to %u already, "
                               "but it's hard to check that and not much "
                               "overhead to just do it again)",
                               s.maxVersion,
                               s.maxVersion);
                        // 8. 先构造一条DATA类型的entry log，data内容是此次升级的version版本号
                        Log::Entry entry;
                        entry.set_term(currentTerm);
                        entry.set_type(Protocol::Raft::EntryType::DATA);
                        entry.set_cluster_time(clusterClock.leaderStamp());
                        Protocol::Client::StateMachineCommand::Request command;
                        command.mutable_advance_version()->
                            set_requested_version(s.maxVersion);
                        Core::Buffer cmdBuf;
                        Core::ProtoBuf::serialize(command, cmdBuf);
                        entry.set_data(cmdBuf.getData(), cmdBuf.getLength());

                        // 9，跟一般的state machine command和setConfiguration command一样，走replicateEntry
                        //    append到本地log后复制到quorum多数派和本地fsync，等待成功commit返回或者返回NOT_LEADER。
                        std::pair<ClientResult, uint64_t> result =
                            replicateEntry(entry, lockGuard);
                        if (result.first == ClientResult::SUCCESS) {
                            // 10. 成功commit了，更新一下lastVersionCommitted
                            lastVersionCommitted = s.maxVersion;
                        } else {
                            // 11. 复制多数派quorum失败了，可能是失去了leader，总之version升级没成功，
                            //     设置backoffUntil sleep时间后续重试
                            using Core::StringUtil::toString;
                            WARNING("Failed to commit entry to advance state "
                                    "machine version to version %u (%s). "
                                    "Will retry later after backoff period",
                                    s.maxVersion,
                                    toString(result.first).c_str());
                            backoffUntil = now + STATE_MACHINE_UPDATER_BACKOFF;
                        }
                        continue;
                    } else {
                        // We're in good shape, go back to sleep.
                        // 12. maxVersion不大于上次commit的升级版本，忽略此次升级，继续sleep
                    }
                }
            } else { // missing info from at least one server
                // 13. 本机已知的servers中有的server还没有回复过其支持的版本区间，信息不全，放弃本次升级，
                //     设置backoffUntil避免频繁重试失败。
                // Do nothing until we have info from everyone else
                // (stateChanged will be notified). The backoff is here just to
                // avoid spamming the NOTICE message.
                NOTICE("Waiting to receive state machine supported version "
                       "information from all peers (missing %lu of %lu)",
                       s.missingCount, s.allCount);
                backoffUntil = now + STATE_MACHINE_UPDATER_BACKOFF;
            }
        }
        // 14. 进入睡眠，等待下次向某server节点appendEntries后response返回了其supported version区间时被唤醒，
        //     或者直到backoffUntil超时被唤醒。
        if (backoffUntil <= now)
            stateChanged.wait(lockGuard);
        else
            stateChanged.wait_until(lockGuard, backoffUntil);
    }
    NOTICE("Exiting");
}

void
RaftConsensus::leaderDiskThreadMain()
{
    std::unique_lock<Mutex> lockGuard(mutex);
    Core::ThreadId::setName("LeaderDisk");
    // Each iteration of this loop syncs the log to disk once or sleeps until
    // that is necessary.
    while (!exiting) {
        if (state == State::LEADER && logSyncQueued) {
            uint64_t term = currentTerm;
            std::unique_ptr<Log::Sync> sync = log->takeSync();
            logSyncQueued = false;
            leaderDiskThreadWorking = true;
            {
                Core::MutexUnlock<Mutex> unlockGuard(lockGuard);
                sync->wait();
                // Mark this false before re-acquiring RaftConsensus lock,
                // since stepDown() polls on this to go false while holding the
                // lock.
                leaderDiskThreadWorking = false;
            }
            if (state == State::LEADER && currentTerm == term) {
                configuration->localServer->lastSyncedIndex = sync->lastIndex;
                advanceCommitIndex();
            }
            log->syncComplete(std::move(sync));
            continue;
        }
        stateChanged.wait(lockGuard);
    }
}

// ！！！！（Leader节点heartbeat + Leader节点定时stepdown检测 + Follower/Candidate节点定时选举检测，raft通过该机制保证了集群leader的持续活性）
// Follower节点和Candidate节点通过该后台线程定时发起新选举，只有当Leader节点不断heartbeat过来
// 重置本机选举超时时间，才能避免新选举的发生。
void
RaftConsensus::timerThreadMain()
{
    std::unique_lock<Mutex> lockGuard(mutex);
    Core::ThreadId::setName("startNewElection");
    while (!exiting) {
        if (Clock::now() >= startElectionAt)
            startNewElection();
        stateChanged.wait_until(lockGuard, startElectionAt);
    }
}

// peerThreadMain线程需要保证始终持有Peer实例的shared引用，防止configuration随时发生变化时Peer实例exit后被销毁，导致peerThreadMain线程无法访问peer
void
RaftConsensus::peerThreadMain(std::shared_ptr<Peer> peer)
{
    std::unique_lock<Mutex> lockGuard(mutex);
    Core::ThreadId::setName(
        Core::StringUtil::format("Peer(%lu)", peer->serverId));
    NOTICE("Peer thread for server %lu started", peer->serverId);

    // Each iteration of this loop issues a new RPC or sleeps on the condition
    // variable.
    while (!peer->exiting) {
        TimePoint now = Clock::now();
        TimePoint waitUntil = TimePoint::min();

        if (peer->backoffUntil > now) {
            // 上一次rpc的时候FAILED了，为了避免对方网络/cpu被打满，需要等待一段时间，此时等待时间还没到。
            waitUntil = peer->backoffUntil;
        } else {
            switch (state) {
                // Followers don't issue RPCs.
                case State::FOLLOWER:
                    waitUntil = TimePoint::max();
                    break;

                // Candidates request votes.
                case State::CANDIDATE:
                    if (!peer->requestVoteDone)
                        requestVote(lockGuard, *peer);
                    else
                        waitUntil = TimePoint::max();
                    break;

                // Leaders replicate entries and periodically send heartbeats.
                case State::LEADER:
                    if (peer->getMatchIndex() < log->getLastLogIndex() ||
                        peer->nextHeartbeatTime < now) {
                        // appendEntries delegates to installSnapshot if we
                        // need to send a snapshot instead
                        appendEntries(lockGuard, *peer);
                    } else {
                        waitUntil = peer->nextHeartbeatTime;
                    }
                    break;
            }
        }

        stateChanged.wait_until(lockGuard, waitUntil);
    }

    // must return immediately after this
    --numPeerThreads;
    stateChanged.notify_all();
    NOTICE("Peer thread for server %lu exiting", peer->serverId);
}

// ！！！！（Leader节点heartbeat + Leader节点定时stepdown检测 + Follower/Candidate节点定时选举检测，raft通过该机制保证了集群leader的持续活性）
// Leader节点通过该后台线程定时开新epoch+检测ackEpoch，只有当Leader发出去的对应epoch的heartbeat能得到集群多数派ack的时候，
// 检测才会通过，从而避免本机stepdown为Follower。
// 该后台线程用于leader节点通过定时开新epoch并检测ackEpoch的方式确认leader身份是否仍被集群多数派承认，如果检测失败将直接stepdown放弃leader身份。
// ！！！需要注意的是，心跳并不是这里发的，心跳由appendEntries定时发，这里只是定时检测ackEpoch。
void
RaftConsensus::stepDownThreadMain()
{
    std::unique_lock<Mutex> lockGuard(mutex);
    Core::ThreadId::setName("stepDown");
    while (true) {
        // Wait until this server is the leader and is not the only server in
        // the cluster.
        while (true) {
            if (exiting)
                return;
            if (state == State::LEADER) {
                // If this local server forms a quorum (it is the only server
                // in the configuration), we need to sleep. Without this guard,
                // this method would not relinquish the CPU.
                // 1. 先开新epoch
                ++currentEpoch;
                // 2. 有两种情况：
                //   1）本机已知configuration是单节点集群，该方法的所有ackEpoch检测都将自动成立，会导致该线程一直空转占据cpu，所以需要这个if判断迫使本机sleep到下面的stateChanged信号量中。
                //   2）本机已知configuration是多节点集群，以下的ackEpoch检测必然失败（因为当前线程还拿着锁，新epoch的心跳必然还没发），也即if为true，这时候直接break进入后续的正式quorumMin。
                if (configuration->quorumMin(&Server::getLastAckEpoch) <
                    currentEpoch) {
                    break;
                }
            }
            // 3. 非leader节点/已知为单节点集群，该线程一直sleep
            stateChanged.wait(lockGuard);
        }
        // Now, if an election timeout goes by without confirming leadership,
        // step down. The election timeout is a reasonable amount of time,
        // since it's about when other servers will start elections and bump
        // the term.
        TimePoint stepDownAt = Clock::now() + ELECTION_TIMEOUT;
        // 4. sleep等待心跳确认期间是放锁的，所以要先记录保存当前的term和epoch。
        uint64_t term = currentTerm;
        uint64_t epoch = currentEpoch; // currentEpoch was incremented above
        while (true) {
            if (exiting)
                return;
            if (currentTerm > term)
                // 5. sleep期间term变化了，放弃检测ackEpoch
                break;
            if (configuration->quorumMin(&Server::getLastAckEpoch) >= epoch)
                // 6. ackEpoch检测通过证明leader身份仍然被集群多数派认可，继续执行下一轮检测
                break;
            if (Clock::now() >= stepDownAt) {
                // 7. 超时时间内没有收到多数派ack认可，leader身份已不保，直接stepdown放弃leader身份，并且将本地term + 1
                NOTICE("No broadcast for a timeout, stepping down from leader "
                       "of term %lu (converting to follower in term %lu)",
                       currentTerm, currentTerm + 1);
                stepDown(currentTerm + 1);
                break;
            }
            // 8. 放锁sleep直到被唤醒或者超时唤醒
            stateChanged.wait_until(lockGuard, stepDownAt);
        }
    }
}

//// RaftConsensus private methods that MUST NOT acquire the lock
//// 以下private方法只被public方法或者带锁的private方法调用，所以不能带锁。

void
RaftConsensus::advanceCommitIndex()
{
    if (state != State::LEADER) {
        // getMatchIndex is undefined unless we're leader
        WARNING("advanceCommitIndex called as %s",
                Core::StringUtil::toString(state).c_str());
        return;
    }

    // calculate the largest entry ID stored on a quorum of servers
    // 所有raft节点共同参与多数派验证：
    // --对于leader节点来说，getMatchIndex() 返回 lastSyncedIndex，也即是本地log sync成功。
    // --对于远端peer follower节点来说，getMatchIndex() 返回 matchIndex，也即是AppendEntries返回成功。
    uint64_t newCommitIndex =
        configuration->quorumMin(&Server::getMatchIndex);
    if (commitIndex >= newCommitIndex)
        return;
    // If we have discarded the entry, it's because we already knew it was
    // committed.
    assert(newCommitIndex >= log->getLogStartIndex());
    // At least one of these entries must also be from the current term to
    // guarantee that no server without them can be elected.
    // 只有当多数派的matchIndex对应的term达到了当前leader的currentTerm，leader才能够推进commitIndex，避免committed log后续又被覆盖（参考becomeLeader方法中append no-op log的作用）
    if (log->getEntry(newCommitIndex).term() != currentTerm)
        return;
    commitIndex = newCommitIndex;
    VERBOSE("New commitIndex: %lu", commitIndex);
    assert(commitIndex <= log->getLastLogIndex());
    // commit Index成功推进了，这里主要是为了唤醒state machine的applyThreadMain后台线程，让他执行apply操作；
    // 其次是唤醒rpc service线程，让他进入waitForResponse。
    stateChanged.notify_all();

    // leader已经成功推动一个configuration复制到了多数派后成功commit，该configuration已经事实上成为了集群共识，
    // 可以根据configuration类型继续配置变更的下一步操作。
    if (state == State::LEADER && commitIndex >= configuration->id) {
        // Upon committing a configuration that excludes itself, the leader
        // steps down.
        // 1. 一般新commit的是stable configuration时，如果新配置确实不包含leader本机，leader需要主动stepdown，
        //    然后新集群节点会根据新配置membership重新发起选举选出新leader。
        if (!configuration->hasVote(configuration->localServer)) {
            NOTICE("Newly committed configuration does not include self. "
                   "Stepping down as leader");
            stepDown(currentTerm + 1);
            return;
        }

        // Upon committing a reconfiguration (Cold,new) entry, the leader
        // creates the next configuration (Cnew) entry.
        // 2. 新commit的是transitional configuration时，说明新旧集群都达成了联合共识状态，
        //    leader本地log可以追加并激活stable configuration，然后触发往新集群多数派复制
        //    stable configuration（注意这里不会再复制到旧集群多数派）。
        if (configuration->state == Configuration::State::TRANSITIONAL) {
            Log::Entry entry;
            entry.set_term(currentTerm);
            entry.set_type(Protocol::Raft::EntryType::CONFIGURATION);
            entry.set_cluster_time(clusterClock.leaderStamp());
            *entry.mutable_configuration()->mutable_prev_configuration() =
                configuration->description.next_configuration();
            append({&entry});
            return;
        }
    }
}

void
RaftConsensus::append(const std::vector<const Log::Entry*>& entries)
{
    for (auto it = entries.begin(); it != entries.end(); ++it)
        assert((*it)->term() != 0);
    std::pair<uint64_t, uint64_t> range = log->append(entries);
    if (state == State::LEADER) { // defer log sync
        logSyncQueued = true;
    } else { // sync log now
        std::unique_ptr<Log::Sync> sync = log->takeSync();
        sync->wait();
        log->syncComplete(std::move(sync));
    }
    uint64_t index = range.first;
    // ！！！！
    // 在本地已知一个新的configuration entry之时，需要立即激活该configuration，用于后续的log复制、quorum计算以及peer管理，
    // 本地先基于新的configuration做这些工作，并不会影响集群已有共识，反而这是后续这个configuration能够成功被committed的基础，
    // 但是在这个configuration被复制到多数派再commit之前，都是可以被覆盖的，这意味着本地激活的该configuration最终未能成为集群共识。
    for (auto it = entries.begin(); it != entries.end(); ++it) {
        const Log::Entry& entry = **it;
        if (entry.type() == Protocol::Raft::EntryType::CONFIGURATION)
            configurationManager->add(index, entry.configuration());
        ++index;
    }
    // 对于leader节点来说，log被append到本地（但是没sync）之后，会设置logSyncQueued = true以及更新LastLogIndex，
    // 然后notify_all会唤醒leaderDiskThread以及各个peer thread。
    // leaderDiskThreadMain 看到 logSyncQueued == true，就把 leader 本地尚未 sync 的 log 刷到稳定存储。
    // peerThread 看到 peer.matchIndex < log->getLastLogIndex()，就给对应 follower 发 AppendEntries。
    // ！！所以leader本地sync和复制到多数派实际上是并行进行的。
    // 需要注意的是，这里其实state machine的applyThreadMain这个后台线程也会也唤醒，只是在commitIndex被成功推进之前，他不会执行apply而是会继续睡眠。
    stateChanged.notify_all();
}

// 这个方法由Leader节点调用，兼顾了Leader向Folower发送的三种信息：
// 1. 正常发送本地的raft log entry
// 2. 作为入口触发installSnapshot向follower发送snapshot
// 3. 周期性发送heartbeat信号确保自身的Leader身份
void
RaftConsensus::appendEntries(std::unique_lock<Mutex>& lockGuard,
                             Peer& peer)
{
    uint64_t lastLogIndex = log->getLastLogIndex();
    // 1. Leader首先推测Follower的log同步点在prevLogIndex处
    uint64_t prevLogIndex = peer.nextIndex - 1;
    assert(prevLogIndex <= lastLogIndex);

    // Don't have needed entry: send a snapshot instead.
    if (peer.nextIndex < log->getLogStartIndex()) {
        // 2. 这种情况对应于Leader本地raft log的nextIndex对应entry已经在生成snapshot的时候被截断，
        //    Leader无法再提供单条log entry，只能直接发送snapshot
        installSnapshot(lockGuard, peer);
        return;
    }

    // Find prevLogTerm or fall back to sending a snapshot.
    // 3. term值是校验两副raft log在相同的index下的log entry内容是否相同的标识，
    //    因此必须获取prevLogTerm以确认Follower的log同步点是否就是prevLogIndex
    uint64_t prevLogTerm;
    if (prevLogIndex >= log->getLogStartIndex()) {
        // 1）当prevLogIndex处于Leader本地raft log区间时，直接访问对应log entry取term
        prevLogTerm = log->getEntry(prevLogIndex).term();
    } else if (prevLogIndex == 0) {
        // 2）follower从未从Leader处同步过任何log，term直接赋0
        prevLogTerm = 0;
    } else if (prevLogIndex == lastSnapshotIndex) {
        // 3）当prevLogIndex是Leader的lastSnapshotIndex时，直接读取lastSnapshotTerm
        prevLogTerm = lastSnapshotTerm;
    } else {
        // Don't have needed entry for prevLogTerm: send snapshot instead.
        // 4）prevLogIndex log entry已经被snapshot截断，无法通过访问单条log entry获取term，只能直接发送snapshot
        installSnapshot(lockGuard, peer);
        return;
    }

    // Build up request
    Protocol::Raft::AppendEntries::Request request;
    request.set_server_id(serverId);
    request.set_term(currentTerm);
    request.set_prev_log_term(prevLogTerm);
    request.set_prev_log_index(prevLogIndex);
    uint64_t numEntries = 0;
    // 4. suppressBulkData这个变量用于Leader决定当前是否应该携带log数据发送Follower RPC：
    //   1）当某次RPC的时候收到RPC failed（可能RPC ping心跳超时说明可能与Follower断连了），这个值会设置为true从而抑制下次RPC携带log数据，只做心跳探测；
    //   2）当某次RPC的时候获得了Follower的response success，说明Leader已经获取到了Follower的log同步点，这个值会设置为false
    //      从而允许下次RPC从同步点开始携带log数据进行发送。
    //   3）其余情况该值则继续保持旧值进行作用。
    if (!peer.suppressBulkData)
        numEntries = packEntries(peer.nextIndex, request);
    // 5. 告知Follower他应该推进commitIndex到什么位置，Follower的commitIndex不能超过Leader的值
    request.set_commit_index(std::min(commitIndex, prevLogIndex + numEntries));

    // Execute RPC
    Protocol::Raft::AppendEntries::Response response;
    TimePoint start = Clock::now();
    // 6. leader发送rpc之前先记录currentEpoch，后续如果收到follower的leader acknowledge response时，就更新peer的lastAckEpoch为该值，
    //    表示peer在该epoch内是认可当前leader的。
    //    ！！！注意，这个epoch必须在rpc发送之前记录，保证follower的leader ack是在leader对应的read-only操作开始（线性化点）之后再由follower发出的。
    uint64_t epoch = currentEpoch;
    Peer::CallStatus status = peer.callRPC(
                Protocol::Raft::OpCode::APPEND_ENTRIES,
                request, response,
                lockGuard);
    switch (status) {
        case Peer::CallStatus::OK:
            break;
        case Peer::CallStatus::FAILED:
            // 7. RPC Failed了，设置backoffUntil让在一段时间内不再发送rpc，并且suppressBulkData设置true以使得下次rpc为不携带数据的探测消息
            peer.suppressBulkData = true;
            peer.backoffUntil = start + RPC_FAILURE_BACKOFF;
            return;
        case Peer::CallStatus::INVALID_REQUEST:
            PANIC("The server's RaftService doesn't support the AppendEntries "
                  "RPC or claims the request is malformed");
    }

    // Process response
    // 8. follower正常接收到rpc request并正常返回response了，处理response

    if (currentTerm != request.term() || peer.exiting) {
        // we don't care about result of RPC
        // 9. 由于rpc等待response期间是放锁的，期间可能发生以下情况，直接return即可：
        //    1) leader重新选举导致term变化
        //    2) configuration变更导致peer被移除集群而被exit
        return;
    }
    // Since we were leader in this term before, we must still be leader in
    // this term.
    assert(state == State::LEADER);
    if (response.term() > currentTerm) {
        // 10. follower认的term比当前term要大，当前server直接stepDown退为Follower
        //     ！！！需要注意的是，从发起新选举的逻辑可知，response中的这个大term并不一定对应一个
        //     ！！！有效的leader，可能只是那个server此前长期处于candidate状态重复发起选举导致term不断抬高。
        //     ！！！而当前leader在stepdown之后，如果这个term的leader确实不存在，那
        //     ！！！后续会由于心跳缺失而重新发起新选举。
        //     ！！！这种情况不会影响正确性，但是会由于leader被迫重新选举导致影响可用性
        NOTICE("Received AppendEntries response from server %lu in term %lu "
               "(this server's term was %lu)",
                peer.serverId, response.term(), currentTerm);
        stepDown(response.term());
    } else {
        // ！！！
        // 11. follower认可当前server的LEADER身份了，更新peer的lastAckEpoch，并且唤醒依赖lastAckEpoch进行quorum多数派检查的线程
        assert(response.term() == currentTerm);
        peer.lastAckEpoch = epoch;
        stateChanged.notify_all();
        peer.nextHeartbeatTime = start + HEARTBEAT_PERIOD;
        if (response.success()) {
            // ！！！
            // 12. 当前Leader猜测的follower的log同步点正确，且follower已经成功把request中的entry log加到了其本地raft log，
            //     更新follower的log同步点matchIndex，然后试图推进本地commitIndex
            if (peer.matchIndex > prevLogIndex + numEntries) {
                // Revisit this warning if we pipeline AppendEntries RPCs for
                // performance.
                WARNING("matchIndex should monotonically increase within a "
                        "term, since servers don't forget entries. But it "
                        "didn't.");
            } else {
                peer.matchIndex = prevLogIndex + numEntries;
                advanceCommitIndex();
            }
            peer.nextIndex = peer.matchIndex + 1;
            peer.suppressBulkData = false;

            // 在一个server刚刚加入configuration，Peer在本地初始被创建的时候，需要监测follower的log同步点的caughtUp时机，
            // 用于RaftConsensus::setConfiguration操作判断何时才能将该server真正加入raft集群中，防止新server由于log同步点太旧
            // 导致影响集群的多数派检测。
            // ！！！
            // 这里判断follower是否caughtUp的依据不是follower必须赶上leader的实时lastLogIndex（或一定阈值内），因为leader的log会一直增长，
            // 如果要实现这种方式的话，caughtUp将很难成立，从而阻塞该server加入集群的流程，而是判断follower的追赶一段log entry的速率
            // 是否与leader的产生该段log entry的速率接近（耗时差值绝对值在ELECTION_TIMEOUT内，经验值），只要速率接近，那意味着follower的追赶
            // 跟上了leader的生产节奏，即使目前log同步仍有差距，但是未来预期是乐观的，可以继续该server加入集群的流程。有两种情况：
            //   1）leader生产速率 < follower追赶速率，即使暂时两者耗时差值大于ELECTION_TIMEOUT，但是由于follower会
            //      越来越接近leader的实时lastLogIndex，数轮后耗时差值会很快收敛到ELECTION_TIMEOUT内，然后设置caughtUp true。
            //   2）leader生产速率 > follower追赶速率，这时候两者的耗时差值可能持续大于ELECTION_TIMEOUT，follower为了赶上leader
            //      所需要同步的log entry数量越来越多，这时候需要持续开启新一轮的追赶。直到后续某一时刻，leader的生产速率下降或follower
            //      的追赶速率提升，两者耗时差值将收敛到ELECTION_TIMEOUT内，这时候follower的追赶预期乐观，就可以设置caughtUp true。
            if (!peer.isCaughtUp_ &&
                peer.thisCatchUpIterationGoalId <= peer.matchIndex) {
                // 13. follower当前轮的log追赶到达了此前设定的goal，当前轮结束，可以触发“生产-追赶”速率判断
                Clock::duration duration =
                    Clock::now() - peer.thisCatchUpIterationStart;
                // thisCatchUpIterationMs是当前轮的follower的追赶耗时；
                // lastCatchUpIterationMs是上一轮的follower的追赶耗时，也就是leader之前生产当前轮log entry的生产耗时；
                uint64_t thisCatchUpIterationMs =
                    uint64_t(std::chrono::duration_cast<
                                 std::chrono::milliseconds>(duration).count());
                if (labs(int64_t(peer.lastCatchUpIterationMs -
                                 thisCatchUpIterationMs)) * 1000L * 1000L <
                    std::chrono::nanoseconds(ELECTION_TIMEOUT).count()) {
                    // 14. 两者速率接近了，follower追赶预期乐观，caughtUp设置true，并且唤醒RaftConsensus::setConfiguration线程继续执行任务
                    peer.isCaughtUp_ = true;
                    stateChanged.notify_all();
                } else {
                    // 15. 两者速率仍有差距，follower追赶预期悲观，需要开启新一轮的追赶，更新goal为leader当前的lastLogIndex
                    peer.lastCatchUpIterationMs = thisCatchUpIterationMs;
                    peer.thisCatchUpIterationStart = Clock::now();
                    peer.thisCatchUpIterationGoalId = log->getLastLogIndex();
                }
            }
        } else {
            // 16. 当前Leader猜测的follower的log同步点错误，说明两者的log存在尾部冲突，回退nextIndex，
            //     下次尝试更早的log entry猜测follower log同步点。
            if (peer.nextIndex > 1)
                --peer.nextIndex;
            // A server that hasn't been around for a while might have a much
            // shorter log than ours. The AppendEntries reply contains the
            // index of its last log entry, and there's no reason for us to
            // set nextIndex to be more than 1 past that (that would leave a
            // gap, so it will always be rejected).
            if (response.has_last_log_index() &&
                peer.nextIndex > response.last_log_index() + 1) {
                // 17. follower给leader提供了其本地raft log的lastLogIndex，下次尝试可以直接基于该index发送nextIndex，
                //     从而加速猜测follower log同步点的回退。
                peer.nextIndex = response.last_log_index() + 1;
            }
        }
    }
    if (response.has_server_capabilities()) {
        auto& cap = response.server_capabilities();
        if (cap.has_min_supported_state_machine_version() &&
            cap.has_max_supported_state_machine_version()) {
            // 18. 本次response包含了follower支持的stateMachine版本期间，leader需要记录下来，
            //     然后唤醒stateMachineUpdaterThreadMain后台线程。
            peer.haveStateMachineSupportedVersions = true;
            peer.minStateMachineVersion = Core::Util::downCast<uint16_t>(
                cap.min_supported_state_machine_version());
            peer.maxStateMachineVersion = Core::Util::downCast<uint16_t>(
                cap.max_supported_state_machine_version());
            stateChanged.notify_all();
        }
    }
}

// 该方法是leader向follower发送自己snapshot的方法，当appendEntries时发现需要发送的entry已经被截断时就会触发该方法。
void
RaftConsensus::installSnapshot(std::unique_lock<Mutex>& lockGuard,
                               Peer& peer)
{
    // Build up request
    Protocol::Raft::InstallSnapshot::Request request;
    request.set_server_id(serverId);
    request.set_term(currentTerm);
    request.set_version(2);

    // Open the latest snapshot if we haven't already. Stash a copy of the
    // lastSnapshotIndex that goes along with the file, since it's possible
    // that this will change while we're transferring chunks).
    // 1. snapshotFile本质上是保存对于snapshot文件的一个mmap映射 + 一个file descriptor，因为
    //    rpc期间可能leader新的snashot已经生成，父目录的同名目录项也指向了新的snapshot文件，但是
    //    只要peer snapshotFile中还hold住fd，正在执行installSnapshot任务的snapshot文件就不会被系统回收。
    //    记录lastSnapshotIndex也是相同道理。
    if (!peer.snapshotFile) {
        namespace FS = Storage::FilesystemUtil;
        peer.snapshotFile.reset(new FS::FileContents(
            FS::openFile(storageLayout.snapshotDir, "snapshot", O_RDONLY)));
        peer.snapshotFileOffset = 0;
        peer.lastSnapshotIndex = lastSnapshotIndex;
        NOTICE("Beginning to send snapshot of %lu bytes up through index %lu "
               "to follower",
               peer.snapshotFile->getFileLength(),
               lastSnapshotIndex);
    }
    request.set_last_snapshot_index(peer.lastSnapshotIndex);
    request.set_byte_offset(peer.snapshotFileOffset);
    uint64_t numDataBytes = 0;
    if (!peer.suppressBulkData) {
        // The amount of data we can send is bounded by the remaining bytes in
        // the file and the maximum length for RPCs.
        numDataBytes = std::min(
            peer.snapshotFile->getFileLength() - peer.snapshotFileOffset,
            SOFT_RPC_SIZE_LIMIT);
    }
    request.set_data(peer.snapshotFile->get<char>(peer.snapshotFileOffset,
                                                  numDataBytes),
                     numDataBytes);
    request.set_done(peer.snapshotFileOffset + numDataBytes ==
                     peer.snapshotFile->getFileLength());

    // Execute RPC
    Protocol::Raft::InstallSnapshot::Response response;
    TimePoint start = Clock::now();
    uint64_t epoch = currentEpoch;
    Peer::CallStatus status = peer.callRPC(
                Protocol::Raft::OpCode::INSTALL_SNAPSHOT,
                request, response,
                lockGuard);
    switch (status) {
        case Peer::CallStatus::OK:
            break;
        case Peer::CallStatus::FAILED:
            peer.suppressBulkData = true;
            peer.backoffUntil = start + RPC_FAILURE_BACKOFF;
            return;
        case Peer::CallStatus::INVALID_REQUEST:
            PANIC("The server's RaftService doesn't support the "
                  "InstallSnapshot RPC or claims the request is malformed");
    }

    // Process response

    if (currentTerm != request.term() || peer.exiting) {
        // we don't care about result of RPC
        // 2. 等待rpc期间无锁，term可能已经发生改变，peer也可能已经被移出集群
        return;
    }
    // Since we were leader in this term before, we must still be leader in
    // this term.
    assert(state == State::LEADER);
    if (response.term() > currentTerm) {
        // 3. follower的term更新，leader主动stepdown退位
        NOTICE("Received InstallSnapshot response from server %lu in "
               "term %lu (this server's term was %lu)",
                peer.serverId, response.term(), currentTerm);
        stepDown(response.term());
    } else {
        // 4. follower认可了当前leader的身份。
        assert(response.term() == currentTerm);
        peer.lastAckEpoch = epoch;
        stateChanged.notify_all();
        peer.nextHeartbeatTime = start + HEARTBEAT_PERIOD;
        // 5. follower认可leader身份后会直接写snapshot，而不需要像appendEntries那样还要严格定位log同步点，因为snapshot本身默认都是正确的、已commit的。
        peer.suppressBulkData = false;
        if (response.has_bytes_stored()) {
            // Normal path (since InstallSnapshot version 2).
            // 6. 根据follower返回的实际已写入byte size更新snapshotFileOffset，而不是靠leader推测
            peer.snapshotFileOffset = response.bytes_stored();
        } else {
            // This is the old path for InstallSnapshot version 1 followers
            // only. The leader would just assume the snapshot chunk was always
            // appended to the file if the terms matched.
            peer.snapshotFileOffset += numDataBytes;
        }
        // 7. 只有当follower已经完整的复制了整个snapshot，才会触发更新peer的matchIndex和nextIndex，为的是
        //    在snapshot不断发送的过程中，appendEntries能够不断被触发并且总会重定向到installSnapshot方法中。
        if (peer.snapshotFileOffset == peer.snapshotFile->getFileLength()) {
            NOTICE("Done sending snapshot through index %lu to follower",
                   peer.lastSnapshotIndex);
            // 8. follower的本地log都会失效，全部以刚刚接收到的snapshot为准，所以matchIndex直接就是lastSnapshotIndex即可
            peer.matchIndex = peer.lastSnapshotIndex;
            peer.nextIndex = peer.lastSnapshotIndex + 1;
            // These entries are already committed if they're in a snapshot, so
            // the commitIndex shouldn't advance, but let's just follow the
            // simple rule that bumping matchIndex should always be
            // followed by a call to advanceCommitIndex():
            // 9. peer matchIndex更新后，按规则触发一次advanceCommitIndex，但实际上leader本地的commitIndex必然早已 >= lastSnapshotIndex，所以这里并不会真正增加leader commitIndex
            advanceCommitIndex();
            // 10. snapshot已经完成发送，析构snapshotFile实例，这会解映射mmap然后close file descriptor，如果该snapshot此前已经被新snapshot取代目录项，最终会触发系统回收旧snapshot文件
            peer.snapshotFile.reset();
            peer.snapshotFileOffset = 0;
            peer.lastSnapshotIndex = 0;
        }
    }
}

// candidate节点在获取到多数派选票之后，就会调用该方法成为currentTerm下的leader
void
RaftConsensus::becomeLeader()
{
    assert(state == State::CANDIDATE);
    NOTICE("Now leader for term %lu (appending no-op at index %lu)",
           currentTerm,
           log->getLastLogIndex() + 1);
    state = State::LEADER;
    leaderId = serverId;
    printElectionState();
    // 1. leader节点无条件永久禁止发起新选举以及接受其他节点的选举拉票（无论term值任何）
    startElectionAt = TimePoint::max();
    withholdVotesUntil = TimePoint::max();

    // Our local cluster time clock has been ticking ever since we got the last
    // log entry/snapshot. Set the clock back to when that happened, since we
    // don't really want to count that time (the cluster probably had no leader
    // for most of it).
    // 2. 重置clusterClock的计时，排除集群未选出leader期间的计时消耗。
    clusterClock.newEpoch(clusterClock.clusterTimeAtEpoch);

    // The ordering is pretty important here: First set nextIndex and
    // matchIndex for ourselves and each follower, then append the no op.
    // Otherwise we'll set our localServer's last agree index too high.
    // 3. 重置peer server的matchIndex、nextIndex等变量，以及local server的lastSyncedIndex。
    configuration->forEach(&Server::beginLeadership);

    // Append a new entry so that commitment is not delayed indefinitely.
    // Otherwise, if the leader never gets anything to append, it will never
    // return to read-only operations (it can't prove that its committed index
    // is up-to-date).
    // ！！！！
    // 4. 往log内append一条当前term的no-op的log，然后再以本地log为权威复制到多数派，该操作可以将旧term下已经存在于log中但是还未commit过的log一起commit了，
    //    而且不需要担心commit的log丢失，如果没有这一条的话，考虑一种时序：
    //    节点A是term 2的leader，然后他append一条log到本地之后未commit就和集群断联，然后集群选出节点B成为term 3的leader，节点B也append一条log
    //    到本地之后未commit就和集群断联，这时候节点A正常并且发起选举并赢得了term 4的leader，这时候节点A会以自己本地log为权威复制到多数派并提交，即
    //    之前那条term 2的log也会被commit，然后节点A再次断联，节点B正常后凭借自己本地的最新term 3 log赢得了新选举term 5的的leader，这时候节点B就会
    //    以自己本地log为基准覆盖掉之前节点A的committed term 2 log，导致committed log丢失。
    //    但是如果之前节点A在提交term 2 log的时候是先append了term 4 log然后再顺便一起提交term 2 log的话，节点B后来就不会赢得term 5的leader，因为他
    //    本地的term 3 log比多数派的term 4 log要旧，那么最后选出来的term 5 leader必然是包含了节点A提交的term 4 log的，也自然包含了term 2 log，也就不会导致committed log丢失。
    Log::Entry entry;
    entry.set_term(currentTerm);
    entry.set_type(Protocol::Raft::EntryType::NOOP);
    entry.set_cluster_time(clusterClock.leaderStamp());
    // 5. append到本地log之后，唤醒leaderDiskThreadMain进行本地持久化、唤醒peer线程进行复制到多数派，最后会推进commitIndex到currentTerm的no-op log
    append({&entry});

    // Outstanding RequestVote RPCs are no longer needed.
    // 6. 可能有些peer线程还在等待之前本机candidate阶段时发出的RequestVote request的rpc response，现在已经不需要了，直接interrupt了接着执行appendEntries逻辑
    interruptAll();
}

// 用于截断本地raft log中和snapshot重复了的前缀部分，同时重新设置configuration
void
RaftConsensus::discardUnneededEntries()
{
    if (log->getLogStartIndex() <= lastSnapshotIndex) {
        // 当前的segment文件的log区间和snapshot的有重复，将重复的部分截断删除
        NOTICE("Removing log entries through %lu (inclusive) since "
               "they're no longer needed", lastSnapshotIndex);
        log->truncatePrefix(lastSnapshotIndex + 1);
        configurationManager->truncatePrefix(lastSnapshotIndex + 1);
        // leader节点可能走到这里，所以需要调用stateChanged.notify一下。
        stateChanged.notify_all();
        if (state == State::LEADER) { // defer log sync
            logSyncQueued = true;
        } else { // sync log now
            std::unique_ptr<Log::Sync> sync = log->takeSync();
            sync->wait();
            log->syncComplete(std::move(sync));
        }
    }
}

uint64_t
RaftConsensus::getLastLogTerm() const
{
    uint64_t lastLogIndex = log->getLastLogIndex();
    if (lastLogIndex >= log->getLogStartIndex()) {
        return log->getEntry(lastLogIndex).term();
    } else {
        assert(lastLogIndex == lastSnapshotIndex); // potentially 0
        return lastSnapshotTerm;
    }
}

// 该方法通常在本机所认的leader发生了变化的时候调用，例如stepdown（我不当了）、startElection（我要当）、becomeLeader（我当了）、exiting（我退出了）
// 这个函数主要有两个目的：
//   1. 唤醒所有wait在stateChanged信号量上的peer线程，通知其起来执行任务
//   2. 唤醒所有wait在rpc response ready信号量上的peer线程，cancel正在进行的rpc，之后去执行其他任务
void
RaftConsensus::interruptAll()
{
    stateChanged.notify_all();
    // A configuration is sometimes missing for unit tests.
    if (configuration)
        // 唤醒所有还在rpc response ready信号量上等待的peer线程，直接cancel rpc不关心此次rpc结果了。
        configuration->forEach(&Server::interrupt);
}

// 在保持request.ByteSize不大于SOFT_RPC_SIZE_LIMIT的情况下，尽可能多的将entry log加入request中。
uint64_t
RaftConsensus::packEntries(
        uint64_t nextIndex,
        Protocol::Raft::AppendEntries::Request& request) const
{
    // Add as many as entries as will fit comfortably in the request. It's
    // easiest to add one entry at a time until the RPC gets too big, then back
    // the last one out.

    // Calculating the size of the request ProtoBuf is a bit expensive, so this
    // estimates high, then if it reaches the size limit, corrects the estimate
    // and keeps going. This is a dumb algorithm but does well enough. It gets
    // the number of calls to request.ByteSize() down to about 15 even with
    // extremely small entries (10 bytes of payload data in each of 50,000
    // entries filling to a 1MB max).

    // Processing 19000 entries here with 10 bytes of data each (total request
    // size of 1MB) still takes about 42 milliseconds on an overloaded laptop
    // when compiling in DEBUG mode. That's a bit slow, in case someone has
    // aggressive election timeouts. As a result, the total number of entries
    // in a request is now limited to MAX_LOG_ENTRIES_PER_REQUEST=5000, which
    // amortizes RPC overhead well enough anyhow. This limit will only kick in
    // when the entry size drops below 200 bytes, since 1M/5K=200.

    using Core::Util::downCast;
    // 限制每次request最多包含MAX_LOG_ENTRIES_PER_REQUEST个log entry
    uint64_t lastIndex = std::min(log->getLastLogIndex(),
                                  nextIndex + MAX_LOG_ENTRIES_PER_REQUEST - 1);
    google::protobuf::RepeatedPtrField<Protocol::Raft::Entry>& requestEntries =
        *request.mutable_entries();

    uint64_t numEntries = 0;
    uint64_t currentSize = downCast<uint64_t>(request.ByteSize());

    for (uint64_t index = nextIndex; index <= lastIndex; ++index) {
        const Log::Entry& entry = log->getEntry(index);
        *requestEntries.Add() = entry;

        // Each member of a repeated message field is encoded with a tag
        // and a length. We conservatively assume the tag and length will
        // be up to 10 bytes each (2^64), though in practice the tag is
        // probably one byte and the length is probably two.
        // 由于无法单独获得一个entry在加入request序列化之后的精确byte size增量，为了避免每次都
        // 调用request.ByteSize进行O(n)计算，这里直接进行宽松增量估算
        currentSize += uint64_t(entry.ByteSize()) + 20;

        if (currentSize >= SOFT_RPC_SIZE_LIMIT) {
            // The message might be too big: calculate more exact but more
            // expensive size.
            // 估算总byte size超过了上限，调用一次O(n)操作计算精确总byte size，
            // 并将currentSize纠正为精确值。
            uint64_t actualSize = downCast<uint64_t>(request.ByteSize());
            assert(currentSize >= actualSize);
            currentSize = actualSize;
            if (currentSize >= SOFT_RPC_SIZE_LIMIT && numEntries > 0) {
                // This entry doesn't fit and we've already got some
                // entries to send: discard this one and stop adding more.
                // 纠正为精确值后的总byte size仍大于上限，在保证request内至少有一个entry log的情况下，直接抛弃最后一个entry后退出。
                requestEntries.RemoveLast();
                break;
            }
        }
        // This entry fit, so we'll send it.
        ++numEntries;
    }

    assert(numEntries == uint64_t(requestEntries.size()));
    return numEntries;
}

// 该方法主要是：
//   1. 解析本机最新snapshot文件的header信息
//   2. 对log文件进行截断以适应最新snapshot
//   3. 对configuration进行重新设置以适应最新snapshot
// 所以该方法并不会读取snapshot的真正数据内容，后续getNextEntry时再真正读取
void
RaftConsensus::readSnapshot()
{
    std::unique_ptr<Storage::SnapshotFile::Reader> reader;
    if (storageLayout.serverDir.fd != -1) {
        try {
            reader.reset(new Storage::SnapshotFile::Reader(storageLayout));
        } catch (const std::runtime_error& e) { // file not found
            // 1. 这种情况对应snapshot文件不存在
            NOTICE("%s", e.what());
        }
    }
    if (reader) {
        // Check that this snapshot uses format version 1
        uint8_t version = 0;
        uint64_t bytesRead = reader->readRaw(&version, sizeof(version));
        if (bytesRead < 1) {
            // 2. 这种情况对应snapshot文件存在但是size为空，直接PANIC
            PANIC("Found completely empty snapshot file (it doesn't even "
                  "have a version field)");
        } else {
            if (version != 1) {
                // 3. 这种情况对应version不为1，当前代码无法解析snapshot格式，直接PANIC
                PANIC("Snapshot format version read was %u, but this code can "
                      "only read version 1",
                      version);
            }
        }

        // load header contents
        SnapshotMetadata::Header header;
        std::string error = reader->readMessage(header);
        if (!error.empty()) {
            // 4. 这种情况对应header解析失败，基本是因为snapshot文件损坏了，直接PANIC
            PANIC("Couldn't read snapshot header: %s", error.c_str());
        }
        if (header.last_included_index() < lastSnapshotIndex) {
            // 5. 这种情况对应当前读取的snapshot比上一次读取的覆盖范围还小，直接PANIC
            PANIC("Trying to load a snapshot that is more stale than one this "
                  "server loaded earlier. The earlier snapshot covers through "
                  "log index %lu (inclusive); this one covers through log "
                  "index %lu (inclusive)",
                  lastSnapshotIndex,
                  header.last_included_index());

        }
        lastSnapshotIndex = header.last_included_index();
        lastSnapshotTerm = header.last_included_term();
        lastSnapshotClusterTime = header.last_cluster_time();
        lastSnapshotBytes = reader->getSizeBytes();
        // ！！！
        // 一个server节点在崩溃恢复时，在与leader节点取得联系之前，本地commitIndex的确认只能通过snapshot文件来获取
        //（注意不是本地raft log，因为存在于本地raft log并不意味着存在于多数派，只有存在于多数派的才能算作commit，而
        // snapshot中包含的必然都是commit了的），然后后续在获取到leader的心跳之后，再基于这个commitIndex追赶Leader的commit进度，
        // 所以本地及时打snapshot非常重要，他直接影响崩溃恢复后的commit追赶成本。
        commitIndex = std::max(lastSnapshotIndex, commitIndex);

        NOTICE("Reading snapshot which covers log entries 1 through %lu "
               "(inclusive)", lastSnapshotIndex);

        // We should keep log entries if they might be needed for a quorum. So:
        // 1. Discard log if it is shorter than the snapshot.
        // 2. Discard log if its lastSnapshotIndex entry disagrees with the
        //    lastSnapshotTerm.
        // 符合以下if条件的直接将log抛弃。
        if (log->getLastLogIndex() < lastSnapshotIndex ||
           // 这个判断基于一个事实：如果两个日志中的两个 entry 有相同的 index 和相同的 term，那么这两个 entry 存储的是同一条命令，且它们之前的所有 entry 也完全相同。
           // 因此如果log中的这个index的term和snapshot中的不一致，说明该index后续的log全部都坏了，而最后一个完好的log entry的index必然对应到snapshot内的某个地方，但是
           // 已经没有必要也无法（leader无法再提供，只能提供snapshot）再找到这个精确index了，只需要直接以snapshot为基准重新记录log即可。
            (log->getLogStartIndex() <= lastSnapshotIndex &&
             log->getEntry(lastSnapshotIndex).term() != lastSnapshotTerm)) {
            // The NOTICE message can be confusing if the log is empty, so
            // don't print it in that case. We still want to shift the log
            // start index, though.
            if (log->getLogStartIndex() <= log->getLastLogIndex()) {
                NOTICE("Discarding the entire log, since it's not known to be "
                       "consistent with the snapshot that is being read");
            }
            // Discard the entire log, setting the log start to point to the
            // right place.
            // 删除所有现有的segment文件，然后重新openNewSegment创建新的openSegment用于接收log写入
            log->truncatePrefix(lastSnapshotIndex + 1);
            log->truncateSuffix(lastSnapshotIndex);
            // 重置confuguration
            configurationManager->truncatePrefix(lastSnapshotIndex + 1);
            configurationManager->truncateSuffix(lastSnapshotIndex);
            // Clean up resources.
            if (state == State::LEADER) { // defer log sync
                // 事实上不会进入到这里，在这段代码前没有调用stateChanged.notify也能看出
                logSyncQueued = true;
            } else { // sync log now
                std::unique_ptr<Log::Sync> sync = log->takeSync();
                sync->wait();
                log->syncComplete(std::move(sync));
            }
            // log被完全抛弃，clusterClock被重置为snapshot的time
            clusterClock.newEpoch(lastSnapshotClusterTime);
        }

        // 截断本地raft log中和snapshot重复了的前缀部分，同时重新设置configuration。
        discardUnneededEntries();

        if (header.has_configuration_index() && header.has_configuration()) {
            // 将snapshot中的configuration设置进ConfigurationManager，然后重新设置configuration。
            configurationManager->setSnapshot(header.configuration_index(),
                                              header.configuration());
        } else {
            WARNING("No configuration. This is unexpected, since any snapshot "
                    "should contain a configuration (they're the first thing "
                    "found in any log).");
        }

        // commitIndex可能发生了变化，这里notify主要是为了唤醒StateMachine::applyThreadMain()后台线程推进apply
        stateChanged.notify_all();
    }
    // 有两种情况有进入以下if分支：
    //   1. 本机snapshot已经生成过但是丢失或者损坏了，导致无法解析
    //   2. 本机snapshot虽然存在且能够正常解析，但是由于太旧了，导致snapshot和log之间存在空隙没被覆盖。
    // 理论上正常的snapshot在经过上面的解析之后，下面的if值应该是==，但是由于<情况下并不破坏正确性，所以只对真正致命的>做PANIC。
    if (log->getLogStartIndex() > lastSnapshotIndex + 1) {
        PANIC("The newest snapshot on this server covers up through log index "
              "%lu (inclusive), but its log starts at index %lu. This "
              "should never happen and indicates a corrupt disk state. If you "
              "want this server to participate in your cluster, you should "
              "back up all of its state, delete it, and add the server back "
              "as a new cluster member using the reconfiguration mechanism.",
              lastSnapshotIndex, log->getLogStartIndex());
    }

    // 保存snapshot reader提供给后续getNextEntry操作真正读取snapshot的内容。
    snapshotReader = std::move(reader);
}

std::pair<RaftConsensus::ClientResult, uint64_t>
RaftConsensus::replicateEntry(Log::Entry& entry,
                              std::unique_lock<Mutex>& lockGuard)
{
    if (state == State::LEADER) {
        entry.set_term(currentTerm);
        entry.set_cluster_time(clusterClock.leaderStamp());
        append({&entry});
        uint64_t index = log->getLastLogIndex();
        // ！！！
        // 这里会存在一个当前write操作的log已经commit但是还是会返回Not Leader的特例：
        // 当前正在执行集群的配置变更，leader作为新集群的临时leader把当前write操作的log连同stable state configuration log一起复制到新集群多数派，
        // 在当前leader节点advanceCommitIndex的时候，会把当前write操作的log也一起commit，但是leader节点如果不在新配置中，leader会马上
        // 主动stepdown(currentTerm+1)，随后当前线程醒来的时候会发现term发生变化而返回Not Leader，这在raft中是允许的，因为Not Leader
        // 本身就意味着操作结果不确定。
        while (!exiting && currentTerm == entry.term()) {
            if (commitIndex >= index) {
                VERBOSE("replicate succeeded");
                return {ClientResult::SUCCESS, index};
            }
            // 释放lock后再进入睡眠
            // !!!
            // 如果当前节点在集群中已经失去leader身份，他的写复制将一直拿不到多数派接受，commitIndex将无法被推进。
            // 最后stepdownThreadMain后台线程也将由于ackEpoch拿不到多数派接受而将当前节点stepdown(currentTerm+1)，stepdown时
            // 会执行interruptAll，该线程也会被唤醒，结果就是while循环由于term变化而退出，最后返回NOT_LEADER。
            stateChanged.wait(lockGuard);
        }
    }
    return {ClientResult::NOT_LEADER, 0};
}

// candidate节点使用该方法向每个peer节点拉票。
void
RaftConsensus::requestVote(std::unique_lock<Mutex>& lockGuard, Peer& peer)
{
    Protocol::Raft::RequestVote::Request request;
    request.set_server_id(serverId);
    // 1. 带上需要拉票的term，该值在此前通过startNewElection方法中单增产生。
    request.set_term(currentTerm);
    // 2. 带上本地raft log的最后term和index，用于和peer节点比较谁的日志更新，作为对方是否投票的依据
    request.set_last_log_term(getLastLogTerm());
    request.set_last_log_index(log->getLastLogIndex());

    Protocol::Raft::RequestVote::Response response;
    VERBOSE("requestVote start");
    TimePoint start = Clock::now();
    uint64_t epoch = currentEpoch;
    Peer::CallStatus status = peer.callRPC(
                Protocol::Raft::OpCode::REQUEST_VOTE,
                request, response,
                lockGuard);
    VERBOSE("requestVote done");
    switch (status) {
        case Peer::CallStatus::OK:
            break;
        case Peer::CallStatus::FAILED:
            // 3. rpc failed并不会导致此次选举终止。
            peer.suppressBulkData = true;
            peer.backoffUntil = start + RPC_FAILURE_BACKOFF;
            return;
        case Peer::CallStatus::INVALID_REQUEST:
            PANIC("The server's RaftService doesn't support the RequestVote "
                  "RPC or claims the request is malformed");
    }

    // 4. rpc等待期间放锁，所以rpc回来之后需要检查term和state是否还是之前的值
    if (currentTerm != request.term() || state != State::CANDIDATE ||
        peer.exiting) {
        VERBOSE("ignore RPC result");
        // we don't care about result of RPC
        return;
    }

    if (response.term() > currentTerm) {
        // 5. 如果对方的term比本机当前拉票的term还要大，无条件stepDown为Follower，终止本轮选举流程。
        //    但是选举计时器不会重置，一旦新选举超时前对应term的leader没有heartbeat过来，将重新触发选举。
        NOTICE("Received RequestVote response from server %lu in "
               "term %lu (this server's term was %lu)",
                peer.serverId, response.term(), currentTerm);
        stepDown(response.term());
    } else {
        // 6. 本机的term确实比对方更新，对该peer的拉票结束，检查拉票是否成功
        peer.requestVoteDone = true;
        peer.lastAckEpoch = epoch;
        stateChanged.notify_all();

        if (response.granted()) {
            // 7. 本机在该term下对该peer拉票成功，说明本机的log >= peer的log。
            peer.haveVote_ = true;
            NOTICE("Got vote from server %lu for term %lu",
                   peer.serverId, currentTerm);
            // 8. 检测该term下是否获得了多数派选票，是则成为leader，否则继续等待其他选票到达或者下一轮选举被超时触发
            if (configuration->quorumAll(&Server::haveVote))
                becomeLeader();
        } else {
            // 9. 本机在该term下对该peer拉票失败了，说明本机的log < peer的log。
            //    但是仍然不终止本轮选举，继续看其他选票到达情况。
            NOTICE("Vote denied by server %lu for term %lu",
                   peer.serverId, currentTerm);
        }
    }
}

// 重置本机的重新选举触发时间。
// ！！！
// 需要注意的是，选举等待时间并不是固定ELECTION_TIMEOUT，而是一个区间内的随机值（随机抖动机制）。
// 由于每个follower在触发重新选举成为candidate后都是首先投自己，如果多个follower同时
// 成为candidate，将导致split vote选举冲突，结果是没人能拿到多数派，最后所有server不得不再次等待一段时间
// 再重新选举。而引入选举等待时间的随机抖动机制则可以错开每个follower成为candidate的时间，减少
// 选举冲突，更快的选出新leader。
void
RaftConsensus::setElectionTimer()
{
    std::chrono::nanoseconds duration(
        Core::Random::randomRange(
            uint64_t(std::chrono::nanoseconds(ELECTION_TIMEOUT).count()),
            uint64_t(std::chrono::nanoseconds(ELECTION_TIMEOUT).count()) * 2));
    VERBOSE("Will become candidate in %s",
            Core::StringUtil::toString(duration).c_str());
    startElectionAt = Clock::now() + duration;
    // 唤醒timerThreadMain后台线程，重置wait_until等待时间。
    stateChanged.notify_all();
}

void
RaftConsensus::printElectionState() const
{
    const char* s = NULL;
    switch (state) {
        case State::FOLLOWER:
            s = "FOLLOWER, ";
            break;
        case State::CANDIDATE:
            s = "CANDIDATE,";
            break;
        case State::LEADER:
            s = "LEADER,   ";
            break;
    }
    NOTICE("server=%lu, term=%lu, state=%s leader=%lu, vote=%lu",
           serverId,
           currentTerm,
           s,
           leaderId,
           votedFor);
}

// 本机触发新选举的入口方法（本机想成为新leader），有三种情况会触发：
// 1. 现有leader超过一段时间没有发送heartbeat给本机，导致timerThreadMain后台线程被超时唤醒，触发选举。
// 2. 本机已经处于candidate选举状态，但是超过一段时间没有集齐多数派投票无法成为leader，导致timerThreadMain后台线程被超时唤醒，触发重新选举。
// 3. 本机崩溃重启/收到其他server的更高term，本机stepdown成Follower了，但是超过一段时间没有正确的leader找过来（headerbeat过来），
//    一直不知道leader是谁，导致timerThreadMain后台线程被超时唤醒，触发选举。
void
RaftConsensus::startNewElection()
{
    if (configuration->id == 0) {
        // Don't have a configuration: go back to sleep.
        // 1. 本地没有集群的membership配置，不知道该向谁拉票，不申请选举
        setElectionTimer();
        return;
    }

    // ！！！
    // 注意，这里并不会约束未提交configuration，也就是说即使本机不在当前configuration内，但是只要当前的configuration未提交，
    // 本机就应该被允许发起选举。这个主要是出于一个情况：
    // A作为旧配置leader和新配置的临时leader（新配置不包含A），此时配置变更流程A已经将stable configuration追加到了自己本地，
    // 但是还没将stable configuration复制到任何一台新配置节点。突然A断线重连，此时A基于新配置重新发起选举向新配置节点拉票应该是合理的，
    // 因为A试图继续成为新配置集群的临时leader，能够继续执行未完成的配置变更流程。当然，即使A不重新成为历史leader，新旧配置集群节点
    // 同样可以根据联合共识选出新leader。
    if (commitIndex >= configuration->id &&
        !configuration->hasVote(configuration->localServer)) {
        // we are not in the latest configuration, do not start an election
        // 2. 本机不在已提交的集群正式membership配置中，可能是已经被移出集群了，不申请选举
        setElectionTimer();
        return;
    }

    if (leaderId > 0) {
        NOTICE("Running for election in term %lu "
               "(haven't heard from leader %lu lately)",
               currentTerm + 1,
               leaderId);
    } else if (state == State::CANDIDATE) {
        NOTICE("Running for election in term %lu "
               "(previous candidacy for term %lu timed out)",
               currentTerm + 1,
               currentTerm);
    } else {
        NOTICE("Running for election in term %lu",
               currentTerm + 1);
    }
    // 3. 无论是否重复发起选举，每次新选举必然开新term
    //    ！！！因此如果本机由于被集群隔离，长期处于candidate状态反复发起新选举，
    //    ！！！那本机的term将会变得很大，这种大term不对应任何leader，但却是raft
    //    ！！！协议下合法的。
    // JOEY_TODO: 做Pre-Vote
    ++currentTerm;
    // 4. 处于candidate拉票状态
    state = State::CANDIDATE;
    // 5. 在已开的新term中还不知道leader是谁
    leaderId = 0;
    // 6. 新开的term首先投给自己
    votedFor = serverId;
    printElectionState();
    // 7. 重置选举计时器，如果此次选举后续没有获取多数派选票无法成为leader，超时后会再次触发新选举
    setElectionTimer();
    // 8. 重置peer给本机的投票信息，之前的拉票信息全部作废
    configuration->forEach(&Server::beginRequestVote);
    // 9. 已经不认之前的leader了，抛弃正在从leader接收的snapshot，删除正在写的partial snaposhot
    if (snapshotWriter) {
        snapshotWriter->discard();
        snapshotWriter.reset();
    }
    // 10. 在peer真正发送requestVote请求前，必须将新开的term以及votefor信息持久化。避免出现其他peer已经给我选票但是metadata没持久化时崩溃，
    //     自己崩溃恢复后丢失了选举信息。
    updateLogMetadata();
    // 11. 这个操作会唤醒所有peer线程就位（等RaftConsensus mutex）等待发送requestVote，正在进行的rpc会被cancel，包括上一次触发选举时还在等待的requestVote请求。
    interruptAll();

    // if we're the only server, this election is already done
    // 12. 两种情况：
    //  1) 如果配置中有其他peer存在，所有这些peer的haveVote必然为false，因为刚刚beginRequestVote时重置了所有peer的投票信息，
    //     而当前线程一直持有RaftConsensus mutex，peer线程必不可能发送得了requestVote，因此haveVote截至此时必然false。
    //  2) 如果目前已知是单节点集群，quorumAll自动满足，本机直接成为leader即可。
    if (configuration->quorumAll(&Server::haveVote))
        becomeLeader();
}

// 该方法用于节点将本机状态转换成Follower状态，主要有五种情况：
//  1）节点刚启动进行RaftConsensus::init时，需要调用stepDown(currentTerm)加入Follower状态，后续才能发起新选举
//  2）任意状态节点从leader节点request处获得不小于currentTerm的term，无论该leader还是不是事实上的leader
//  3）follower/candidate节点从candidate节点request处获得比currentTerm更高的term
//  4）leader节点从其他server节点response处获得比currentTerm更高的newTerm，无论该newTerm是否对应一个合法leader，都调用stepdown(newTerm)
//  5）leader节点定时心跳quorum失败/leader节点发现新的committed stable configuration不再包含本机，也调用stepdown(currentTerm+1)
void
RaftConsensus::stepDown(uint64_t newTerm)
{
    // 1. 只有当newTerm不小于currentTerm时才允许执行stepDown
    assert(currentTerm <= newTerm);
    if (currentTerm < newTerm) {
        // 2. newTerm比currentTerm要大，currentTerm变更为更大的newTerm
        VERBOSE("stepDown(%lu)", newTerm);
        currentTerm = newTerm;
        // 3. 由于在更大的newTerm下，本机还不知道leader是谁（甚至可能还没有leader），也没有在newTerm下投过票，所以需要重置leaderId和votedFor
        leaderId = 0;
        votedFor = 0;
        // 4. term和votedFor发生变化时必须马上同步持久化metadata
        updateLogMetadata();
        // 5. 如果leader节点有正在进行的staging configuration变更状态，直接放弃
        configuration->resetStagingServers();
        // 6. 如果follower节点有正在进行的snaoshit接收任务，直接放弃
        if (snapshotWriter) {
            snapshotWriter->discard();
            snapshotWriter.reset();
        }
        state = State::FOLLOWER;
        printElectionState();
    } else {
        // 7. newTerm和currentTerm是一样的，而leader节点不会stepDown和currentTerm相同的term，所以对于follower/candidate节点，
        //    原来的term下所记录的leaderId、votedFor、正在接收的snaoshot流程都不应该发生变化。
        if (state != State::FOLLOWER) {
            state = State::FOLLOWER;
            printElectionState();
        }
    }
    // 8. 本机切换至Follower状态后，需要保证选举计时器能够正常工作，且能够正常接收其他server的拉票
    if (startElectionAt == TimePoint::max()) // was leader
        setElectionTimer();
    if (withholdVotesUntil == TimePoint::max()) // was leader
        withholdVotesUntil = TimePoint::min();
    // 9. 本机状态已经发生变化，原来的rpc任务已经没有意义，cancel所有进行中的rpc request流程，唤醒所有peer线程重新进入新的Follower状态执行逻辑
    interruptAll();

    // ！！！
    // 10. 还有一个很关键的点，就是raft需要保证Follower节点本地所有的append log都是已经fsync持久化了的。否则一旦后续该节点再次发起选举，可能会基于一个未持久化的log
    //     去竞争选票。因此在leader stepDown为follower节点时，还需要同时将所有已经append log进行持久化。
    // If the leader disk thread is currently writing to disk, wait for it to
    // finish. We poll here because we don't want to release the lock (this
    // server would then believe its writes have been flushed when they
    // haven't).
    //     1）首先轮询等待正在执行fsync任务的leaderDiskThreadMain线程把任务完成，因为我们不能在stepDown操作完整执行完之前释放mutex，所以这里只能持锁轮询等待。
    while (leaderDiskThreadWorking)
        usleep(500);

    // If a recent append has been queued, empty it here. Do this after waiting
    // for leaderDiskThread to preserve FIFO ordering of Log::Sync objects.
    // Don't bother updating the localServer's lastSyncedIndex, since it
    // doesn't matter for non-leaders.
    //     2）如果leader此前刚刚append log之后马上就stepDown了，leaderDoskThreadMain没来得及取该sync进行操作，这里需要立即同步将其执行。在leaderDiskThreadMain
    //        执行完后我们再取sync执行，是为了维护文件操作的FIFO语义，先产生的文件操作必须先执行。
    if (logSyncQueued) {
        std::unique_ptr<Log::Sync> sync = log->takeSync();
        sync->wait();
        log->syncComplete(std::move(sync));
        logSyncQueued = false;
    }
}

void
RaftConsensus::updateLogMetadata()
{
    // ！！！
    // metadata需要对term和votefor持久化，是为了标记当前term下本机投票给了谁，
    // 主要是防止在本机崩溃恢复后重复投票。
    log->metadata.set_current_term(currentTerm);
    log->metadata.set_voted_for(votedFor);
    VERBOSE("updateMetadata start");
    log->updateMetadata();
    VERBOSE("updateMetadata end");
}

// ！！！
// 该方法是Server端应对Client端statemachine read-only请求的ReadIndex机制的核心实现方法，
// 在Leader节点接收到client端的read-only请求时，Leader节点需要先通过该方法开新epoch
// 然后检测ackEpoch（由心跳成功回复后更新，机制上和stepDownThreadMain一样），以确保自己的Leader身份还被多数派认可。（该方法不负责发心跳）
// JOEY_TODO: 可以实现lease租约，从而加速read-only请求。
bool
RaftConsensus::upToDateLeader(std::unique_lock<Mutex>& lockGuard) const
{
    // !!!
    // read-only请求由于不涉及raft log的append，所以leader在进行quorum多数派检查时没办法像read-write操作一样检查peer server的matchIndex。
    // 在ReadIndex机制下，通过currentEpoch这个值实现这个检查功能。
    // read-only请求在schedule hearbeat前，先++currentEpoch开一个新的epoch，随后发送的peer rpc
    // (包括appendEntries、installSnapshot、requestVote所有这些会与peer进行request-response沟通的rpc)如果获得了follower的认可leader
    // acknowledge response，就会更新peer的laskAckEpoch为该epoch值，表示在该epoch内，该peer认可当前leader。
    // 这里其实涉及一个时序问题：
    // timeLine :---------------------------------------------------------------------------------------------------------------------------------------->
    // Leader   ： read start    epoch+1    send heartbeat     quorum ok                                                                       read end
    // Followers:                                                          start vote    new leader write start        new leader write end
    // 这时候旧leader的read没有包括新leader的write，但是由于旧leader的read是开始于新leader的write之前的，
    // 所以事实上read的线性化点可以是read start时刻，这种情况下read不包括新leader write就还是符合线性强一致的，
    // 比如单机对atomic进行load和fetch_add操作，即使两者同时进行，操作系统也会给他们有一个顺序，这种情况下read包括不包括write都是合理的。
    ++currentEpoch;
    uint64_t epoch = currentEpoch;
    // schedule a heartbeat now so that this returns quickly
    // 不同于stepDownThreadMain的周期性检测，本方法出于及时回应client请求的考虑，会提前触发心跳。
    configuration->forEach(&Server::scheduleHeartbeat);
    stateChanged.notify_all();
    while (true) {
        // 这里不需要再检查term是否和之前开epoch时的term一致，我们关心的是：当前节点认定自己是leader的前提下，当前节点是否真的含有足够新的数据提供给read。
        if (exiting || state != State::LEADER)
            return false;
        if (configuration->quorumMin(&Server::getLastAckEpoch) >= epoch) {
            // So we know we're the current leader, but do we have an
            // up-to-date commitIndex yet? What we'd like to check is whether
            // the entry's term at commitIndex matches our currentTerm, but
            // snapshots mean that we may not have the entry in our log. Since
            // commitIndex >= lastSnapshotIndex, we split into two cases:
            // 这里其实甚至都不需要此刻当前节点就是集群真正的leader，我们只需要保证多数派的quorum ok是发生在read start之后，就可以确定在read start的后续某个时刻，
            // 当前节点确实被多数派认定为了leader，即使在那次quorum ok之后当前节点又丢失了leader身份，也无所谓，只要我们能够确保当前节点在此时的commitTerm已经达到了
            // 此前那次quorum ok时的leader term就OK了，就可以确保当前节点的log必然是包括了read start之前的所有已提交write。
            // 由于此前quorum ok时精确时刻的leader term无法获取，但是鉴于term是集群全局单增的，所以直接保守判断当前的commitTerm==currentTerm，由于currentTerm必然>=此前
            // quorum ok时的leader term，只要commitTerm==currentTerm，就可以。
            uint64_t commitTerm;
            if (commitIndex == lastSnapshotIndex) {
                commitTerm = lastSnapshotTerm;
            } else {
                assert(commitIndex > lastSnapshotIndex);
                assert(commitIndex >= log->getLogStartIndex());
                assert(commitIndex <= log->getLastLogIndex());
                commitTerm = log->getEntry(commitIndex).term();
            }
            if (commitTerm == currentTerm)
                return true;
        }
        stateChanged.wait(lockGuard);
    }
}

std::ostream&
operator<<(std::ostream& os, RaftConsensus::ClientResult clientResult)
{
    typedef RaftConsensus::ClientResult ClientResult;
    switch (clientResult) {
        case ClientResult::SUCCESS:
            os << "ClientResult::SUCCESS";
            break;
        case ClientResult::FAIL:
            os << "ClientResult::FAIL";
            break;
        case ClientResult::RETRY:
            os << "ClientResult::RETRY";
            break;
        case ClientResult::NOT_LEADER:
            os << "ClientResult::NOT_LEADER";
            break;
    }
    return os;
}

std::ostream&
operator<<(std::ostream& os, RaftConsensus::State state)
{
    typedef RaftConsensus::State State;
    switch (state) {
        case State::FOLLOWER:
            os << "State::FOLLOWER";
            break;
        case State::CANDIDATE:
            os << "State::CANDIDATE";
            break;
        case State::LEADER:
            os << "State::LEADER";
            break;
    }
    return os;
}

} // namespace LogCabin::Server
} // namespace LogCabin

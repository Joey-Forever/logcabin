/* Copyright (c) 2012 Stanford University
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

#include <string.h>

#include "build/Protocol/Client.pb.h"
#include "Core/Buffer.h"
#include "Core/ProtoBuf.h"
#include "Core/Time.h"
#include "RPC/ServerRPC.h"
#include "Server/RaftConsensus.h"
#include "Server/ClientService.h"
#include "Server/Globals.h"
#include "Server/StateMachine.h"

namespace LogCabin {
namespace Server {

typedef RaftConsensus::ClientResult Result;

ClientService::ClientService(Globals& globals)
    : globals(globals)
{
}

ClientService::~ClientService()
{
}

void
ClientService::handleRPC(RPC::ServerRPC rpc)
{
    using Protocol::Client::OpCode;

    // Call the appropriate RPC handler based on the request's opCode.
    switch (rpc.getOpCode()) {
        case OpCode::GET_SERVER_INFO:
            getServerInfo(std::move(rpc));
            break;
        case OpCode::VERIFY_RECIPIENT:
            verifyRecipient(std::move(rpc));
            break;
        case OpCode::GET_CONFIGURATION:
            getConfiguration(std::move(rpc));
            break;
        case OpCode::SET_CONFIGURATION:
            setConfiguration(std::move(rpc));
            break;
        case OpCode::STATE_MACHINE_COMMAND:
            stateMachineCommand(std::move(rpc));
            break;
        case OpCode::STATE_MACHINE_QUERY:
            stateMachineQuery(std::move(rpc));
            break;
        default:
            WARNING("Received RPC request with unknown opcode %u: "
                    "rejecting it as invalid request",
                    rpc.getOpCode());
            rpc.rejectInvalidRequest();
    }
}

std::string
ClientService::getName() const
{
    return "ClientService";
}


/**
 * Place this at the top of each RPC handler. Afterwards, 'request' will refer
 * to the protocol buffer for the request with all required fields set.
 * 'response' will be an empty protocol buffer for you to fill in the response.
 */
#define PRELUDE(rpcClass) \
    Protocol::Client::rpcClass::Request request; \
    Protocol::Client::rpcClass::Response response; \
    if (!rpc.getRequest(request)) \
        return;

////////// RPC handlers //////////


void
ClientService::getServerInfo(RPC::ServerRPC rpc)
{
    PRELUDE(GetServerInfo);
    Protocol::Client::Server& info = *response.mutable_server_info();
    info.set_server_id(globals.raft->serverId);
    info.set_addresses(globals.raft->serverAddresses);
    rpc.reply(response);
}

// leader节点处理client端的read集群当前membership配置的request，client在配置变更之前都需要先getConfiguration，
// 使用CAS的方式，保证基于最新的集群configuration进行配置变更。
void
ClientService::getConfiguration(RPC::ServerRPC rpc)
{
    PRELUDE(GetConfiguration);
    Protocol::Raft::SimpleConfiguration configuration;
    uint64_t id;
    Result result = globals.raft->getConfiguration(configuration, id);
    if (result == Result::RETRY || result == Result::NOT_LEADER) {
        Protocol::Client::Error error;
        error.set_error_code(Protocol::Client::Error::NOT_LEADER);
        std::string leaderHint = globals.raft->getLeaderHint();
        if (!leaderHint.empty())
            error.set_leader_hint(leaderHint);
        rpc.returnError(error);
        return;
    }
    response.set_id(id);
    for (auto it = configuration.servers().begin();
         it != configuration.servers().end();
         ++it) {
        Protocol::Client::Server* server = response.add_servers();
        server->set_server_id(it->server_id());
        server->set_addresses(it->addresses());
    }
    rpc.reply(response);
}

// leader节点处理client端的集群membership配置变更request
void
ClientService::setConfiguration(RPC::ServerRPC rpc)
{
    PRELUDE(SetConfiguration);
    // 使用联合共识机制变更集群membership配置
    Result result = globals.raft->setConfiguration(request, response);
    if (result == Result::RETRY || result == Result::NOT_LEADER) {
        Protocol::Client::Error error;
        error.set_error_code(Protocol::Client::Error::NOT_LEADER);
        std::string leaderHint = globals.raft->getLeaderHint();
        if (!leaderHint.empty())
            error.set_leader_hint(leaderHint);
        rpc.returnError(error);
        return;
    }
    // 配置变更成功但是新配置不包括本机的话，其实到这里的时候本机已经step down了，但是还是
    // 要返回给client配置变更成功的response
    rpc.reply(response);
}

// client 的read-write类状态机命令会 RPC 到这个方法中；follower 也可能收到，
// 但只有 leader 会真正复制日志，非 leader 会返回 NOT_LEADER。
// read-write state machine command：包括 open session、close session、tree mkdir/write/remove、advance version。这类操作会修改复制状态机状态，必须走 Raft
void
ClientService::stateMachineCommand(RPC::ServerRPC rpc)
{
    // 1. 解析出业务请求
    PRELUDE(StateMachineCommand);
    Core::Buffer cmdBuffer;
    rpc.getRequest(cmdBuffer);
    // 2. 如果当前节点是 leader，将请求复制到本地raft log以及远端多数派；
    // 非 leader 会在这里返回 NOT_LEADER。
    std::pair<Result, uint64_t> result = globals.raft->replicate(cmdBuffer);
    if (result.first == Result::RETRY || result.first == Result::NOT_LEADER) {
        Protocol::Client::Error error;
        error.set_error_code(Protocol::Client::Error::NOT_LEADER);
        std::string leaderHint = globals.raft->getLeaderHint();
        if (!leaderHint.empty())
            error.set_leader_hint(leaderHint);
        rpc.returnError(error);
        return;
    }
    assert(result.first == Result::SUCCESS);
    uint64_t logIndex = result.second;
    // 3. 本command的log已经确认commit，现在等待该log最终被apply到state machine后构造rpc response
    if (!globals.stateMachine->waitForResponse(logIndex, request, response)) {
        // !!!
        // 如果leader在apply一条state machine command的时候state machine是某个runningVersion，
        // 说明这个runningVersion必然已经被apply，说明这个runningVersion必然已经commit。其他follower
        // 节点可以不需要此时实时的runningVersion和leader保持一致。但是他的commit log必然和leader保持一致，
        // 也就意味着follower节点未来apply到同一条state machine command的时候，state machine runningVersion
        // 必然会apply成和leader现在的一样，进而对同一条state machine command的apply进行和leader现在情况一样的
        // 约束。
        // 因此，如果leader此时在apply这条state machine command的时候的结果是invalid request，那么其他follower
        // 节点后续也必然是invalid request，从而不会真正apply这条command。所以leader在这里直接返回invalid request
        // 给client是全集群的共识，其他follower节点在apply这条command时也会是这个结果。
        rpc.rejectInvalidRequest();
        return;
    }
    rpc.reply(response);
}

// read-only state machine query：包括读文件、读目录等这类client的所有只读查询。只需要先等本节点状态机 apply 到 leader 当前 commit index，然后直接查本地状态机，不需要走raft。
void
ClientService::stateMachineQuery(RPC::ServerRPC rpc)
{
    PRELUDE(StateMachineQuery);
    // 阻塞性获取当前节点的足够新的能够满足当前read操作的commitIndex
    std::pair<Result, uint64_t> result = globals.raft->getLastCommitIndex();
    if (result.first == Result::RETRY || result.first == Result::NOT_LEADER) {
        Protocol::Client::Error error;
        error.set_error_code(Protocol::Client::Error::NOT_LEADER);
        std::string leaderHint = globals.raft->getLeaderHint();
        if (!leaderHint.empty())
            error.set_leader_hint(leaderHint);
        rpc.returnError(error);
        return;
    }
    assert(result.first == Result::SUCCESS);
    uint64_t logIndex = result.second;
    // 在真正前往state machine执行read操作之前，需要先等待state machine至少apply到了目标的commitIndex，
    // 这样才能确保不会stale read。
    globals.stateMachine->wait(logIndex);
    if (!globals.stateMachine->query(request, response))
        // read-only请求在当前运行的代码版本中无法被识别
        rpc.rejectInvalidRequest();
    rpc.reply(response);
}

// 对端client在TCP连接上本机之后，会发送一个verify request过来，该方法就是该request的处理方法，
// 用于检测此次连接是否有效，如果verify不通过，对方会收到response not ok并将ClientSession返回一个ErrorSession，
// verify失败后后续对端Client拿到的error session无法再和本机进行rpc了
void
ClientService::verifyRecipient(RPC::ServerRPC rpc)
{
    PRELUDE(VerifyRecipient);

    std::string clusterUUID = globals.clusterUUID.getOrDefault();
    uint64_t serverId = globals.serverId;

    if (!clusterUUID.empty())
        response.set_cluster_uuid(clusterUUID);
    // server本地必然有serverId，这是启动初始化时必须从config文件中读取的
    response.set_server_id(serverId);

    // 以自己本地记录到的clusterUUID和serverId为基准，检测是否和对方client认为的有冲突
    if (request.has_cluster_uuid() &&
        !request.cluster_uuid().empty() &&
        !clusterUUID.empty() &&
        clusterUUID != request.cluster_uuid()) {
        response.set_ok(false);
        response.set_error(Core::StringUtil::format(
           "Mismatched cluster UUIDs: request intended for %s, "
           "but this server is in %s",
           request.cluster_uuid().c_str(),
           clusterUUID.c_str()));
    } else if (request.has_server_id() &&
               serverId != request.server_id()) {
        response.set_ok(false);
        response.set_error(Core::StringUtil::format(
           "Mismatched server IDs: request intended for %lu, "
           "but this server is %lu",
           request.server_id(),
           serverId));
    } else {
        // 自己本地的值（如果有）和对端client认为的值（如果有）没有冲突
        response.set_ok(true);
        // server本地的clusterUUID可能为空，如果对端有，那就以对方的值来填充本地的值
        if (clusterUUID.empty() &&
            request.has_cluster_uuid() &&
            !request.cluster_uuid().empty()) {
            NOTICE("Adopting cluster UUID %s",
                   request.cluster_uuid().c_str());
            globals.clusterUUID.set(request.cluster_uuid());
            response.set_cluster_uuid(request.cluster_uuid());
        }
    }
    rpc.reply(response);
}

} // namespace LogCabin::Server
} // namespace LogCabin

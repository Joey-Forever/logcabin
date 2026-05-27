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

#include <signal.h>

#include "Core/Debug.h"
#include "Core/StringUtil.h"
#include "Protocol/Common.h"
#include "RPC/Server.h"
#include "Server/ClientService.h"
#include "Server/ControlService.h"
#include "Server/Globals.h"
#include "Server/RaftConsensus.h"
#include "Server/RaftService.h"
#include "Server/StateMachine.h"

namespace LogCabin {
namespace Server {

////////// Globals::SigIntHandler //////////

Globals::ExitHandler::ExitHandler(
        Event::Loop& eventLoop,
        int signalNumber)
    : Signal(signalNumber)
    , eventLoop(eventLoop)
{
}

void
Globals::ExitHandler::handleSignalEvent()
{
    NOTICE("%s: shutting down", strsignal(signalNumber));
    // 主线程在epoll_wait被SIGINT、SIGTERM这些终止信号唤醒后最后会执行到这里对event loop设置shouldExit为true，
    // 然后event loop就会退出runForever循环，最后引起Globals实例的析构，完成程序的安全退出。
    eventLoop.exit();
}

Globals::LogRotateHandler::LogRotateHandler(
        Event::Loop& eventLoop,
        int signalNumber)
    : Signal(signalNumber)
    , eventLoop(eventLoop)
{
}

void
Globals::LogRotateHandler::handleSignalEvent()
{
    NOTICE("%s: rotating logs", strsignal(signalNumber));
    std::string error = Core::Debug::reopenLogFromFilename();
    if (!error.empty()) {
        PANIC("Failed to rotate log file: %s",
              error.c_str());
    }
    NOTICE("%s: done rotating logs", strsignal(signalNumber));
}


////////// Globals //////////

Globals::Globals()
    : config()
    , serverStats(*this)
    , eventLoop()
    // 1. 设置几个主要信号的blocker，用于将信号进行block，使得信号能够被加到epoll中被捕捉，由于blocker在Globals类中最早被声明：
    //   1）Globals构造最开始信号就会被block，后续所有新创建的线程都会继承这些信号的block，防止有的线程绕过epoll直接接收并执行了信号的默认行为。
    //   2）Globals析构最后blocker才会析构并执行可能的unblock，防止过早unblock导致信号绕过epoll直接执行默认行为。
    , sigIntBlocker(SIGINT)
    , sigTermBlocker(SIGTERM)
    , sigUsr1Blocker(SIGUSR1)
    , sigUsr2Blocker(SIGUSR2)
    // 2. 将几个被block的信号以fd形式以EPOLLIN可读的方式加入epoll中，后续有信号过来epoll_wait被唤醒的时候，需要从fd中将内容read消费掉，防止
    //    epoll_wait反复被同一个信号触发，然后再执行自定义任务即可（例如析构流程），信号默认的行为不会再触发。
    , sigIntHandler(eventLoop, SIGINT)
    , sigIntMonitor(eventLoop, sigIntHandler)
    , sigTermHandler(eventLoop, SIGTERM)
    , sigTermMonitor(eventLoop, sigTermHandler)
    , sigUsr2Handler(eventLoop, SIGUSR2)
    , sigUsr2Monitor(eventLoop, sigUsr2Handler)
    , clusterUUID()
    , serverId(~0UL)
    , raft()
    , stateMachine()
    , controlService()
    , raftService()
    , clientService()
    , rpcServer()
{
}

Globals::~Globals()
{
    serverStats.exit();
}

// 1. 完成三个rpc服务的注册，占用指定的socket监听端口
// 2. init RaftConsensus对象
// 3. 创建 stateMachine对象
void
Globals::init()
{
    std::string uuid = config.read("clusterUUID", std::string(""));
    if (!uuid.empty())
        clusterUUID.set(uuid);
    // 一个server节点启动初始化的时候，config文件中必须要有serverId
    serverId = config.read<uint64_t>("serverId");
    Core::Debug::processName = Core::StringUtil::format("%lu", serverId);
    {
        ServerStats::Lock serverStatsLock(serverStats);
        serverStatsLock->set_server_id(serverId);
    }
    if (!raft) {
        raft.reset(new RaftConsensus(*this));
        raft->serverId = serverId;
    }

    if (!controlService) {
        controlService.reset(new ControlService(*this));
    }

    if (!raftService) {
        raftService.reset(new RaftService(*this));
    }

    if (!clientService) {
        clientService.reset(new ClientService(*this));
    }

    if (!rpcServer) {
        // rpcServer是Globals类定义中最后一个声明的，所以在析构的时候会首先析构，即比stateMachine实例和raft实例要早。
        rpcServer.reset(new RPC::Server(eventLoop,
                                        Protocol::Common::MAX_MESSAGE_LENGTH));

        uint32_t maxThreads = config.read<uint16_t>("maxThreads", 16);
        namespace ServiceId = Protocol::Common::ServiceId;
        rpcServer->registerService(ServiceId::CONTROL_SERVICE,
                                   controlService,
                                   maxThreads);
        rpcServer->registerService(ServiceId::RAFT_SERVICE,
                                   raftService,
                                   maxThreads);
        rpcServer->registerService(ServiceId::CLIENT_SERVICE,
                                   clientService,
                                   maxThreads);

        std::string listenAddressesStr =
            config.read<std::string>("listenAddresses");
        {
            ServerStats::Lock serverStatsLock(serverStats);
            serverStatsLock->set_server_id(serverId);
            serverStatsLock->set_addresses(listenAddressesStr);
        }
        std::vector<std::string> listenAddresses =
            Core::StringUtil::split(listenAddressesStr, ',');
        if (listenAddresses.empty()) {
            EXIT("No server addresses specified to listen on");
        }
        for (auto it = listenAddresses.begin();
             it != listenAddresses.end();
             ++it) {
            RPC::Address address(*it, Protocol::Common::DEFAULT_PORT);
            address.refresh(RPC::Address::TimePoint::max());
            // 这里会触发socket端口占用并且马上开始监听client的连接请求。
            std::string error = rpcServer->bind(address);
            if (!error.empty()) {
                EXIT("Could not listen on address %s: %s",
                     address.toString().c_str(),
                     error.c_str());
            }
            NOTICE("Serving on %s",
                   address.toString().c_str());
        }
        raft->serverAddresses = listenAddressesStr;
        raft->init();
    }

    if (!stateMachine) {
        // 创建构造state machine实例，同时创建相关后台线程
        stateMachine.reset(new StateMachine(raft, config, *this));
    }

    // 使能server stat的周期性打log，以及通过远程获取server stat，
    // 这能让开发者能够获取关于这台server节点的实时运行情况，包括raft consensus、state machine、log、snapshot等等所有状态数据
    serverStats.enable();
}

void
Globals::leaveSignalsBlocked()
{
    sigIntBlocker.leaveBlocked();
    sigTermBlocker.leaveBlocked();
    sigUsr1Blocker.leaveBlocked();
    sigUsr2Blocker.leaveBlocked();
}

void
Globals::run()
{
    eventLoop.runForever();
}

void
Globals::unblockAllSignals()
{
    sigIntBlocker.unblock();
    sigTermBlocker.unblock();
    sigUsr1Blocker.unblock();
    sigUsr2Blocker.unblock();
}


} // namespace LogCabin::Server
} // namespace LogCabin

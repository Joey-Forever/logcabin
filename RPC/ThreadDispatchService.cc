/* Copyright (c) 2012 Stanford University
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

#include <assert.h>

#include "Core/StringUtil.h"
#include "Core/ThreadId.h"
#include "RPC/ThreadDispatchService.h"

namespace LogCabin {
namespace RPC {

ThreadDispatchService::ThreadDispatchService(
        std::shared_ptr<Service> threadSafeService,
        uint32_t minThreads,
        uint32_t maxThreads)
    : threadSafeService(threadSafeService)
    , maxThreads(maxThreads)
    , mutex()
    , threads()
    , numFreeWorkers(0)
    , conditionVariable()
    , exit(false)
    , rpcQueue()
{
    assert(minThreads <= maxThreads);
    assert(0 < maxThreads);
    for (uint32_t i = 0; i < minThreads; ++i)
        threads.emplace_back(&ThreadDispatchService::workerMain, this);
}

// Globals实例销毁时，service实例会比raft实例、state machine实例等功能组件更早析构，
// 所以在这里等待所有worker threads完成手头工作是安全的
ThreadDispatchService::~ThreadDispatchService()
{
    // Signal the threads to exit.
    {
        std::lock_guard<std::mutex> lockGuard(mutex);
        // 设置service exit为true，唤醒所有worker线程（完成执行手头工作后）直接退出
        exit = true;
        conditionVariable.notify_all();
    }

    // Join the threads.
    // 等待所有worker线程退出
    while (!threads.empty()) {
        threads.back().join();
        threads.pop_back();
    }

    // Close the sessions of any remaining RPCs that didn't get processed.
    // 关闭所有还在queue中排队的rpc request的socket连接，对端rpc层会收到tcp断连。
    // 由于所有worker线程已经退出了，所以该操作不需要再拿mutex了
    while (!rpcQueue.empty()) {
        rpcQueue.front().closeSession();
        rpcQueue.pop();
    }
}

// epoll主线程在读取到某service的socket事件之后，最终会调用到这个handleRPC，随即返回继续epoll_wait，并不会实际执行rpc任务，这里会做三件事：
//   1. 将socket过来的rpc任务抛到rpcQueue中，提供给worker线程消费
//   1. 如果service的worker线程不足，就会创建新thread
//   2. 唤醒一个worker线程执行任务
void
ThreadDispatchService::handleRPC(ServerRPC serverRPC)
{
    std::lock_guard<std::mutex> lockGuard(mutex);
    assert(!exit);
    rpcQueue.push(std::move(serverRPC));
    if (numFreeWorkers == 0 && threads.size() < maxThreads)
        threads.emplace_back(&ThreadDispatchService::workerMain, this);
    conditionVariable.notify_one();
}

std::string
ThreadDispatchService::getName() const
{
    return threadSafeService->getName();
}

void
ThreadDispatchService::workerMain()
{
    Core::ThreadId::setName(
        Core::StringUtil::format("%s(%lu)",
                                 threadSafeService->getName().c_str(),
                                 Core::ThreadId::getId()));
    while (true) {
        ServerRPC rpc;
        { // find an RPC to process
            std::unique_lock<std::mutex> lockGuard(mutex);
            ++numFreeWorkers;
            while (!exit && rpcQueue.empty())
                conditionVariable.wait(lockGuard);
            --numFreeWorkers;
            if (exit)
                return;
            rpc = std::move(rpcQueue.front());
            rpcQueue.pop();
        }
        // execute RPC handler
        // 这个threadSafeService才是ClientService、RaftService和ControlService的本体，由一个worker线程实际执行rpc任务。
        threadSafeService->handleRPC(std::move(rpc));
    }
}

} // namespace LogCabin::RPC
} // namespace LogCabin

/* Copyright (c) 2026 Joey Project
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

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

#include <LogCabin/Client.h>
#include <LogCabin/Debug.h>
#include <LogCabin/Util.h>

namespace {

using LogCabin::Client::Cluster;
using LogCabin::Client::Configuration;
using LogCabin::Client::ConfigurationResult;
using LogCabin::Client::Result;
using LogCabin::Client::Server;
using LogCabin::Client::Status;
using LogCabin::Client::Tree;
const char* ADDRESSES[] = {
    "127.0.0.1:5254",
    "127.0.0.1:5255",
    "127.0.0.1:5256",
};

std::string clusterAddresses();
std::string joinPath(const std::string& a, const std::string& b);
void mkdirIfNeeded(const std::string& path);

const char* SERVER_BINARY = "build/LogCabin";
const char* WORKDIR = "raftdemo";
const char* LOG_POLICY = "NOTICE";
const char* CLIENT_PATH = "/demo/message";
const char* CLIENT_VALUE = "hello raft";

struct Process {
    Process(uint64_t serverId, pid_t pid, const std::string& logPath)
        : serverId(serverId)
        , pid(pid)
        , logPath(logPath)
    {
    }
    uint64_t serverId;
    pid_t pid;
    std::string logPath;
};

std::string
clusterAddresses()
{
    std::string cluster;
    for (size_t i = 0; i < 3; ++i) {
        if (i > 0)
            cluster += ",";
        cluster += ADDRESSES[i];
    }
    return cluster;
}

void
checkUsage(int argc, char** argv)
{
    if (argc == 1)
        return;
    if (argc == 2 && std::string(argv[1]) == "--help") {
        std::cout
            << "Runs a fixed 3-server LogCabin Raft demo on the local machine."
            << std::endl
            << std::endl
            << "Usage: " << argv[0] << std::endl
            << std::endl;
        exit(0);
    }
    throw std::runtime_error("RaftDemo does not take command-line arguments");
}

void
removeRecursive(const std::string& path)
{
    DIR* dir = opendir(path.c_str());
    if (dir == NULL) {
        if (errno == ENOENT)
            return;
        if (unlink(path.c_str()) == 0)
            return;
        throw std::runtime_error("could not remove " + path + ": " +
                                 strerror(errno));
    }
    while (true) {
        errno = 0;
        dirent* entry = readdir(dir);
        if (entry == NULL)
            break;
        std::string name = entry->d_name;
        if (name == "." || name == "..")
            continue;
        removeRecursive(joinPath(path, name));
    }
    if (errno != 0) {
        closedir(dir);
        throw std::runtime_error("could not read " + path + ": " +
                                 strerror(errno));
    }
    closedir(dir);
    if (rmdir(path.c_str()) != 0)
        throw std::runtime_error("could not remove " + path + ": " +
                                 strerror(errno));
}

void
prepareWorkdir()
{
    removeRecursive(WORKDIR);
    mkdirIfNeeded(WORKDIR);
}

void
mkdirIfNeeded(const std::string& path)
{
    if (mkdir(path.c_str(), 0755) == 0)
        return;
    if (errno == EEXIST)
        return;
    throw std::runtime_error("mkdir failed for " + path + ": " +
                             strerror(errno));
}

void
ensureBinary(const std::string& path)
{
    if (access(path.c_str(), X_OK) != 0)
        throw std::runtime_error("missing executable " + path);
}

std::string
joinPath(const std::string& a, const std::string& b)
{
    return a + "/" + b;
}

std::string
writeConfig(uint64_t serverId, const std::string& addr)
{
    std::string storagePath = joinPath(WORKDIR,
                                       "storage" + std::to_string(serverId));
    std::string configPath = joinPath(WORKDIR,
                                      "server" + std::to_string(serverId) +
                                      ".conf");
    std::ofstream f(configPath.c_str());
    if (!f)
        throw std::runtime_error("could not write " + configPath);
    f << "serverId = " << serverId << "\n";
    f << "listenAddresses = " << addr << "\n";
    f << "clusterUUID = raftdemo-local-3\n";
    f << "logPolicy = " << LOG_POLICY << "\n";
    f << "electionTimeoutMilliseconds = 500\n";
    f << "heartbeatPeriodMilliseconds = 250\n";
    f << "storageModule = Segmented\n";
    f << "storagePath = " << storagePath << "\n";
    f << "storageSegmentBytes = 1048576\n";
    f << "snapshotMinLogSize = 1048576\n";
    return configPath;
}

std::vector<char*>
makeArgv(std::vector<std::string>& args)
{
    std::vector<char*> argv;
    for (size_t i = 0; i < args.size(); ++i)
        argv.push_back(&args[i][0]);
    argv.push_back(NULL);
    return argv;
}

pid_t
spawn(const std::vector<std::string>& command,
      const std::string& logPath)
{
    pid_t pid = fork();
    if (pid < 0)
        throw std::runtime_error("fork failed: " + std::string(strerror(errno)));
    if (pid == 0) {
        int fd = open(logPath.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (fd < 0)
            _exit(127);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        close(fd);
        int nullFd = open("/dev/null", O_RDONLY);
        if (nullFd >= 0) {
            dup2(nullFd, STDIN_FILENO);
            close(nullFd);
        }
        std::vector<std::string> args = command;
        std::vector<char*> argv = makeArgv(args);
        execvp(argv[0], &argv[0]);
        _exit(127);
    }
    return pid;
}

void
waitSuccess(pid_t pid, const std::string& name)
{
    int status = 0;
    if (waitpid(pid, &status, 0) < 0)
        throw std::runtime_error("waitpid failed for " + name);
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
        throw std::runtime_error(name + " failed");
}

void
terminate(const std::vector<Process>& processes)
{
    for (size_t i = 0; i < processes.size(); ++i)
        kill(processes[i].pid, SIGTERM);
    usleep(200 * 1000);
    for (size_t i = 0; i < processes.size(); ++i) {
        int status = 0;
        if (waitpid(processes[i].pid, &status, WNOHANG) == 0)
            kill(processes[i].pid, SIGKILL);
    }
    for (size_t i = 0; i < processes.size(); ++i) {
        int status = 0;
        waitpid(processes[i].pid, &status, 0);
    }
}

void
checkServers(const std::vector<Process>& processes)
{
    for (size_t i = 0; i < processes.size(); ++i) {
        int status = 0;
        pid_t result = waitpid(processes[i].pid, &status, WNOHANG);
        if (result == processes[i].pid)
            throw std::runtime_error("server " +
                                     std::to_string(processes[i].serverId) +
                                     " exited; see " +
                                     processes[i].logPath);
    }
}

// Demo 启动阶段需要做一次固定 membership 初始化：
// --bootstrap 只让 server 1 具备初始配置，server 2/3 启动后还不是
// voting member。这里通过 client API 把集群 membership 固定设置为
// 本 demo 的 3 个本地节点；
// 正常流程是开一个单独的reconfigure client来动态执行raft集群membership变更逻辑，
// 本demo不需要动态变更membership，因此直接将demo主进程作为替代client对集群membership进行初始化即可。
void
configureCluster()
{
    LogCabin::Client::Debug::setLogPolicy(
        LogCabin::Client::Debug::logPolicyFromString(LOG_POLICY));

    Cluster cluster(clusterAddresses());
    std::pair<uint64_t, Configuration> configuration =
        cluster.getConfiguration();

    Configuration servers;
    for (size_t i = 0; i < 3; ++i) {
        Server info;
        Result result = cluster.getServerInfo(ADDRESSES[i],
                                              /* timeout = 2s */ 2000000000UL,
                                              info);
        if (result.status != Status::OK)
            throw std::runtime_error("could not get server info from " +
                                     std::string(ADDRESSES[i]) + ": " +
                                     result.error);
        servers.emplace_back(info.serverId, info.addresses);
    }

    ConfigurationResult result =
        cluster.setConfiguration(configuration.first, servers);
    if (result.status == ConfigurationResult::OK)
        return;
    throw std::runtime_error("could not set cluster membership: " +
                             result.error);
}

int
runClient()
{
    LogCabin::Client::Debug::setLogPolicy(
        LogCabin::Client::Debug::logPolicyFromString(LOG_POLICY));

    Cluster cluster(clusterAddresses());
    Tree tree = cluster.getTree();

    std::cout << "Creating /demo" << std::endl;
    Result result = tree.makeDirectory("/demo");
    if (result.status != Status::OK && result.status != Status::TYPE_ERROR)
        throw LogCabin::Client::Exception(result.error);

    std::cout << "Writing " << CLIENT_PATH << " = "
              << CLIENT_VALUE << std::endl;
    tree.writeEx(CLIENT_PATH, CLIENT_VALUE);

    std::string contents = tree.readEx(CLIENT_PATH);
    std::cout << "Read back " << CLIENT_PATH << " = "
              << contents << std::endl;
    return contents == CLIENT_VALUE ? 0 : 1;
}

pid_t
spawnClient(const std::string& logPath)
{
    pid_t pid = fork();
    if (pid < 0)
        throw std::runtime_error("fork failed: " + std::string(strerror(errno)));
    if (pid == 0) {
        int fd = open(logPath.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (fd < 0)
            _exit(127);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        close(fd);
        try {
            exit(runClient());
        } catch (const std::exception& e) {
            std::cerr << "RaftDemo client failed: " << e.what() << std::endl;
            exit(1);
        }
    }
    return pid;
}

int
runLauncher()
{
    ensureBinary(SERVER_BINARY);
    prepareWorkdir();

    std::vector<std::string> configs;
    for (uint64_t i = 0; i < 3; ++i)
        configs.push_back(writeConfig(i + 1, ADDRESSES[i]));

    std::cout << "Bootstrapping server 1" << std::endl;
    pid_t bootstrapPid = spawn({SERVER_BINARY, "--bootstrap",
                                "--config", configs[0]},
                               joinPath(WORKDIR, "bootstrap.log"));
    waitSuccess(bootstrapPid, "bootstrap");

    std::vector<Process> servers;
    try {
        for (uint64_t i = 0; i < 3; ++i) {
            std::string logPath = joinPath(WORKDIR,
                                           "server" + std::to_string(i + 1) +
                                           ".log");
            pid_t pid = spawn({SERVER_BINARY, "--config", configs[i]},
                              logPath);
            servers.push_back(Process(i + 1, pid, logPath));
            std::cout << "Started server " << i + 1
                      << " pid=" << pid
                      << " address=" << ADDRESSES[i]
                      << " log=" << logPath
                      << std::endl;
        }

        sleep(1);
        checkServers(servers);

        std::cout << "Configuring fixed 3-node cluster membership"
                  << std::endl;
        configureCluster();

        pid_t clientPid = spawnClient(joinPath(WORKDIR, "client.log"));
        std::cout << "Started client pid=" << clientPid
                  << " log=" << joinPath(WORKDIR, "client.log")
                  << std::endl;

        waitSuccess(clientPid, "client");
        std::cout << "Client completed" << std::endl;

        terminate(servers);
        return 0;
    } catch (...) {
        terminate(servers);
        throw;
    }
}

} // anonymous namespace

int
main(int argc, char** argv)
{
    try {
        checkUsage(argc, argv);
        return runLauncher();
    } catch (const std::exception& e) {
        std::cerr << "RaftDemo failed: " << e.what() << std::endl;
        return 1;
    }
}

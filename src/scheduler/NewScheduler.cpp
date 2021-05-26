/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "scheduler/NewScheduler.h"

namespace nebula {
namespace graph {
NewScheduler::NewScheduler(QueryContext* qctx) {
    qctx_ = qctx;
}

folly::Future<Status> NewScheduler::schedule() {
    auto executor = Executor::create(qctx_->plan()->root(), qctx_);
    return doSchedule(executor);
}

folly::Future<Status> NewScheduler::doSchedule(Executor* root) const {
    std::unordered_map<int64_t, std::vector<folly::Promise<Status>>> promiseMap;
    std::unordered_map<int64_t, std::vector<folly::Future<Status>>> futureMap;
    std::queue<Executor*> queue;
    std::queue<Executor*> queue2;
    std::unordered_set<Executor*> visited;
    queue.push(root);
    visited.emplace(root);
    auto* runner = qctx_->rctx()->runner();
    folly::Promise<Status> promiseForRoot;
    auto resultFuture = promiseForRoot.getFuture();
    while (!queue.empty()) {
        auto* exe = queue.front();
        queue.pop();
        queue2.push(exe);

        std::vector<folly::Future<Status>> futures;
        for (auto* dep : exe->depends()) {
            auto notVisited = visited.emplace(dep).second;
            if (notVisited) {
                queue.push(dep);
            }
            folly::Promise<Status> p;
            futures.emplace_back(p.getFuture());
            auto& promises = promiseMap[dep->id()];
            promises.emplace_back(std::move(p));
        }
        futureMap.emplace(exe->id(), std::move(futures));
    }

    while (!queue2.empty()) {
        auto* exe = queue2.front();
        queue2.pop();

        std::vector<folly::Future<Status>> currentExeFutures;
        auto currentFuturesFound = futureMap.find(exe->id());
        if (currentFuturesFound != futureMap.end()) {
            currentExeFutures = std::move(currentFuturesFound->second);
        } else {
            return Status::Error();
        }

        std::vector<folly::Promise<Status>> currentExePromises;
        auto currentPromisesFound = promiseMap.find(exe->id());
        if (currentPromisesFound != promiseMap.end()) {
            currentExePromises = std::move(currentPromisesFound->second);
        } else {
            currentExePromises.emplace_back(std::move(promiseForRoot));
        }
        scheduleExecutor(std::move(currentExeFutures), exe, runner, std::move(currentExePromises));
    }

    return resultFuture;
}

void NewScheduler::scheduleExecutor(
    std::vector<folly::Future<Status>>&& futures,
    Executor* exe,
    folly::Executor* runner,
    std::vector<folly::Promise<Status>>&& promises) const {
    switch (exe->node()->kind()) {
        case PlanNode::Kind::kSelect: {
            folly::collect(futures).via(runner).thenValue(
                [this, pros = std::move(promises)](std::vector<Status>&& status) mutable {
                    auto s = checkStatus(std::move(status));
                    if (!s.ok()) {
                        return notifyError(pros, s);
                    }
                });
            break;
        }
        case PlanNode::Kind::kLoop: {
            auto loop = static_cast<LoopExecutor*>(exe);
            runLoop(std::move(futures), loop, runner, std::move(promises));
            break;
        }
        default: {
            if (exe->depends().empty()) {
                runLeafExecutor(exe, runner, std::move(promises));
            } else {
                runExecutor(std::move(futures), exe, runner, std::move(promises));
            }
            break;
        }
    }
}

void NewScheduler::runExecutor(
    std::vector<folly::Future<Status>>&& futures,
    Executor* exe,
    folly::Executor* runner,
    std::vector<folly::Promise<Status>>&& promises) const {
    folly::collect(futures).via(runner).thenValue(
        [exe, runner, pros = std::move(promises), this](std::vector<Status>&& status) mutable {
            auto depStatus = checkStatus(std::move(status));
            if (!depStatus.ok()) {
                return notifyError(pros, depStatus);
            }
            // Execute in current thread.
            std::move(execute(exe)).thenValue(
                [pros = std::move(pros), this](Status exeStatus) mutable {
                    if (!exeStatus.ok()) {
                        return notifyError(pros, exeStatus);
                    }
                    return notifyOK(pros);
                });
        });
}

void NewScheduler::runLeafExecutor(
    Executor* exe,
    folly::Executor* runner,
    std::vector<folly::Promise<Status>>&& promises) const {
    std::move(execute(exe))
        .via(runner)
        .thenValue([pros = std::move(promises), this](Status s) mutable {
            if (!s.ok()) {
                return notifyError(pros, s);
            }
            return notifyOK(pros);
        });
}

void NewScheduler::runLoop(std::vector<folly::Future<Status>>&& futures,
                                            LoopExecutor* loop,
                                            folly::Executor* runner,
                                            std::vector<folly::Promise<Status>>&& promises) const {
    folly::collect(futures).via(runner).thenValue(
        [loop, runner, pros = std::move(promises), this](std::vector<Status>&& status) mutable {
            auto s = checkStatus(std::move(status));
            if (!s.ok()) {
                return notifyError(pros, s);
            }

            std::move(execute(loop)).thenValue(
                [loop, runner, pros = std::move(pros), this](Status loopStatus) mutable {
                    if (!loopStatus.ok()) {
                        return notifyError(pros, loopStatus);
                    }
                    auto val = qctx_->ectx()->getValue(loop->node()->outputVar());
                    if (!val.isBool()) {
                        std::stringstream ss;
                        ss << "Loop produces a bad condition result: " << val
                           << " type: " << val.type();
                        return notifyError(pros, Status::Error(ss.str()));
                    }
                    if (val.getBool()) {
                        auto loopBody = loop->loopBody();
                        auto scheduleFuture = doSchedule(loopBody);
                        std::vector<folly::Future<Status>> fs;
                        fs.emplace_back(std::move(scheduleFuture));
                        runLoop(std::move(fs), loop, runner, std::move(pros));
                    } else {
                        return notifyOK(pros);
                    }
                });
        });
}

Status NewScheduler::checkStatus(std::vector<Status>&& status) const {
    for (auto& s : status) {
        if (!s.ok()) {
            return s;
        }
    }
    return Status::OK();
}

void NewScheduler::notifyOK(std::vector<folly::Promise<Status>>& promises) const {
    for (auto& p : promises) {
        p.setValue(Status::OK());
    }
}

void NewScheduler::notifyError(std::vector<folly::Promise<Status>>& promises, Status status) const {
    for (auto& p : promises) {
        p.setValue(status);
    }
}

folly::Future<Status> NewScheduler::execute(Executor *executor) const {
    auto status = executor->open();
    if (!status.ok()) {
        return executor->error(std::move(status));
    }
    return executor->execute().thenValue([executor](Status s) {
        NG_RETURN_IF_ERROR(s);
        return executor->close();
    });
}

}  // namespace graph
}  // namespace nebula

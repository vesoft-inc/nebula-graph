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
    std::queue<Executor*> queue;
    queue.push(root);
    auto* runner = qctx_->rctx()->runner();
    auto result = folly::makeFuture(Status::OK());
    while (!queue.empty()) {
        auto* exe = queue.front();
        queue.pop();

        std::vector<folly::Future<Status>> futures;
        for (auto* dep : exe->depends()) {
            queue.push(dep);
            folly::Promise<Status> p;
            futures.emplace_back(p.getFuture());
            auto& promises = promiseMap[dep->id()];
            promises.emplace_back(std::move(p));
        }
        std::vector<folly::Promise<Status>> currentExePromises;
        auto currentPromisesFound = promiseMap.find(exe->id());
        if (currentPromisesFound != promiseMap.end()) {
            currentExePromises = std::move(currentPromisesFound->second);
        }
        auto resultFuture =
            scheduleExecutor(std::move(futures), exe, runner, std::move(currentExePromises));
        if (exe == root) {
            result = std::move(resultFuture);
        }
    }

    return result;
}

folly::Future<Status> NewScheduler::scheduleExecutor(
    std::vector<folly::Future<Status>>&& futures,
    Executor* exe,
    folly::Executor* runner,
    std::vector<folly::Promise<Status>>&& promises) const {
    switch (exe->node()->kind()) {
        case PlanNode::Kind::kSelect: {
            return folly::collect(futures).via(runner).thenValue(
                [this, pros = std::move(promises)](std::vector<Status>&& status) mutable {
                    auto s = checkStatus(std::move(status));
                    if (!s.ok()) {
                        notifyError(pros, s);
                        return s;
                    }
                    return Status::OK();
                });
        }
        case PlanNode::Kind::kLoop: {
            auto loop = static_cast<LoopExecutor*>(exe);
            return runLoop(std::move(futures), loop, runner, std::move(promises));
        }
        default: {
            if (exe->depends().empty()) {
                return runLeafExecutor(exe, runner, std::move(promises));
            }
            return runExecutor(std::move(futures), exe, runner, std::move(promises));
        }
    }
}

folly::Future<Status> NewScheduler::runExecutor(
    std::vector<folly::Future<Status>>&& futures,
    Executor* exe,
    folly::Executor* runner,
    std::vector<folly::Promise<Status>>&& promises) const {
    return folly::collect(futures).via(runner).thenValue(
        [exe, runner, pros = std::move(promises), this](std::vector<Status>&& status) mutable {
            auto depStatus = checkStatus(std::move(status));
            if (!depStatus.ok()) {
                notifyError(pros, depStatus);
                return depStatus;
            }
            auto f = execute(exe);
            std::move(f).via(runner).thenValue(
                [pros = std::move(pros), this](Status exeStatus) mutable {
                    if (!exeStatus.ok()) {
                        notifyError(pros, exeStatus);
                        return exeStatus;
                    }
                    notifyOK(pros);
                    return Status::OK();
                });
            return Status::OK();
        });
}

folly::Future<Status> NewScheduler::runLeafExecutor(
    Executor* exe,
    folly::Executor* runner,
    std::vector<folly::Promise<Status>>&& promises) const {
    return std::move(execute(exe))
        .via(runner)
        .thenValue([pros = std::move(promises), this](Status s) mutable {
            if (!s.ok()) {
                notifyError(pros, s);
                return s;
            }
            return Status::OK();
        });
}

folly::Future<Status> NewScheduler::runLoop(std::vector<folly::Future<Status>>&& futures,
                                            LoopExecutor* loop,
                                            folly::Executor* runner,
                                            std::vector<folly::Promise<Status>>&& promises) const {
    return folly::collect(futures).via(runner).thenValue(
        [loop, runner, pros = std::move(promises), this](std::vector<Status>&& status) mutable {
            auto s = checkStatus(std::move(status));
            if (!s.ok()) {
                notifyError(pros, s);
                return s;
            }

            auto f = execute(loop);
            std::move(f).via(runner).thenValue(
                [loop, runner, pros = std::move(pros), this](Status loopStatus) mutable {
                    if (!loopStatus.ok()) {
                        notifyError(pros, loopStatus);
                        return loopStatus;
                    }
                    auto val = qctx_->ectx()->getValue(loop->node()->outputVar());
                    if (!val.isBool()) {
                        std::stringstream ss;
                        ss << "Loop produces a bad condition result: " << val
                           << " type: " << val.type();
                        notifyError(pros, Status::Error(ss.str()));
                        return Status::Error(ss.str());
                    }
                    if (val.getBool()) {
                        auto loopBody = loop->loopBody();
                        auto scheduleFuture = doSchedule(loopBody);
                        std::vector<folly::Future<Status>> fs;
                        fs.emplace_back(std::move(scheduleFuture));
                        runLoop(std::move(fs), loop, runner, std::move(pros));
                    }
                    return Status::OK();
                });
            return Status::OK();
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

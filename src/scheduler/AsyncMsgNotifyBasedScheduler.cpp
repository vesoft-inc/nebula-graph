/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "scheduler/AsyncMsgNotifyBasedScheduler.h"

DECLARE_bool(enable_lifetime_optimize);

namespace nebula {
namespace graph {
AsyncMsgNotifyBasedScheduler::AsyncMsgNotifyBasedScheduler(QueryContext* qctx) : Scheduler() {
    qctx_ = qctx;
}

folly::Future<Status> AsyncMsgNotifyBasedScheduler::schedule() {
    if (FLAGS_enable_lifetime_optimize) {
        // special for root
        qctx_->plan()->root()->outputVarPtr()->userCount.store(std::numeric_limits<uint64_t>::max(),
                                                               std::memory_order_relaxed);
        analyzeLifetime(qctx_->plan()->root());
    }
    auto executor = Executor::create(qctx_->plan()->root(), qctx_);
    return doSchedule(executor);
}

folly::Future<Status> AsyncMsgNotifyBasedScheduler::doSchedule(Executor* root) const {
    std::unordered_map<int64_t, std::vector<Notifier>> notifierMap;
    std::unordered_map<int64_t, std::vector<Receiver>> receiverMap;
    std::queue<Executor*> queue;
    std::queue<Executor*> queue2;
    std::unordered_set<Executor*> visited;

    auto* runner = qctx_->rctx()->runner();
    Notifier notifierOfRoot;
    auto resultReceiver = notifierOfRoot.getFuture();
    notifierMap[root->id()].emplace_back(std::move(notifierOfRoot));
    queue.push(root);
    visited.emplace(root);
    while (!queue.empty()) {
        auto* exe = queue.front();
        queue.pop();
        queue2.push(exe);

        std::vector<Receiver> receivers;
        for (auto* dep : exe->depends()) {
            auto notVisited = visited.emplace(dep).second;
            if (notVisited) {
                queue.push(dep);
            }
            Notifier p;
            receivers.emplace_back(p.getFuture());
            auto& notifiers = notifierMap[dep->id()];
            notifiers.emplace_back(std::move(p));
        }
        receiverMap.emplace(exe->id(), std::move(receivers));
    }

    while (!queue2.empty()) {
        auto* exe = queue2.front();
        queue2.pop();

        auto currentReceiversFound = receiverMap.find(exe->id());
        DCHECK(currentReceiversFound != receiverMap.end());
        auto currentExeReceivers = std::move(currentReceiversFound->second);

        auto currentNotifiersFound = notifierMap.find(exe->id());
        DCHECK(currentNotifiersFound != notifierMap.end());
        auto currentExeNotifiers = std::move(currentNotifiersFound->second);

        scheduleExecutor(
            std::move(currentExeReceivers), exe, runner, std::move(currentExeNotifiers));
    }

    return resultReceiver;
}

void AsyncMsgNotifyBasedScheduler::scheduleExecutor(
    std::vector<Receiver>&& receivers,
    Executor* exe,
    folly::Executor* runner,
    std::vector<Notifier>&& notifiers) const {
    switch (exe->node()->kind()) {
        case PlanNode::Kind::kSelect: {
            auto select = static_cast<SelectExecutor*>(exe);
            runSelect(std::move(receivers), select, runner, std::move(notifiers));
            break;
        }
        case PlanNode::Kind::kLoop: {
            auto loop = static_cast<LoopExecutor*>(exe);
            runLoop(std::move(receivers), loop, runner, std::move(notifiers));
            break;
        }
        default: {
            if (exe->depends().empty()) {
                runLeafExecutor(exe, runner, std::move(notifiers));
            } else {
                runExecutor(std::move(receivers), exe, runner, std::move(notifiers));
            }
            break;
        }
    }
}

void AsyncMsgNotifyBasedScheduler::runSelect(std::vector<Receiver>&& receivers,
                             SelectExecutor* select,
                             folly::Executor* runner,
                             std::vector<Notifier>&& notifiers) const {
    folly::collect(receivers).via(runner).thenTry(
        [select, notifiers = std::move(notifiers), this](auto&& t) mutable {
            if (t.hasException()) {
                return notifyError(notifiers, Status::Error(t.exception().what()));
            }
            auto status = std::move(t).value();
            auto s = checkStatus(std::move(status));
            if (!s.ok()) {
                return notifyError(notifiers, s);
            }

            std::move(execute(select))
                .thenTry([select, notifiers = std::move(notifiers), this](
                             auto&& selectTry) mutable {
                    if (selectTry.hasException()) {
                        return notifyError(notifiers, Status::Error(selectTry.exception().what()));
                    }
                    auto selectStatus = std::move(selectTry).value();
                    if (!selectStatus.ok()) {
                        return notifyError(notifiers, selectStatus);
                    }
                    auto val = qctx_->ectx()->getValue(select->node()->outputVar());
                    if (!val.isBool()) {
                        std::stringstream ss;
                        ss << "Loop produces a bad condition result: " << val
                           << " type: " << val.type();
                        return notifyError(notifiers, Status::Error(ss.str()));
                    }

                    auto selectFuture = folly::makeFuture<Status>(Status::OK());
                    if (val.getBool()) {
                        selectFuture = doSchedule(select->thenBody());
                    } else {
                        selectFuture = doSchedule(select->elseBody());
                    }
                    std::move(selectFuture)
                        .thenTry([notifiers = std::move(notifiers), this](auto&& bodyTry) mutable {
                            if (bodyTry.hasException()) {
                                return notifyError(notifiers,
                                                   Status::Error(bodyTry.exception().what()));
                            }
                            auto bodyStatus = std::move(bodyTry).value();
                            if (!bodyStatus.ok()) {
                                return notifyError(notifiers, bodyStatus);
                            } else {
                                return notifyOK(notifiers);
                            }
                        });
                });
        });
}

void AsyncMsgNotifyBasedScheduler::runExecutor(std::vector<Receiver>&& receivers,
                                               Executor* exe,
                                               folly::Executor* runner,
                                               std::vector<Notifier>&& notifiers) const {
    folly::collect(receivers).via(runner).thenTry(
        [exe, notifiers = std::move(notifiers), this](auto&& t) mutable {
            if (t.hasException()) {
                return notifyError(notifiers, Status::Error(t.exception().what()));
            }
            auto status = std::move(t).value();
            auto depStatus = checkStatus(std::move(status));
            if (!depStatus.ok()) {
                return notifyError(notifiers, depStatus);
            }
            // Execute in current thread.
            std::move(execute(exe)).thenTry(
                [notifiers = std::move(notifiers), this](auto&& exeTry) mutable {
                    if (exeTry.hasException()) {
                        return notifyError(notifiers, Status::Error(exeTry.exception().what()));
                    }
                    auto exeStatus = std::move(exeTry).value();
                    if (!exeStatus.ok()) {
                        return notifyError(notifiers, exeStatus);
                    }
                    return notifyOK(notifiers);
                });
        });
}

void AsyncMsgNotifyBasedScheduler::runLeafExecutor(
    Executor* exe,
    folly::Executor* runner,
    std::vector<Notifier>&& notifiers) const {
    std::move(execute(exe))
        .via(runner)
        .thenTry([notifiers = std::move(notifiers), this](auto&& t) mutable {
            if (t.hasException()) {
                return notifyError(notifiers, Status::Error(t.exception().what()));
            }
            auto s = std::move(t).value();
            if (!s.ok()) {
                return notifyError(notifiers, s);
            }
            return notifyOK(notifiers);
        });
}

void AsyncMsgNotifyBasedScheduler::runLoop(std::vector<Receiver>&& receivers,
                                            LoopExecutor* loop,
                                            folly::Executor* runner,
                                            std::vector<Notifier>&& notifiers) const {
    folly::collect(receivers).via(runner).thenTry(
        [loop, runner, notifiers = std::move(notifiers), this](auto&& t) mutable {
            if (t.hasException()) {
                return notifyError(notifiers, Status::Error(t.exception().what()));
            }
            auto status = std::move(t).value();
            auto s = checkStatus(std::move(status));
            if (!s.ok()) {
                return notifyError(notifiers, s);
            }

            std::move(execute(loop)).thenTry(
                [loop, runner, notifiers = std::move(notifiers), this](auto&& loopTry) mutable {
                    if (loopTry.hasException()) {
                        return notifyError(notifiers, Status::Error(loopTry.exception().what()));
                    }
                    auto loopStatus = std::move(loopTry).value();
                    if (!loopStatus.ok()) {
                        return notifyError(notifiers, loopStatus);
                    }
                    auto val = qctx_->ectx()->getValue(loop->node()->outputVar());
                    if (!val.isBool()) {
                        std::stringstream ss;
                        ss << "Loop produces a bad condition result: " << val
                           << " type: " << val.type();
                        return notifyError(notifiers, Status::Error(ss.str()));
                    }
                    if (val.getBool()) {
                        auto loopBody = loop->loopBody();
                        auto scheduleReceiver = doSchedule(loopBody);
                        std::vector<Receiver> rs;
                        rs.emplace_back(std::move(scheduleReceiver));
                        runLoop(std::move(rs), loop, runner, std::move(notifiers));
                    } else {
                        return notifyOK(notifiers);
                    }
                });
        });
}

Status AsyncMsgNotifyBasedScheduler::checkStatus(std::vector<Status>&& status) const {
    for (auto& s : status) {
        if (!s.ok()) {
            return s;
        }
    }
    return Status::OK();
}

void AsyncMsgNotifyBasedScheduler::notifyOK(std::vector<Notifier>& notifiers) const {
    for (auto& n : notifiers) {
        n.setValue(Status::OK());
    }
}

void AsyncMsgNotifyBasedScheduler::notifyError(std::vector<Notifier>& notifiers,
                                               Status status) const {
    for (auto& n : notifiers) {
        n.setValue(status);
    }
}

folly::Future<Status> AsyncMsgNotifyBasedScheduler::execute(Executor *executor) const {
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

/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "scheduler/Scheduler.h"

#include "context/QueryContext.h"
#include "executor/ExecutionError.h"
#include "executor/Executor.h"
#include "executor/logic/LoopExecutor.h"
#include "executor/logic/PassThroughExecutor.h"
#include "executor/logic/SelectExecutor.h"
#include "planner/PlanNode.h"

namespace nebula {
namespace graph {

Scheduler::Task::Task(const Executor *e) : planId(DCHECK_NOTNULL(e)->node()->id()) {}

<<<<<<< HEAD
Scheduler::PassThroughData::PassThroughData(int32_t outputs)
    : promise(std::make_unique<folly::SharedPromise<Status>>()), numOutputs(outputs) {}

Scheduler::Scheduler(QueryContext *qctx) : qctx_(DCHECK_NOTNULL(qctx)) {}

folly::Future<Status> Scheduler::schedule() {
    auto executor = Executor::create(qctx_->plan()->root(), qctx_);
=======
Scheduler::MultiOutputsData::MultiOutputsData(int32_t outputs)
    : promise(std::make_unique<folly::SharedPromise<GraphStatus>>()), numOutputs(outputs) {}

Scheduler::Scheduler(QueryContext *qctx) : qctx_(DCHECK_NOTNULL(qctx)) {}

folly::Future<GraphStatus> Scheduler::schedule() {
    auto executor = Executor::makeExecutor(qctx_->plan()->root(), qctx_);
>>>>>>> all use GraphStatus
    analyze(executor);
    return doSchedule(executor);
}

void Scheduler::analyze(Executor *executor) {
    switch (executor->node()->kind()) {
        case PlanNode::Kind::kPassThrough: {
            const auto &name = executor->node()->varName();
            auto it = passThroughPromiseMap_.find(name);
            if (it == passThroughPromiseMap_.end()) {
                PassThroughData data(executor->successors().size());
                passThroughPromiseMap_.emplace(name, std::move(data));
            }
            break;
        }
        case PlanNode::Kind::kSelect: {
            auto sel = static_cast<SelectExecutor *>(executor);
            analyze(sel->thenBody());
            analyze(sel->elseBody());
            break;
        }
        case PlanNode::Kind::kLoop: {
            auto loop = static_cast<LoopExecutor *>(executor);
            analyze(loop->loopBody());
            break;
        }
        default:
            break;
    }

    for (auto dep : executor->depends()) {
        analyze(dep);
    }
}

folly::Future<GraphStatus> Scheduler::doSchedule(Executor *executor) {
    switch (executor->node()->kind()) {
        case PlanNode::Kind::kSelect: {
            auto sel = static_cast<SelectExecutor *>(executor);
            return doScheduleParallel(sel->depends())
                .then(task(sel,
                           [sel, this](GraphStatus status) {
                               if (!status.ok()) return sel->error(std::move(status));
                               return execute(sel);
                           }))
                .then(task(sel, [sel, this](GraphStatus status) {
                    if (!status.ok()) return sel->error(std::move(status));

                    auto val = qctx_->ectx()->getValue(sel->node()->varName());
                    auto cond = val.moveBool();
                    return doSchedule(cond ? sel->thenBody() : sel->elseBody());
                }));
        }
        case PlanNode::Kind::kLoop: {
            auto loop = static_cast<LoopExecutor *>(executor);
            return doScheduleParallel(loop->depends()).then(task(loop,
                        [loop, this](GraphStatus status) {
                            if (!status.ok()) return loop->error(std::move(status));
                            return iterate(loop);
                        }));
        }
        case PlanNode::Kind::kPassThrough: {
            auto mout = static_cast<PassThroughExecutor *>(executor);
            auto it = passThroughPromiseMap_.find(mout->node()->varName());
            CHECK(it != passThroughPromiseMap_.end());

            auto &data = it->second;

            folly::SpinLockGuard g(data.lock);
            if (data.numOutputs == 0) {
                // Reset promise of output executors when it's in loop
                data.numOutputs = static_cast<int32_t>(mout->successors().size());
                data.promise = std::make_unique<folly::SharedPromise<GraphStatus>>();
            }

            data.numOutputs--;
            if (data.numOutputs > 0) {
                return data.promise->getFuture();
            }

            return doScheduleParallel(mout->depends())
                .then(task(mout, [&data, mout, this](GraphStatus status) {
                    // Notify and wake up all waited tasks
                    data.promise->setValue(status);

                    if (!status.ok()) return mout->error(std::move(status));
                    return execute(mout);
                }));
        }
        default: {
            auto deps = executor->depends();
            if (deps.empty()) {
                return execute(executor);
            }

            return doScheduleParallel(deps).then(task(executor,
                        [executor, this](GraphStatus stats) {
                            if (!stats.ok()) return executor->error(std::move(stats));
                            return execute(executor);
                        }));
        }
    }
}

folly::Future<GraphStatus> Scheduler::doScheduleParallel(const std::set<Executor *> &dependents) {
    CHECK(!dependents.empty());

    std::vector<folly::Future<GraphStatus>> futures;
    for (auto dep : dependents) {
        futures.emplace_back(doSchedule(dep));
    }
    return folly::collect(futures).then([](std::vector<GraphStatus> stats) {
        for (auto &s : stats) {
            if (!s.ok()) return s;
        }
        return GraphStatus::OK();
    });
}

folly::Future<GraphStatus> Scheduler::iterate(LoopExecutor *loop) {
    return execute(loop).then(task(loop, [loop, this](GraphStatus status) {
        if (!status.ok()) return loop->error(std::move(status));

        auto val = qctx_->ectx()->getValue(loop->node()->varName());
        if (!val.isBool()) {
            std::stringstream ss;
            ss << "Loop produces a bad condition result: " << val << " type: " << val.type();
            return loop->error(GraphStatus::setInternalError(ss.str()));
        }
        auto cond = val.moveBool();
        if (!cond) return folly::makeFuture(GraphStatus::OK());
        return doSchedule(loop->loopBody()).then(task(loop, [loop, this](GraphStatus s) {
            if (!s.ok()) return loop->error(std::move(s));
            return iterate(loop);
        }));
    }));
}

folly::Future<GraphStatus> Scheduler::execute(Executor *executor) {
    auto status = executor->open();
    if (!status.ok()) {
        return executor->error(std::move(status));
    }
    return executor->execute().then([executor](GraphStatus s) {
        if (!s.ok()) {
            return s;
        }
        return executor->close();
    });
}

}   // namespace graph
}   // namespace nebula

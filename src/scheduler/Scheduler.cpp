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
#include "planner/Logic.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

Scheduler::Task::Task(const Executor *e) : planId(DCHECK_NOTNULL(e)->node()->id()) {}

Scheduler::PassThroughData::PassThroughData(int32_t outputs)
    : promise(std::make_unique<folly::SharedPromise<Status>>()), numOutputs(outputs) {}

Scheduler::Scheduler(QueryContext *qctx) : qctx_(DCHECK_NOTNULL(qctx)) {}

folly::Future<Status> Scheduler::schedule() {
    if (FLAGS_enable_lifetime_optimize) {
        qctx_->addLastUser(qctx_->plan()->root()->outputVar(), -1);  // special for root
        analyzeLifetime(qctx_->plan()->root(), qctx_);
    }
    auto executor = Executor::create(qctx_->plan()->root(), qctx_);
    analyze(executor);
    return doSchedule(executor);
}

void Scheduler::analyze(Executor *executor) {
    switch (executor->node()->kind()) {
        case PlanNode::Kind::kPassThrough: {
            const auto &name = executor->node()->outputVar();
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


void Scheduler::analyzeLifetime(const PlanNode *node, QueryContext *qctx, bool inLoop) {
    for (const auto& inputVar : node->inputVars()) {
        if (inputVar != nullptr) {
            DCHECK_NOTNULL(qctx)->addLastUser(inputVar->name,
                                              (node->kind() == PlanNode::Kind::kLoop || inLoop) ?
                                                  -1 : node->id());
        }
    }
    switch (node->kind()) {
        case PlanNode::Kind::kSelect: {
            auto sel = static_cast<const Select *>(node);
            analyzeLifetime(sel->then(), qctx, inLoop);
            analyzeLifetime(sel->otherwise(), qctx, inLoop);
            break;
        }
        case PlanNode::Kind::kLoop: {
            auto loop = static_cast<const Loop *>(node);
            DCHECK_NOTNULL(qctx)->addLastUser(loop->outputVar(), -1);
            analyzeLifetime(loop->body(), qctx, true);
            break;
        }
        default:
            break;
    }

    for (auto dep : node->dependencies()) {
        analyzeLifetime(dep, qctx, inLoop);
    }
}

folly::Future<Status> Scheduler::doSchedule(Executor *executor) {
    switch (executor->node()->kind()) {
        case PlanNode::Kind::kSelect: {
            auto sel = static_cast<SelectExecutor *>(executor);
            return doScheduleParallel(sel->depends())
                .then(task(sel,
                           [sel, this](Status status) {
                               if (!status.ok()) return sel->error(std::move(status));
                               return execute(sel);
                           }))
                .then(task(sel, [sel, this](Status status) {
                    if (!status.ok()) return sel->error(std::move(status));

                    auto val = qctx_->ectx()->getValue(sel->node()->outputVar());
                    auto cond = val.moveBool();
                    return doSchedule(cond ? sel->thenBody() : sel->elseBody());
                }));
        }
        case PlanNode::Kind::kLoop: {
            auto loop = static_cast<LoopExecutor *>(executor);
            return doScheduleParallel(loop->depends()).then(task(loop, [loop, this](Status status) {
                if (!status.ok()) return loop->error(std::move(status));
                return iterate(loop);
            }));
        }
        case PlanNode::Kind::kPassThrough: {
            auto mout = static_cast<PassThroughExecutor *>(executor);
            auto it = passThroughPromiseMap_.find(mout->node()->outputVar());
            CHECK(it != passThroughPromiseMap_.end());

            auto &data = it->second;

            folly::SpinLockGuard g(data.lock);
            if (data.numOutputs == 0) {
                // Reset promise of output executors when it's in loop
                data.numOutputs = static_cast<int32_t>(mout->successors().size());
                data.promise = std::make_unique<folly::SharedPromise<Status>>();
            }

            data.numOutputs--;
            if (data.numOutputs > 0) {
                return data.promise->getFuture();
            }

            return doScheduleParallel(mout->depends())
                .then(task(mout, [&data, mout, this](Status status) {
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

            return doScheduleParallel(deps).then(task(executor, [executor, this](Status stats) {
                if (!stats.ok()) return executor->error(std::move(stats));
                return execute(executor);
            }));
        }
    }
}

folly::Future<Status> Scheduler::doScheduleParallel(const std::set<Executor *> &dependents) {
    CHECK(!dependents.empty());

    std::vector<folly::Future<Status>> futures;
    for (auto dep : dependents) {
        futures.emplace_back(doSchedule(dep));
    }
    return folly::collect(futures).then([](std::vector<Status> stats) {
        for (auto &s : stats) {
            if (!s.ok()) return s;
        }
        return Status::OK();
    });
}

folly::Future<Status> Scheduler::iterate(LoopExecutor *loop) {
    return execute(loop).then(task(loop, [loop, this](Status status) {
        if (!status.ok()) return loop->error(std::move(status));

        auto val = qctx_->ectx()->getValue(loop->node()->outputVar());
        if (!val.isBool()) {
            std::stringstream ss;
            ss << "Loop produces a bad condition result: " << val << " type: " << val.type();
            return loop->error(Status::Error(ss.str()));
        }
        auto cond = val.moveBool();
        if (!cond) return folly::makeFuture(Status::OK());
        return doSchedule(loop->loopBody()).then(task(loop, [loop, this](Status s) {
            if (!s.ok()) return loop->error(std::move(s));
            return iterate(loop);
        }));
    }));
}

folly::Future<Status> Scheduler::execute(Executor *executor) {
    auto status = executor->open();
    if (!status.ok()) {
        return executor->error(std::move(status));
    }
    return executor->execute().then([executor](Status s) {
        NG_RETURN_IF_ERROR(s);
        return executor->close();
    });
}

}   // namespace graph
}   // namespace nebula

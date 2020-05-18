/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "schedule/Scheduler.h"

#include "exec/ExecutionError.h"
#include "exec/Executor.h"
#include "exec/logic/LoopExecutor.h"
#include "exec/logic/MultiOutputsExecutor.h"
#include "exec/logic/SelectExecutor.h"
#include "planner/PlanNode.h"
#include "service/ExecutionContext.h"

namespace nebula {
namespace graph {

Scheduler::Task::Task(const Executor *e) : planId(DCHECK_NOTNULL(e)->node()->id()) {}

Scheduler::Scheduler(ExecutionContext *ectx) : ectx_(DCHECK_NOTNULL(ectx)) {}

void Scheduler::analyze(Executor *executor) {
    if (executor->node()->kind() == PlanNode::Kind::kMultiOutputs) {
        const auto &name = executor->node()->varName();
        auto it = multiOutputPromiseMap_.find(name);
        if (it == multiOutputPromiseMap_.end()) {
            MultipleData data(executor->successors().size());
            multiOutputPromiseMap_.emplace(name, std::move(data));
        }
    }

    for (auto dep : executor->depends()) {
        analyze(dep);
    }
}

folly::Future<Status> Scheduler::schedule(Executor *executor) {
    switch (executor->node()->kind()) {
        case PlanNode::Kind::kSelector: {
            auto sel = static_cast<SelectExecutor *>(executor);
            return schedule(sel->depends())
                .then(task(sel,
                           [sel, this](Status status) {
                               if (!status.ok()) return error(std::move(status));
                               return sel->execute();
                           }))
                .then(task(sel, [sel, this](Status status) {
                    if (!status.ok()) return error(std::move(status));

                    auto val = ectx_->getValue(sel->node()->varName());
                    auto cond = val.moveBool();
                    return schedule(cond ? sel->thenBody() : sel->elseBody());
                }));
        }
        case PlanNode::Kind::kLoop: {
            auto loop = static_cast<LoopExecutor *>(executor);
            return schedule(loop->depends()).then(task(loop, [loop, this](Status status) {
                if (!status.ok()) return error(std::move(status));
                return iterate(loop);
            }));
        }
        case PlanNode::Kind::kMultiOutputs: {
            auto mout = static_cast<MultiOutputsExecutor *>(executor);
            auto it = multiOutputPromiseMap_.find(mout->node()->varName());
            CHECK(it != multiOutputPromiseMap_.end());

            auto &data = it->second;
            bool runnable = false;
            {
                folly::SpinLockGuard g(data.lock);
                if (data.numOutputs == 0) {
                    // Reset executor outputs executor status when it's in loop
                    data.numOutputs = static_cast<int32_t>(mout->successors().size());
                    data.promise = std::make_unique<folly::SharedPromise<Status>>();
                }

                data.numOutputs--;
                runnable = data.numOutputs > 0;
            }

            if (!runnable) {
                return data.promise->getFuture();
            }

            return schedule(mout->depends()).then([&data, mout, this](Status status) {
                // Notify and wake up all wait tasks
                data.promise->setValue(status);

                if (!status.ok()) return error(std::move(status));

                return mout->execute();
            });
        }
        default: {
            auto deps = executor->depends();
            if (deps.empty()) return executor->execute();

            return schedule(deps).then(task(executor, [executor, this](Status stats) {
                if (!stats.ok()) return error(std::move(stats));
                return executor->execute();
            }));
        }
    }
}

folly::Future<Status> Scheduler::schedule(const std::set<Executor *> &dependents) {
    CHECK(!dependents.empty());

    std::vector<folly::Future<Status>> futures;
    for (auto dep : dependents) {
        futures.emplace_back(schedule(dep));
    }
    return folly::collect(futures).then([this](std::vector<Status> stats) {
        for (auto s : stats) {
            if (!s.ok()) return error(std::move(s));
        }
        return folly::makeFuture(Status::OK());
    });
}

folly::Future<Status> Scheduler::error(Status status) const {
    return folly::makeFuture<Status>(ExecutionError(std::move(status)));
}

folly::Future<Status> Scheduler::iterate(LoopExecutor *loop) {
    return loop->execute().then(task(loop, [loop, this](Status status) {
        if (!status.ok()) return error(std::move(status));

        auto val = ectx_->getValue(loop->node()->varName());
        auto cond = val.moveBool();
        if (!cond) return folly::makeFuture(Status::OK());
        return schedule(loop->loopBody()).then(task(loop, [loop, this](Status s) {
            if (!s.ok()) return error(std::move(s));
            return iterate(loop);
        }));
    }));
}

}   // namespace graph
}   // namespace nebula

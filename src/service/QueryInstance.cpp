/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "service/QueryInstance.h"

#include "common/base/Base.h"
#include "executor/ExecutionError.h"
#include "executor/Executor.h"
#include "parser/ExplainSentence.h"
#include "planner/ExecutionPlan.h"
#include "planner/PlanNode.h"
#include "scheduler/Scheduler.h"
#include "validator/Validator.h"

namespace nebula {
namespace graph {

void QueryInstance::execute() {
    GraphStatus status = validateAndOptimize();
    if (!status.ok()) {
        onError(std::move(status));
        return;
    }

    if (!explainOrContinue()) {
        onFinish();
        return;
    }

    scheduler_->schedule()
        .then([this](GraphStatus s) {
            if (s.ok()) {
                this->onFinish();
            } else {
                this->onError(std::move(s));
            }
        })
        .onError([this](const ExecutionError &e) { onError(e.status()); })
        .onError([this](const std::exception &e) {
            onError(GraphStatus::setInternalError(folly::stringPrintf("%s", e.what())));
        });
}

GraphStatus QueryInstance::validateAndOptimize() {
    auto *rctx = qctx()->rctx();
    VLOG(1) << "Parsing query: " << rctx->query();
    auto result = GQLParser().parse(rctx->query());
    if (!result.ok()) {
        return GraphStatus::setSyntaxError(result.status().toString());
    }
    sentence_ = std::move(result).value();

    auto status = Validator::validate(sentence_.get(), qctx());
    if (!status.ok()) {
        return status;
    }

    // TODO: optional optimize for plan.

    return GraphStatus::OK();
}

bool QueryInstance::explainOrContinue() {
    if (sentence_->kind() != Sentence::Kind::kExplain) {
        return true;
    }
    qctx_->fillPlanDescription();
    return static_cast<const ExplainSentence *>(sentence_.get())->isProfile();
}

void QueryInstance::onFinish() {
    auto rctx = qctx()->rctx();
    VLOG(1) << "Finish query: " << rctx->query();
    auto ectx = qctx()->ectx();
    auto latency = rctx->duration().elapsedInUSec();
    rctx->resp().set_latency_in_us(latency);
    auto &spaceName = rctx->session()->spaceName();
    rctx->resp().set_space_name(spaceName);
    auto name = qctx()->plan()->root()->varName();
    if (ectx->exist(name)) {
        auto &&value = ectx->moveValue(name);
        if (value.type() == Value::Type::DATASET) {
            auto result = value.moveDataSet();
            if (!result.colNames.empty()) {
                rctx->resp().set_data(std::move(result));
            } else {
                LOG(ERROR) << "Empty column name list";
                rctx->resp().set_error_code(nebula::cpp2::ErrorCode::E_INTERNAL_ERROR);
                rctx->resp().set_error_msg("InternalError: Empty column name list");
            }
        }
    }

    if (qctx()->planDescription() != nullptr) {
        rctx->resp().set_plan_desc(std::move(*qctx()->planDescription()));
    }

    rctx->finish();

    // The `QueryInstance' is the root node holding all resources during the execution.
    // When the whole query process is done, it's safe to release this object, as long as
    // no other contexts have chances to access these resources later on,
    // e.g. previously launched uncompleted async sub-tasks, EVEN on failures.
    delete this;
}

void QueryInstance::onError(GraphStatus status) {
    LOG(ERROR) << status;
    auto *rctx = qctx()->rctx();
    rctx->resp().set_error_code(status.getErrorCode());
    auto &spaceName = rctx->session()->spaceName();
    rctx->resp().set_space_name(spaceName);
    rctx->resp().set_error_msg(status.toString());
    auto latency = rctx->duration().elapsedInUSec();
    rctx->resp().set_latency_in_us(latency);
    rctx->finish();
    delete this;
}

}   // namespace graph
}   // namespace nebula

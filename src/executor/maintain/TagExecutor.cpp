/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/maintain/TagExecutor.h"
#include "context/QueryContext.h"
#include "planner/Maintain.h"
#include "util/SchemaUtil.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<GraphStatus> CreateTagExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *ctNode = asNode<CreateTag>(node());
    auto spaceId = qctx()->rctx()->session()->space();
    return qctx()->getMetaClient()->createTagSchema(spaceId,
            ctNode->getName(), ctNode->getSchema(), ctNode->getIfNotExists())
            .via(runner())
            .then([this, ctNode, spaceId](auto &&resp) {
                auto gStatus = checkMetaResp(resp, ctNode->getName());
                if (!gStatus.ok()) {
                    return gStatus;
                }
                return GraphStatus::OK();
            });
}

folly::Future<GraphStatus> DescTagExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *dtNode = asNode<DescTag>(node());
    auto spaceId = qctx()->rctx()->session()->space();
    return qctx()
        ->getMetaClient()
        ->getTagSchema(spaceId, dtNode->getName())
        .via(runner())
        .then([this, dtNode, spaceId](auto &&resp) {
            auto gStatus = checkMetaResp(resp, dtNode->getName());
            if (!gStatus.ok()) {
                return gStatus;
            }
            auto ds = SchemaUtil::toDescSchema(resp.value().get_schema());
            return finish(ResultBuilder()
                              .value(std::move(ds))
                              .iter(Iterator::Kind::kDefault)
                              .finish());
        });
}

folly::Future<GraphStatus> DropTagExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *dtNode = asNode<DropTag>(node());
    auto spaceId = qctx()->rctx()->session()->space();
    return qctx()->getMetaClient()->dropTagSchema(spaceId,
                                                  dtNode->getName(),
                                                  dtNode->getIfExists())
            .via(runner())
            .then([this, dtNode, spaceId](auto &&resp) {
                return checkMetaResp(resp, dtNode->getName());
            });
}

folly::Future<GraphStatus> ShowTagsExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto spaceId = qctx()->rctx()->session()->space();
    return qctx()->getMetaClient()->listTagSchemas(spaceId).via(runner()).then(
        [this, spaceId](auto &&resp) {
            auto gStatus = checkMetaResp(resp);
            if (!gStatus.ok()) {
                return gStatus;
            }
            auto tagItems = resp.value().get_tags();

            DataSet dataSet;
            dataSet.colNames = {"Name"};
            std::set<std::string> orderTagNames;
            for (auto &tag : tagItems) {
                orderTagNames.emplace(tag.get_tag_name());
            }
            for (auto &name : orderTagNames) {
                Row row;
                row.values.emplace_back(name);
                dataSet.rows.emplace_back(std::move(row));
            }
            return finish(ResultBuilder()
                              .value(Value(std::move(dataSet)))
                              .iter(Iterator::Kind::kDefault)
                              .finish());
        });
}

folly::Future<GraphStatus> ShowCreateTagExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *sctNode = asNode<ShowCreateTag>(node());
    auto spaceId = qctx()->rctx()->session()->space();
    return qctx()->getMetaClient()->getTagSchema(spaceId, sctNode->getName())
            .via(runner())
            .then([this, sctNode, spaceId](auto &&resp) {
                auto gStatus = checkMetaResp(resp, sctNode->getName());
                if (!gStatus.ok()) {
                    return gStatus;
                }
                auto ds = SchemaUtil::toShowCreateSchema(
                        true, sctNode->getName(), resp.value().get_schema());
                return finish(ResultBuilder()
                                  .value(std::move(ds))
                                  .iter(Iterator::Kind::kDefault)
                                  .finish());
            });
}

folly::Future<GraphStatus> AlterTagExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *aeNode = asNode<AlterTag>(node());
    return qctx()->getMetaClient()->alterTagSchema(aeNode->space(),
                                                   aeNode->getName(),
                                                   aeNode->getSchemaItems(),
                                                   aeNode->getSchemaProp())
            .via(runner())
            .then([this, aeNode](auto &&resp) {
                return checkMetaResp(resp, aeNode->getName());
            });
}
}   // namespace graph
}   // namespace nebula

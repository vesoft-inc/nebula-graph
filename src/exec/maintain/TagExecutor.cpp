/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/maintain/TagExecutor.h"
#include "planner/Maintain.h"
#include "util/SchemaUtil.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> CreateTagExecutor::execute() {
    dumpLog();

    auto *ctNode = asNode<CreateTag>(node());
    return qctx()->getMetaClient()->createTagSchema(ctNode->space(),
            ctNode->getName(), ctNode->getSchema(), ctNode->getIfNotExists())
            .via(runner())
            .then([](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}

folly::Future<Status> DescTagExecutor::execute() {
    dumpLog();

    auto *dtNode = asNode<DescTag>(node());
    return qctx()->getMetaClient()->getTagSchema(dtNode->getSpaceId(), dtNode->getName())
            .via(runner())
            .then([this](StatusOr<meta::cpp2::Schema> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                auto ret = SchemaUtil::toDescSchema(resp.value());
                if (!ret.ok()) {
                    LOG(ERROR) << ret.status();
                    return ret.status();
                }
                finish(Value(std::move(ret).value()));
                return Status::OK();
            });
}

folly::Future<Status> DropTagExecutor::execute() {
    dumpLog();

    auto *dtNode = asNode<DropTag>(node());
    return qctx()->getMetaClient()->dropTagSchema(dtNode->getSpaceId(),
                                                  dtNode->getName(),
                                                  dtNode->getIfExists())
            .via(runner())
            .then([this](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}

folly::Future<Status> ShowTagsExecutor::execute() {
    dumpLog();

    auto *stNode = asNode<ShowTags>(node());
    return qctx()->getMetaClient()->listTagSchemas(stNode->getSpaceId())
            .via(runner())
            .then([this](StatusOr<std::vector<meta::cpp2::TagItem>> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                auto tagItems = std::move(resp).value();

                DataSet dataSet;
                dataSet.colNames = {"Name"};
                std::set<std::string> orderTagNames;
                for (auto &tag : tagItems) {
                    orderTagNames.emplace(tag.get_tag_name());
                }
                for (auto &name : orderTagNames) {
                    Row row;
                    row.columns.emplace_back(name);
                    dataSet.rows.emplace_back(std::move(row));
                }
                finish(dataSet);
                return Status::OK();
            });
}

folly::Future<Status> ShowCreateTagExecutor::execute() {
    dumpLog();

    auto *sctNode = asNode<ShowCreateTag>(node());
    return qctx()->getMetaClient()->getTagSchema(sctNode->getSpaceId(), sctNode->getName())
            .via(runner())
            .then([this, sctNode](StatusOr<meta::cpp2::Schema> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                auto ret = SchemaUtil::toShowCreateSchema(true, sctNode->getName(), resp.value());
                if (!ret.ok()) {
                    LOG(ERROR) << ret.status();
                    return ret.status();
                }
                finish(Value(std::move(ret).value()));
                return Status::OK();
            });
}
}   // namespace graph
}   // namespace nebula

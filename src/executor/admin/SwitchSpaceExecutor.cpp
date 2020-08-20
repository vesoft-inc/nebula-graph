/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/admin/SwitchSpaceExecutor.h"

#include "common/clients/meta/MetaClient.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "context/QueryContext.h"
#include "planner/Query.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> SwitchSpaceExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *spaceToNode = asNode<SwitchSpace>(node());
    auto spaceName = spaceToNode->getSpaceName();
    return qctx()->getMetaClient()->getSpace(spaceName)
            .via(runner())
            .then([spaceName, this](StatusOr<meta::cpp2::SpaceItem> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }

                SpaceInfo spaceInfo;

                auto spaceId = resp.value().get_space_id();

                const auto& properties = resp.value().get_properties();
                meta::SpaceDesc spaceDesc;
                spaceDesc.spaceName_ = properties.get_space_name();
                spaceDesc.partNum_ = properties.get_partition_num();
                spaceDesc.replicaFactor_ = properties.get_replica_factor();
                spaceDesc.charsetName_ = properties.get_charset_name();
                spaceDesc.collationName_ = properties.get_collate_name();
                spaceDesc.vidSize_ = properties.get_vid_size();
                if (properties.get_vid_type() == meta::cpp2::PropertyType::STRING) {
                    spaceDesc.vidType_ = Value::Type::STRING;
                } else if (properties.get_vid_type() == meta::cpp2::PropertyType::INT64) {
                    spaceDesc.vidType_ = Value::Type::INT;
                } else {
                    std::stringstream ss;
                    ss << "Unsupported vid type: "
                        << meta::cpp2::_PropertyType_VALUES_TO_NAMES.at(properties.get_vid_type());
                    LOG(ERROR) << ss.str();
                    return Status::Error(ss.str());
                }

                spaceInfo.id = spaceId;
                spaceInfo.name = spaceName;
                spaceInfo.spaceDesc = std::move(spaceDesc);
                qctx_->rctx()->session()->setSpace(std::move(spaceInfo));
                LOG(INFO) << "Graph space switched to `" << spaceName
                          << "', space id: " << spaceId;
                return Status::OK();
            });
}

}   // namespace graph
}   // namespace nebula

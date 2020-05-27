/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/admin/ShowExecutor.h"

#include "planner/Admin.h"
#include "service/ExecutionContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> ShowExecutor::execute() {
    auto *showNode = asNode<Show>(node());
    switch (DCHECK_NOTNULL(showNode)->showKind()) {
    case Show::ShowKind::kUnknown:
        return Status::NotSupported("Unknown show command");
    case Show::ShowKind::kHosts:
        return showHosts();
        // no default so the compiler will warning when lack
    }
    return Status::NotSupported("Unknown show command %d", static_cast<int>(showNode->showKind()));
}

folly::Future<Status> ShowExecutor::showHosts() {
    static constexpr char kNoPartition[]        = "No valid partition";
    static constexpr char kPartitionDelimeter[] = ", ";
    return ectx()
        ->getMetaClient()
        ->listHosts()
        .via(runner())
        .then([this](auto &&resp) {
            if (!resp.ok()) {
                LOG(ERROR) << resp.status();
                return resp.status();
            }
            auto    value = std::move(resp).value();
            DataSet v({"Host",
                       "Port",
                       "Status",
                       "Leader count",
                       "Leader distribution",
                       "Partition distribution"});
            for (const auto &host : value) {
                nebula::Row r({host.get_hostAddr().host,
                               host.get_hostAddr().port,
                               meta::cpp2::_HostStatus_VALUES_TO_NAMES.at(host.get_status()),
                               static_cast<int64_t>(host.get_leader_parts().size())});
                std::stringstream leaders;
                std::stringstream parts;
                std::size_t i = 0;
                for (const auto &l : host.get_leader_parts()) {
                    leaders << l.first << ":" << l.second.size();
                    if (i < host.get_leader_parts().size() - 1) {
                        leaders << kPartitionDelimeter;
                    }
                    ++i;
                }
                if (host.get_leader_parts().empty()) {
                    leaders << kNoPartition;
                }
                i = 0;
                for (const auto &p : host.get_all_parts()) {
                    parts << p.first << ":" << p.second.size();
                    if (i < host.get_leader_parts().size() - 1) {
                        parts << kPartitionDelimeter;
                    }
                    ++i;
                }
                if (host.get_all_parts().empty()) {
                    parts << kNoPartition;
                }
                r.emplace_back(leaders.str());
                r.emplace_back(parts.str());
                v.emplace_back(std::move(r));
            }  // row loop
            finish(std::move(v));
            return Status::OK();
        });
}

}  // namespace graph
}  // namespace nebula

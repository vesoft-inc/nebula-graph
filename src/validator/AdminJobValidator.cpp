/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "validator/AdminJobValidator.h"
#include "planner/Admin.h"

namespace nebula {
namespace graph {

Status AdminJobValidator::validateImpl() {
    return Status::OK();
}

Status AdminJobValidator::toPlan() {
    meta::cpp2::AdminCmd cmd = meta::cpp2::AdminCmd::UNKNOWN;
    std::vector<std::string> params;
    if (!sentence_->getParas().empty()) {
        folly::split(" ", sentence_->getParas().front(), params, true);
    }
    if (sentence_->getType() == meta::cpp2::AdminJobOp::ADD) {
        if (params.front() == "compact") {
            cmd = meta::cpp2::AdminCmd::COMPACT;
        } else if (params.front() == "flush") {
            cmd = meta::cpp2::AdminCmd::FLUSH;
        } else {
            DLOG(FATAL) << "Unknown job command " << params.front();
            return Status::Error("Unknown job command %s", params.front().c_str());
        }
        params.erase(params.begin());
    }
    auto *doNode = SubmitJob::make(qctx_, nullptr, sentence_->getType(), cmd, std::move(params));
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

}  // namespace graph
}  // namespace nebula

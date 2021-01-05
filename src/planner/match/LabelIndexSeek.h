/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MATCH_LABELINDEXSEEK_H_
#define PLANNER_MATCH_LABELINDEXSEEK_H_

#include "common/utils/IndexUtils.h"

#include "planner/match/StartVidFinder.h"

namespace nebula {
namespace graph {

/*
 * The LabelIndexSeek was designed to find if could get the starting vids by tag index.
 */
class LabelIndexSeek final : public StartVidFinder {
public:
    static std::unique_ptr<LabelIndexSeek> make() {
        return std::unique_ptr<LabelIndexSeek>(new LabelIndexSeek());
    }

private:
    LabelIndexSeek() = default;

    bool matchNode(NodeContext* nodeCtx) override;

    bool matchEdge(EdgeContext* edgeCtx) override;

    StatusOr<SubPlan> transformNode(NodeContext* nodeCtx) override;

    StatusOr<SubPlan> transformEdge(EdgeContext* edgeCtx) override;

    static StatusOr<IndexID> pickTagIndex(const NodeContext* nodeCtx);

    static std::pair<std::shared_ptr<meta::cpp2::IndexItem>, std::size_t> selectIndex(
        const std::pair<std::shared_ptr<meta::cpp2::IndexItem>, std::size_t> candidate,
        const std::shared_ptr<meta::cpp2::IndexItem> income) {
        // shorter index key is better
        std::size_t incomeSize = fieldsIndexSize(income->get_fields());
        if (candidate.second > incomeSize) {
            return std::make_pair(income, incomeSize);
        }
        return candidate;
    }

    static std::size_t fieldsIndexSize(const std::vector<meta::cpp2::ColumnDef>& fields) {
        std::size_t len{0};
        for (const auto& f : fields) {
            len += IndexUtils::fieldIndexLen(f.get_type());
        }
        return len;
    }
};

}   // namespace graph
}   // namespace nebula
#endif   // PLANNER_MATCH_LABELINDEXSEEK_H_

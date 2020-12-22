/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MATCH_VERTEXIDSEEK_H_
#define PLANNER_MATCH_VERTEXIDSEEK_H_

#include "context/ast/QueryAstContext.h"
#include "planner/match/StartVidFinder.h"

namespace nebula {
namespace graph {
/*
 * The VertexIdSeek was designed to find if could get the starting vids in filter.
 */
class VertexIdSeek final : public StartVidFinder {
public:
    struct VidPattern {
        enum class IN {
            kIgnore,
            kOtherSource,  // e.g. PropertiesSeek
            kIn,
            kNotIn,
        } in{IN::kIgnore};
        List vids;
    };

    static std::unique_ptr<VertexIdSeek> make() {
        return std::unique_ptr<VertexIdSeek>(new VertexIdSeek());
    }

    bool matchNode(NodeContext* nodeCtx) override;

    bool matchEdge(EdgeContext* edgeCtx) override;

    StatusOr<List> extractVids(const Expression* filter);

    static VidPattern reverseEvalVids(const Expression *filter);

    static VidPattern intersect(VidPattern &left, VidPattern &right);

    StatusOr<SubPlan> transformNode(NodeContext* nodeCtx) override;

    StatusOr<SubPlan> transformEdge(EdgeContext* edgeCtx) override;

    std::pair<std::string, Expression*> listToAnnoVarVid(QueryContext* qctx, const List& list);

    std::pair<std::string, Expression*> constToAnnoVarVid(QueryContext* qctx, const Value& v);

private:
    VertexIdSeek() = default;
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_MATCH_VERTEXIDSEEK_H_

/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_MATCHVALIDATOR_H_
#define VALIDATOR_MATCHVALIDATOR_H_

#include "common/base/Base.h"
#include "validator/TraversalValidator.h"
#include "util/AnonVarGenerator.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

struct MatchAstContext;

class MatchValidator final : public TraversalValidator {
public:
    using Direction = MatchEdge::Direction;
    struct NodeInfo {
        TagID                                   tid{0};
        bool                                    anonymous{false};
        const std::string                      *label{nullptr};
        const std::string                      *alias{nullptr};
        const MapExpression                    *props{nullptr};
        Expression                             *filter{nullptr};
    };

    struct EdgeInfo {
        bool                                    anonymous{false};
        std::vector<EdgeType>                   edgeTypes;
        MatchEdge::Direction                    direction{MatchEdge::Direction::OUT_EDGE};
        std::vector<std::string>                types;
        const std::string                      *alias{nullptr};
        const MapExpression                    *props{nullptr};
        Expression                             *filter{nullptr};
    };

    enum AliasType {
        kNode, kEdge, kPath
    };

    struct ScanInfo {
        Expression                             *filter{nullptr};
        int32_t                                 schemaId{0};
    };

    enum class QueryEntry {
        kId,  // query start by id
        kIndex  // query start by index scan
    };

    MatchValidator(Sentence *sentence, QueryContext *context);

private:
    Status validateImpl() override;

    Status toPlan() override;

    AstContext* getAstContext() override;

    Status validatePath(const MatchPath *path);

    Status validateFilter(const Expression *filter);

    Status validateReturn(MatchReturn *ret);

    Status validateAliases(const std::vector<const Expression*> &exprs) const;

    Status analyzeStartPoint();

    Status ananyzeFilterForIndexing();

    StatusOr<Expression*> makeSubFilter(const std::string &alias,
                                        const MapExpression *map) const;

    Expression* makeIndexFilter(const std::string &label,
                                const MapExpression *map) const;
    Expression* makeIndexFilter(const std::string &label,
                                const std::string &alias,
                                const Expression *filter) const;

    Status buildScanNode();

    Status buildSteps();

    Status buildStep();

    Status buildGetTailVertices();

    Status buildStepJoin();

    Status buildTailJoin();

    Status buildFilter();

    Status buildReturn();

    Expression* rewrite(const LabelExpression*) const;

    Expression* rewrite(const LabelAttributeExpression*) const;

    Status buildQueryById();

    Status buildProjectVertices();

    // extract vids from filter
    StatusOr<std::pair<std::string, Expression*>> extractVids(const Expression *filter) const;

    // TODO using unwind
    std::pair<std::string, Expression*> listToAnnoVarVid(const List &list) const;
    std::pair<std::string, Expression*> constToAnnoVarVid(const Value &list) const;

    template <typename T>
    T* saveObject(T *obj) const {
        return qctx_->objPool()->add(obj);
    }

private:
    bool                                        startFromNode_{true};
    int32_t                                     startIndex_{0};
    int32_t                                     curStep_{-1};
    PlanNode                                   *thisStepRoot_{nullptr};
    PlanNode                                   *prevStepRoot_{nullptr};
    Expression                                 *startExpr_{nullptr};
    Expression                                 *gnSrcExpr_{nullptr};
    AnonVarGenerator                           *anon_{nullptr};
    QueryEntry                                  entry_{QueryEntry::kId};

    std::unique_ptr<MatchAstContext>            matchCtx_;
};

struct MatchAstContext final : AstContext {
    std::vector<MatchValidator::NodeInfo>                       nodeInfos;
    std::vector<MatchValidator::EdgeInfo>                       edgeInfos;
    MatchValidator::ScanInfo                                    scanInfo;
    std::unordered_map<std::string, MatchValidator::AliasType>  aliases;
    std::unique_ptr<Expression>                                 filter;
    const YieldColumns                                         *yieldColumns;
};
}   // namespace graph
}   // namespace nebula

#endif  // VALIDATOR_MATCHVALIDATOR_H_

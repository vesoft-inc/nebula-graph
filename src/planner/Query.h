/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_QUERY_H_
#define PLANNER_QUERY_H_

#include "common/base/Base.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "common/function/AggregateFunction.h"

#include "planner/PlanNode.h"
#include "planner/ExecutionPlan.h"
#include "parser/Clauses.h"
#include "parser/TraverseSentences.h"

/**
 * All query-related nodes would be put in this file,
 * and they are derived from PlanNode.
 */
namespace nebula {
namespace graph {

class StartNode final : public PlanNode {
public:
    static StartNode* make(ExecutionPlan* plan) {
        return new StartNode(plan);
    }

    std::string explain() const override {
        return "Start";
    }

private:
    explicit StartNode(ExecutionPlan* plan) : PlanNode(plan, Kind::kStart) {
    }
};

// Dependencies will cover the inputs, For example bi input require bi dependencies as least,
// but single dependencies may don't need any inputs (I.E admin plan node)

// Single dependecy without input
// It's useful for addmin plan node
class SingleDependencyNode : public PlanNode {
public:
    const PlanNode* dep() const {
        return dependency_;
    }

    void setDep(PlanNode *dep) {
        dependency_ = DCHECK_NOTNULL(dep);
    }

protected:
    SingleDependencyNode(ExecutionPlan *plan, Kind kind, const PlanNode *dep)
        : PlanNode(plan, kind), dependency_(dep) {}

    const PlanNode *dependency_;
};

class SingleInputNode : public SingleDependencyNode {
public:
    void setInputVar(std::string inputVar) {
        inputVar_ = std::move(inputVar);
    }

    const std::string& inputVar() const {
        return inputVar_;
    }

protected:
    SingleInputNode(ExecutionPlan* plan, Kind kind, const PlanNode* dep)
        : SingleDependencyNode(plan, kind, dep) {
    }

    // Datasource for this node.
    std::string inputVar_;
};

class BiInputNode : public PlanNode {
public:
    void setLeft(PlanNode* left) {
        left_ = left;
    }

    void setRight(PlanNode* right) {
        right_ = right;
    }

    void setLeftVar(std::string leftVar) {
        leftVar_ = std::move(leftVar);
    }

    void setRightVar(std::string rightVar) {
        rightVar_ = std::move(rightVar);
    }

    const PlanNode* left() const {
        return left_;
    }

    const PlanNode* right() const {
        return right_;
    }

    const std::string& leftInputVar() const {
        return leftVar_;
    }

    const std::string& rightInputVar() const {
        return rightVar_;
    }

    std::string explain() const override {
        return "";
    }

protected:
    BiInputNode(ExecutionPlan* plan, Kind kind, PlanNode* left, PlanNode* right)
        : PlanNode(plan, kind), left_(left), right_(right) {
    }

    PlanNode* left_{nullptr};
    PlanNode* right_{nullptr};
    // Datasource for this node.
    std::string leftVar_;
    std::string rightVar_;
};

/**
 * This operator is used for multi output situation.
 */
class MultiOutputsNode final : public SingleInputNode {
public:
    static MultiOutputsNode* make(ExecutionPlan* plan, PlanNode* input) {
        return new MultiOutputsNode(input, plan);
    }

    std::string explain() const override {
        return "MultiOutputsNode";
    }

private:
    MultiOutputsNode(PlanNode* input, ExecutionPlan* plan)
        : SingleInputNode(plan, Kind::kMultiOutputs, input) {}
};

/**
 * Now we hava four kind of exploration nodes:
 *  GetNeighbors,
 *  GetVertices,
 *  GetEdges,
 *  ReadIndex
 */
class Explore : public SingleInputNode {
public:
    GraphSpaceID space() const {
        return space_;
    }

    bool dedup() const {
        return dedup_;
    }

    int64_t limit() const {
        return limit_;
    }

    const std::string& filter() const {
        return filter_;
    }

    const std::vector<storage::cpp2::OrderBy>& orderBy() const {
        return orderBy_;
    }

protected:
    Explore(ExecutionPlan* plan,
            Kind kind,
            PlanNode* input,
            GraphSpaceID space,
            bool dedup,
            int64_t limit,
            std::string filter,
            std::vector<storage::cpp2::OrderBy> orderBy)
        : SingleInputNode(plan, kind, input),
          space_(space),
          dedup_(dedup),
          limit_(limit),
          filter_(std::move(filter)),
          orderBy_(std::move(orderBy)) {}

    Explore(ExecutionPlan* plan, Kind kind, PlanNode* input, GraphSpaceID space)
        : SingleInputNode(plan, kind, input), space_(space) {}

protected:
    GraphSpaceID        space_;
    bool dedup_{false};
    int64_t limit_{std::numeric_limits<int64_t>::max()};
    std::string filter_;
    std::vector<storage::cpp2::OrderBy> orderBy_;
};

/**
 * Get neighbors' property
 */
class GetNeighbors final : public Explore {
public:
    using VertexProps = std::unique_ptr<std::vector<storage::cpp2::VertexProp>>;
    using EdgeProps = std::unique_ptr<std::vector<storage::cpp2::EdgeProp>>;
    using StatProps = std::unique_ptr<std::vector<storage::cpp2::StatProp>>;
    using Exprs = std::unique_ptr<std::vector<storage::cpp2::Expr>>;
    static GetNeighbors* make(ExecutionPlan* plan,
                              PlanNode* input,
                              GraphSpaceID space,
                              Expression* src,
                              std::vector<EdgeType> edgeTypes,
                              storage::cpp2::EdgeDirection edgeDirection,
                              VertexProps&& vertexProps,
                              EdgeProps&& edgeProps,
                              StatProps&& statProps,
                              Exprs&& exprs,
                              bool dedup = false,
                              bool random = false,
                              std::vector<storage::cpp2::OrderBy> orderBy = {},
                              int64_t limit = std::numeric_limits<int64_t>::max(),
                              std::string filter = "") {
        return new GetNeighbors(
                plan,
                input,
                space,
                src,
                std::move(edgeTypes),
                edgeDirection,
                std::move(vertexProps),
                std::move(edgeProps),
                std::move(statProps),
                std::move(exprs),
                dedup,
                random,
                std::move(orderBy),
                limit,
                std::move(filter));
    }

    std::string explain() const override;

    Expression* src() const {
        return src_;
    }

    storage::cpp2::EdgeDirection edgeDirection() const {
        return edgeDirection_;
    }

    const std::vector<EdgeType>& edgeTypes() const {
        return edgeTypes_;
    }

    const std::vector<storage::cpp2::VertexProp>* vertexProps() const {
        return vertexProps_.get();
    }

    const std::vector<storage::cpp2::EdgeProp>* edgeProps() const {
        return edgeProps_.get();
    }

    const std::vector<storage::cpp2::StatProp>* statProps() const {
        return statProps_.get();
    }

    const std::vector<storage::cpp2::Expr>* exprs() const {
        return exprs_.get();
    }

    bool random() const {
        return random_;
    }

private:
    GetNeighbors(ExecutionPlan* plan,
                 PlanNode* input,
                 GraphSpaceID space,
                 Expression* src,
                 std::vector<EdgeType> edgeTypes,
                 storage::cpp2::EdgeDirection edgeDirection,
                 VertexProps&& vertexProps,
                 EdgeProps&& edgeProps,
                 StatProps&& statProps,
                 Exprs&&  exprs,
                 bool dedup,
                 bool random,
                 std::vector<storage::cpp2::OrderBy> orderBy,
                 int64_t limit,
                 std::string filter)
        : Explore(plan,
                  Kind::kGetNeighbors,
                  input,
                  space,
                  dedup,
                  limit,
                  std::move(filter),
                  std::move(orderBy)) {
        src_ = src;
        edgeTypes_ = std::move(edgeTypes);
        edgeDirection_ = edgeDirection;
        vertexProps_ = std::move(vertexProps);
        edgeProps_ = std::move(edgeProps);
        exprs_ = std::move(exprs);
        statProps_ = std::move(statProps);
        random_ = random;
    }

private:
    Expression*                                  src_{nullptr};
    std::vector<EdgeType>                        edgeTypes_;
    storage::cpp2::EdgeDirection                 edgeDirection_;
    VertexProps                                  vertexProps_;
    EdgeProps                                    edgeProps_;
    StatProps                                    statProps_;
    Exprs                                        exprs_;
    bool                                         random_;
};

/**
 * Get property with given vertex keys.
 */
class GetVertices final : public Explore {
public:
    static GetVertices* make(ExecutionPlan* plan,
                             PlanNode* input,
                             GraphSpaceID space,
                             std::vector<Row> vertices,
                             Expression* src,
                             std::vector<storage::cpp2::VertexProp> props,
                             std::vector<storage::cpp2::Expr>       exprs,
                             bool dedup = false,
                             std::vector<storage::cpp2::OrderBy> orderBy = {},
                             int64_t limit = std::numeric_limits<int64_t>::max(),
                             std::string filter = "") {
        return new GetVertices(
                plan,
                input,
                space,
                std::move(vertices),
                src,
                std::move(props),
                std::move(exprs),
                dedup,
                std::move(orderBy),
                limit,
                std::move(filter));
    }

    std::string explain() const override;

    const std::vector<Row>& vertices() const {
        return vertices_;
    }

    Expression* src() const {
        return src_;
    }

    const std::vector<storage::cpp2::VertexProp>& props() const {
        return props_;
    }

    const std::vector<storage::cpp2::Expr>& exprs() const {
        return exprs_;
    }

private:
    GetVertices(ExecutionPlan* plan,
                PlanNode* input,
                GraphSpaceID space,
                std::vector<Row> vertices,
                Expression* src,
                std::vector<storage::cpp2::VertexProp> props,
                std::vector<storage::cpp2::Expr>       exprs,
                bool dedup,
                std::vector<storage::cpp2::OrderBy> orderBy,
                int64_t limit,
                std::string filter)
        : Explore(plan,
                  Kind::kGetVertices,
                  input,
                  space,
                  dedup,
                  limit,
                  std::move(filter),
                  std::move(orderBy)),
          vertices_(std::move(vertices)),
          src_(src),
          props_(std::move(props)),
          exprs_(std::move(exprs)) { }

private:
    // vertices are parsing from query.
    std::vector<Row>                         vertices_;
    // vertices may be parsing from runtime.
    Expression*                              src_{nullptr};
    // props of the vertex
    std::vector<storage::cpp2::VertexProp>   props_;
    // expression to get
    std::vector<storage::cpp2::Expr>         exprs_;
};

/**
 * Get property with given edge keys.
 */
class GetEdges final : public Explore {
public:
    static GetEdges* make(ExecutionPlan* plan,
                          PlanNode* input,
                          GraphSpaceID space,
                          std::vector<Row> edges,
                          Expression* src,
                          EdgeType    type,
                          Expression* ranking,
                          Expression* dst,
                          std::vector<storage::cpp2::EdgeProp> props,
                          std::vector<storage::cpp2::Expr>     exprs,
                          bool dedup = false,
                          int64_t limit = std::numeric_limits<int64_t>::max(),
                          std::vector<storage::cpp2::OrderBy> orderBy = {},
                          std::string filter = "") {
        return new GetEdges(
                plan,
                input,
                space,
                std::move(edges),
                src,
                type,
                ranking,
                dst,
                std::move(props),
                std::move(exprs),
                dedup,
                limit,
                std::move(orderBy),
                std::move(filter));
    }

    std::string explain() const override;

    const std::vector<Row>& edges() const {
        return edges_;
    }

    Expression* src() const {
        return src_;
    }

    EdgeType type() const {
        return type_;
    }

    Expression* ranking() const {
        return ranking_;
    }

    Expression* dst() const {
        return dst_;
    }

    const std::vector<storage::cpp2::EdgeProp>& props() const {
        return props_;
    }

    const std::vector<storage::cpp2::Expr>& exprs() const {
        return exprs_;
    }

private:
    GetEdges(ExecutionPlan* plan,
             PlanNode* input,
             GraphSpaceID space,
             std::vector<Row> edges,
             Expression* src,
             EdgeType    type,
             Expression* ranking,
             Expression* dst,
             std::vector<storage::cpp2::EdgeProp> props,
             std::vector<storage::cpp2::Expr>     exprs,
             bool dedup,
             int64_t limit,
             std::vector<storage::cpp2::OrderBy> orderBy,
             std::string filter)
        : Explore(plan,
                  Kind::kGetEdges,
                  input,
                  space,
                  dedup,
                  limit,
                  std::move(filter),
                  std::move(orderBy)),
          edges_(std::move(edges)),
          src_(src),
          type_(type),
          ranking_(ranking),
          dst_(dst),
          props_(std::move(props)),
          exprs_(std::move(exprs)) { }

private:
    // edges_ are parsing from the query.
    std::vector<Row>                         edges_;
    // edges_ may be parsed from runtime.
    Expression*                              src_{nullptr};
    EdgeType                                 type_{0};
    Expression*                              ranking_{nullptr};
    Expression*                              dst_{nullptr};
    // props of edge to get
    std::vector<storage::cpp2::EdgeProp>     props_;
    // expression to show
    std::vector<storage::cpp2::Expr>         exprs_;
};

/**
 * Read data through the index.
 */
class ReadIndex final : public Explore {
public:
    ReadIndex(ExecutionPlan* plan, PlanNode* input, GraphSpaceID space)
        : Explore(plan, Kind::kReadIndex, input, space) {}

    std::string explain() const override;
};

/**
 * A Filter node helps filt some records with condition.
 */
class Filter final : public SingleInputNode {
public:
    static Filter* make(ExecutionPlan* plan,
                        PlanNode* input,
                        Expression* condition) {
        return new Filter(plan, input, condition);
    }

    Expression* condition() const {
        return condition_;
    }

    std::string explain() const override;

private:
    Filter(ExecutionPlan* plan, PlanNode* input, Expression* condition)
      : SingleInputNode(plan, Kind::kFilter, input) {
        condition_ = condition;
    }

private:
    Expression*                 condition_{nullptr};
};

/**
 * Now we have three kind of set operations:
 *   UNION,
 *   INTERSECT,
 *   MINUS
 */
class SetOp : public BiInputNode {
protected:
    SetOp(ExecutionPlan* plan, Kind kind, PlanNode* left, PlanNode* right)
        : BiInputNode(plan, kind, left, right) {
        DCHECK(kind == Kind::kUnion || kind == Kind::kIntersect || kind == Kind::kMinus);
    }
};

/**
 * Combine two set of records.
 */
class Union final : public SetOp {
public:
    static Union* make(ExecutionPlan* plan, PlanNode* left, PlanNode* right) {
        return new Union(plan, left, right);
    }

    std::string explain() const override;

private:
    Union(ExecutionPlan* plan, PlanNode* left, PlanNode* right)
        : SetOp(plan, Kind::kUnion, left, right) {}
};

/**
 * Return the intersected records between two sets.
 */
class Intersect final : public SetOp {
public:
    static Intersect* make(ExecutionPlan* plan, PlanNode* left, PlanNode* right) {
        return new Intersect(plan, left, right);
    }

    std::string explain() const override;

private:
    Intersect(ExecutionPlan* plan, PlanNode* left, PlanNode* right)
        : SetOp(plan, Kind::kIntersect, left, right) {}
};

/**
 * Do subtraction between two sets.
 */
class Minus final : public SetOp {
public:
    static Minus* make(ExecutionPlan* plan, PlanNode* left, PlanNode* right) {
        return new Minus(plan, left, right);
    }

    std::string explain() const override;

private:
    Minus(ExecutionPlan* plan, PlanNode* left, PlanNode* right)
        : SetOp(plan, Kind::kMinus, left, right) {}
};

/**
 * Project is used to specify output vars or field.
 */
class Project final : public SingleInputNode {
public:
    static Project* make(ExecutionPlan* plan,
                         PlanNode* input,
                         YieldColumns* cols) {
        return new Project(plan, input, cols);
    }

    std::string explain() const override;

    const YieldColumns* columns() const {
        return cols_;
    }

private:
    Project(ExecutionPlan* plan, PlanNode* input, YieldColumns* cols)
      : SingleInputNode(plan, Kind::kProject, input), cols_(cols) { }

private:
    YieldColumns*               cols_{nullptr};
};

/**
 * Sort the given record set.
 */
class Sort final : public SingleInputNode {
public:
    static Sort* make(ExecutionPlan* plan,
                      PlanNode* input,
                      std::vector<std::pair<std::string, OrderFactor::OrderType>> factors) {
        return new Sort(plan, input, std::move(factors));
    }

    const std::vector<std::pair<std::string, OrderFactor::OrderType>>& factors() const {
        return factors_;
    }

    std::string explain() const override;

private:
    Sort(ExecutionPlan* plan,
         PlanNode* input,
         std::vector<std::pair<std::string, OrderFactor::OrderType>> factors)
        : SingleInputNode(plan, Kind::kSort, input) {
        factors_ = std::move(factors);
    }

private:
    std::vector<std::pair<std::string, OrderFactor::OrderType>>   factors_;
};

/**
 * Output the records with the given limitation.
 */
class Limit final : public SingleInputNode {
public:
    static Limit* make(ExecutionPlan* plan,
                       PlanNode* input,
                       int64_t offset,
                       int64_t count) {
        return new Limit(plan, input, offset, count);
    }

    int64_t offset() const {
        return offset_;
    }

    int64_t count() const {
        return count_;
    }

    std::string explain() const override;

private:
    Limit(ExecutionPlan* plan, PlanNode* input, int64_t offset, int64_t count)
        : SingleInputNode(plan, Kind::kLimit, input) {
        offset_ = offset;
        count_ = count;
    }

private:
    int64_t     offset_{-1};
    int64_t     count_{-1};
};

/**
 * Do Aggregation with the given set of records,
 * such as AVG(), COUNT()...
 */
class Aggregate final : public SingleInputNode {
public:
    struct GroupItem {
        GroupItem(Expression* e, AggFun::Function f, bool d)
            : expr(e), func(f), distinct(d) {}
        Expression* expr;
        AggFun::Function func;
        bool distinct = false;
    };
    static Aggregate* make(ExecutionPlan* plan,
                           PlanNode* input,
                           std::vector<Expression*>&& groupKeys,
                           std::vector<GroupItem>&& groupItems) {
        return new Aggregate(plan, input, std::move(groupKeys), std::move(groupItems));
    }

    const std::vector<Expression*>& groupKeys() const {
        return groupKeys_;
    }

    const std::vector<GroupItem>& groupItems() const {
        return groupItems_;
    }

    std::string explain() const override;

private:
    Aggregate(ExecutionPlan* plan,
              PlanNode* input,
              std::vector<Expression*>&& groupKeys,
              std::vector<GroupItem>&& groupItems)
        : SingleInputNode(plan, Kind::kAggregate, input) {
        groupKeys_ = std::move(groupKeys);
        groupItems_ = std::move(groupItems);
    }

private:
    std::vector<Expression*>    groupKeys_;
    std::vector<GroupItem>      groupItems_;
};

class BinarySelect : public SingleInputNode {
public:
    Expression* condition() const {
        return condition_;
    }

protected:
    BinarySelect(ExecutionPlan* plan, Kind kind, PlanNode* input, Expression* condition)
        : SingleInputNode(plan, kind, input), condition_(condition) {}

    Expression*  condition_{nullptr};
};

class Select final : public BinarySelect {
public:
    static Select* make(ExecutionPlan* plan,
                          PlanNode* input,
                          PlanNode* ifBranch,
                          PlanNode* elseBranch,
                          Expression* condition) {
        return new Select(plan, input, ifBranch, elseBranch, condition);
    }

    void setIf(PlanNode* ifBranch) {
        if_ = ifBranch;
    }

    void setElse(PlanNode* elseBranch) {
        else_ = elseBranch;
    }

    std::string explain() const override;

    const PlanNode* then() const {
        return if_;
    }

    const PlanNode* otherwise() const {
        return else_;
    }

private:
    Select(ExecutionPlan* plan,
           PlanNode* input,
           PlanNode* ifBranch,
           PlanNode* elseBranch,
           Expression* condition)
        : BinarySelect(plan, Kind::kSelect, input, condition) {
        if_ = ifBranch;
        else_ = elseBranch;
    }

private:
    PlanNode*   if_{nullptr};
    PlanNode*   else_{nullptr};
};

class Loop final : public BinarySelect {
public:
    static Loop* make(ExecutionPlan* plan,
                      PlanNode* input,
                      PlanNode* body,
                      Expression* condition) {
        return new Loop(plan, input, body, condition);
    }

    void setBody(PlanNode* body) {
        body_ = body;
    }

    std::string explain() const override;

    const PlanNode* body() const {
        return body_;
    }

private:
    Loop(ExecutionPlan* plan, PlanNode* input, PlanNode* body, Expression* condition);

    PlanNode*   body_{nullptr};
};

class SwitchSpace final : public SingleInputNode {
public:
    static SwitchSpace* make(ExecutionPlan* plan,
                             PlanNode* input,
                             std::string spaceName) {
        return new SwitchSpace(plan, input, spaceName);
    }

    const std::string& getSpaceName() const {
        return spaceName_;
    }

    std::string explain() const override;

private:
    SwitchSpace(ExecutionPlan* plan,
                PlanNode* input,
                std::string spaceName)
        : SingleInputNode(plan, Kind::kSwitchSpace, input) {
        spaceName_ = std::move(spaceName);
    }

private:
    std::string     spaceName_;
};

class Dedup final : public SingleInputNode {
public:
    static Dedup* make(ExecutionPlan* plan,
                       PlanNode* input) {
        return new Dedup(plan, input);
    }

    std::string explain() const override;

private:
    Dedup(ExecutionPlan* plan,
          PlanNode* input)
        : SingleInputNode(plan, Kind::kDedup, input) {
    }
};

class DataCollect final : public SingleInputNode {
public:
    enum class CollectKind : uint8_t {
        kSubgraph,
        kRowBasedMove,
    };

    static DataCollect* make(ExecutionPlan* plan,
                             PlanNode* input,
                             CollectKind collectKind,
                             std::vector<std::string> vars) {
        return new DataCollect(plan, input, collectKind, std::move(vars));
    }

    CollectKind collectKind() const {
        return collectKind_;
    }

    const std::vector<std::string>& vars() const {
        return vars_;
    }

private:
    DataCollect(ExecutionPlan* plan,
                PlanNode* input,
                CollectKind collectKind,
                std::vector<std::string> vars)
        : SingleInputNode(plan, Kind::kDataCollect, input) {
        collectKind_ = collectKind;
        vars_ = std::move(vars);
    }

    std::string explain() const override;

private:
    CollectKind                 collectKind_;
    std::vector<std::string>    vars_;
};

class ProduceSemiShortestPath : public PlanNode {
};

class ConjunctPath : public PlanNode {
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_QUERY_H_

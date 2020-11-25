/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLANNERS_MATCH_STARTVIDFINDER_H_
#define PLANNER_PLANNERS_MATCH_STARTVIDFINDER_H_
namespace nebula {
namespace graph {
class StartVidFinder;

using StartVidFinderMatchFunc = std::function<bool(PatternContext* ctx)>;
using StartVidFinderInstantiateFunc = std::function<std::unique_ptr<StartVidFinder>()>;
struct FinderMatchAndInstantiate {
    StartVidFinderMatchFunc match;
    StartVidFinderInstantiateFunc instantiate;
};

class StartVidFinder {
public:
    virtual ~StartVidFinder() = default;

    static auto& finders() {
        static std::vector<FinderMatchAndInstantiate> finders;
        return finders;
    }

    virtual StatusOr<SubPlan> transform(PatternContext* context) = 0;

private:
    StartVidFinder() = default;
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_PLANNERS_MATCH_STARTVIDFINDER_H_

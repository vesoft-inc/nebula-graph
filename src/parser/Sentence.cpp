/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "parser/Sentence.h"

namespace nebula {

/*static*/ std::string Sentence::toString(Kind kind) {
    switch (kind) {
        case Kind::kUnknown:
            return "Unknown";
        case Kind::kSequential:
            return "Sequential";
        case Kind::kGo:
            return "Go";
        case Kind::kSet:
            return "Set";
        case Kind::kPipe:
            return "Pipe";
        case Kind::kUse:
            return "Use";
        case Kind::kMatch:
            return "Match";
        case Kind::kAssignment:
            return "Assignment";
        case Kind::kCreateTag:
            return "CreateTag";
        case Kind::kAlterTag:
            return "AlterTag";
        case Kind::kCreateEdge:
            return "CreateEdge";
        case Kind::kAlterEdge:
            return "AlterEdge";
        case Kind::kDescribeTag:
            return "DescribeTag";
        case Kind::kDescribeEdge:
            return "DescribeEdge";
        case Kind::kCreateTagIndex:
            return "CreateTagIndex";
        case Kind::kCreateEdgeIndex:
            return "CreateEdgeIndex";
        case Kind::kDropTagIndex:
            return "DropTagIndex";
        case Kind::kDropEdgeIndex:
            return "DropEdgeIndex";
        case Kind::kDescribeTagIndex:
            return "DescribeTagIndex";
        case Kind::kDescribeEdgeIndex:
            return "DescribeEdgeIndex";
        case Kind::kRebuildTagIndex:
            return "RebuildTagIndex";
        case Kind::kRebuildEdgeIndex:
            return "RebuildEdgeIndex";
        case Kind::kDropTag:
            return "RebuildEdgeIndex";
        case Kind::kDropEdge:
            return "DropEdge";
        case Kind::kInsertVertices:
            return "InsertVertices";
        case Kind::kUpdateVertex:
            return "UpdateVertex";
        case Kind::kInsertEdges:
            return "InsertEdges";
        case Kind::kUpdateEdge:
            return "UpdateEdge";
        case Kind::kShowHosts:
            return "ShowHosts";
        case Kind::kShowSpaces:
            return "ShowSpaces";
        case Kind::kShowParts:
            return "ShowParts";
        case Kind::kShowTags:
            return "ShowTags";
        case Kind::kShowEdges:
            return "ShowEdges";
        case Kind::kShowTagIndexes:
            return "ShowTagIndexes";
        case Kind::kShowEdgeIndexes:
            return "ShowEdgeINdexes";
        case Kind::kShowUsers:
            return "ShowUsers";
        case Kind::kShowRoles:
            return "ShowRoles";
        case Kind::kShowCreateSpace:
            return "ShowCreateSpace";
        case Kind::kShowCreateTag:
            return "ShowCreateTag";
        case Kind::kShowCreateEdge:
            return "ShowCreateEdge";
        case Kind::kShowCreateTagIndex:
            return "ShowCreatTagIndex";
        case Kind::kShowCreateEdgeIndex:
            return "ShowCreateEdgeIndex";
        case Kind::kShowTagIndexStatus:
            return "ShowTagINdexStatus";
        case Kind::kShowEdgeIndexStatus:
            return "ShowEdgeINdexStatus";
        case Kind::kShowSnapshots:
            return "ShowSnapshots";
        case Kind::kShowCharset:
            return "ShowCharset";
        case Kind::kShowCollation:
            return "ShowCollation";
        case Kind::kDeleteVertex:
            return "DeleteVertex";
        case Kind::kDeleteEdges:
            return "DeleteEdges";
        case Kind::kLookup:
            return "Lookup";
        case Kind::kCreateSpace:
            return "CreateSpace";
        case Kind::kDropSpace:
            return "DropSpace";
        case Kind::kDescribeSpace:
            return "DescribeSpace";
        case Kind::kYield:
            return "Yield";
        case Kind::kCreateUser:
            return "CreateUser";
        case Kind::kDropUser:
            return "DropUser";
        case Kind::kAlterUser:
            return "AlterUser";
        case Kind::kGrant:
            return "Grant";
        case Kind::kRevoke:
            return "Revoke";
        case Kind::kChangePassword:
            return "ChangePassword";
        case Kind::kDownload:
            return "Download";
        case Kind::kIngest:
            return "Ingest";
        case Kind::kOrderBy:
            return "OrderBy";
        case Kind::kConfig:
            return "Config";
        case Kind::kFetchVertices:
            return "FetchVertices";
        case Kind::kFetchEdges:
            return "FetchEdges";
        case Kind::kBalance:
            return "Balance";
        case Kind::kFindPath:
            return "FindPath";
        case Kind::kLimit:
            return "Limit";
        case Kind::KGroupBy:
            return "GroupBy";
        case Kind::kReturn:
            return "Return";
        case Kind::kCreateSnapshot:
            return "CreateSnapshot";
        case Kind::kDropSnapshot:
            return "DropSnapshot";
        case Kind::kAdmin:
            return "Admin";
        case Kind::kGetSubgraph:
            return "GetSubgraph";
    }
    LOG(FATAL) << "Impossible sentence " << static_cast<int>(kind);
}

}   // namespace nebula

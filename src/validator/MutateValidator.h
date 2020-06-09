/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_MUTATEVALIDATOR_H
#define VALIDATOR_MUTATEVALIDATOR_H
#include "common/base/Base.h"
#include "validator/Validator.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "parser/MutateSentences.h"
#include "parser/TraverseSentences.h"

namespace nebula {
namespace graph {
class InsertVerticesValidator final : public Validator {
public:
    InsertVerticesValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

    Status check();

    Status prepareVertices();

private:
    using TagSchema = std::shared_ptr<const meta::SchemaProviderIf>;
    GraphSpaceID                                                spaceId_{-1};
    std::vector<VertexRowItem*>                                 rows_;
    std::unordered_map<TagID, std::vector<std::string>>         tagPropNames_;
    std::vector<std::pair<TagID, TagSchema>>                    schemas_;
    uint16_t                                                    propSize_{0};
    bool                                                        overwritable_{false};
    std::vector<storage::cpp2::NewVertex>                       vertices_;
};

class InsertEdgesValidator final : public Validator {
public:
    InsertEdgesValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

    Status check();

    Status prepareEdges();

private:
    GraphSpaceID                                      spaceId_{-1};
    bool                                              overwritable_{true};
    EdgeType                                          edgeType_{-1};
    std::shared_ptr<const meta::SchemaProviderIf>     schema_;
    std::vector<std::string>                          propNames_;
    std::vector<EdgeRowItem*>                         rows_;
    std::vector<storage::cpp2::NewEdge>               edges_;
};

<<<<<<< HEAD
class DeleteVerticesValidator final : public Validator {
public:
    DeleteVerticesValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
=======
class UpdateBaseValidator : public Validator {
public:
    explicit UpdateBaseValidator(Sentence* sentence, QueryContext* context)
            : Validator(sentence, context) {
        sentence_ = static_cast<UpdateBaseSentence*>(sentence);
    }

    virtual ~UpdateBaseValidator() {}

protected:
    void getCondition();

    void getReturnProps();

    Status getUpdateProps();

protected:
    UpdateBaseSentence                                 *sentence_{nullptr};
    bool                                                insertable_;
    std::vector<std::string>                            returnProps_;
    std::vector<std::string>                            yieldProps_;
    std::string                                         condition_;
    std::vector<storage::cpp2::UpdatedProp>             updatedProps_;
};

class UpdateVertexValidator final : public UpdateBaseValidator {
public:
    UpdateVertexValidator(Sentence* sentence, QueryContext* context)
            : UpdateBaseValidator(sentence, context) {
        sentence_ = static_cast<UpdateVertexSentence*>(sentence);
>>>>>>> add update executor and test
    }

private:
    Status validateImpl() override;

<<<<<<< HEAD
    std::string buildVIds();

    Status toPlan() override;

private:
    GraphSpaceID                                  spaceId_{-1};
    // From ConstantExpression
    std::vector<VertexID>                         vertices_;
    // From InputPropertyExpression or InputPropertyExpression
    Expression*                                   vidRef_{nullptr};
    std::vector<EdgeType>                         edgeTypes_;
    std::vector<std::string>                      edgeNames_;
    std::vector<EdgeKeyRef*>                      edgeKeyRefs_;
};

class DeleteEdgesValidator final : public Validator {
public:
    DeleteEdgesValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
=======
    Status toPlan() override;

private:
    UpdateVertexSentence                               *sentence_{nullptr};
    Expression*                                         vId_{nullptr};
    TagID                                               tagId_{-1};
};

class UpdateEdgeValidator final : public UpdateBaseValidator {
public:
    UpdateEdgeValidator(Sentence* sentence, QueryContext* context)
            : UpdateBaseValidator(sentence, context) {
        sentence_ = static_cast<UpdateEdgeSentence*>(sentence);
>>>>>>> add update executor and test
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

<<<<<<< HEAD
<<<<<<< HEAD
    Status checkInput();

    Status buildEdgeKeyRef(const std::vector<EdgeKey*> &edgeKeys,
                           const EdgeType edgeType);

private:
    // From InputPropertyExpression, ConstantExpression will covert to  InputPropertyExpression
    std::vector<EdgeKeyRef*>                       edgeKeyRefs_;
    std::string                                    edgeKeyVar_;
=======
    Status getUpdateProps();

=======
>>>>>>> update expression
private:
    UpdateEdgeSentence                               *sentence_{nullptr};
    Expression*                                       srcId_{nullptr};
    Expression*                                       dstId_{nullptr};
    int64_t                                           rank_{0};
    EdgeType                                          edgeType_{-1};
<<<<<<< HEAD
    std::vector<storage::cpp2::UpdatedEdgeProp>       updatedProps_;
>>>>>>> add update executor and test
=======
>>>>>>> update expression
};
}  // namespace graph
}  // namespace nebula
#endif  // VALIDATOR_MUTATEVALIDATOR_H

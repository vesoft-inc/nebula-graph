/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "util/IdGenerator.h"

#include "base/Logging.h"

namespace nebula {

// static
IdGenerator* IdGenerator::get(IdGenerator::Type type) {
    switch (type) {
        case IdGenerator::Type::SEQUENCE: {
          return SequenceIdGenerator::instance();
        }
        default: {
            LOG(FATAL) << "Invalid id generator type";
            return nullptr;
        }
    }
}

SequenceIdGenerator* SequenceIdGenerator::instance() {
    static SequenceIdGenerator generator;
    return &generator;
}

}   // namespace nebula

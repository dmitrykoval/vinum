#pragma once

#include "agg_funcs.h"


namespace vinum::operators::aggregate {

std::shared_ptr<AbstractAggFunc> agg_func_factory(const AggFuncDef &func,
                                                  const std::shared_ptr<arrow::Schema> &schema);
}  // namespace vinum::operators::aggregate

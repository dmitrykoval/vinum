#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <arrow/python/pyarrow.h>

#include <single_numerical_hash_aggregate.h>
#include <multi_numerical_hash_aggregate.h>
#include <generic_hash_aggregate.h>
#include <one_group_aggregate.h>

#include <sort.cpp>

#include <table_batch_reader.h>

namespace py = pybind11;
namespace agg = vinum::operators::aggregate;
namespace sort = vinum::operators::sort;


PYBIND11_MODULE(vinum_lib, m) {

    m.def("import_pyarrow",
                &arrow::py::import_pyarrow, py::return_value_policy::copy);

    py::enum_<vinum::operators::aggregate::AggFuncType>(m, "AggFuncType")
        .value("COUNT_STAR", agg::AggFuncType::COUNT_STAR)
        .value("COUNT", agg::AggFuncType::COUNT)
        .value("MIN", agg::AggFuncType::MIN)
        .value("MAX", agg::AggFuncType::MAX)
        .value("SUM", agg::AggFuncType::SUM)
        .value("AVG", agg::AggFuncType::AVG)
        .export_values();

    py::enum_<sort::SortOrder>(m, "SortOrder")
        .value("ASC", sort::SortOrder::ASC)
        .value("DESC", sort::SortOrder::DESC)
        .export_values();

    py::class_<agg::AggFuncDef>(m, "AggFuncDef")
        .def(py::init<
                    agg::AggFuncType,
                    const std::string&,
                    const std::string&>())
        .def_readonly("column_name",
                      &agg::AggFuncDef::column_name)
        .def_readonly("out_col_name",
                      &agg::AggFuncDef::out_col_name)
        .def("__repr__", [](const agg::AggFuncDef& obj) {
            return "<AggFuncDef col_name: " + obj.column_name
                    + ", out_col_name: " + obj.out_col_name + ">";
         });


    py::class_<agg::SingleNumericalHashAggregate>(m, "SingleNumericalHashAggregate")
        .def(py::init<
                    const std::vector<std::string>&,
                    const std::vector<std::string>&,
                    const std::vector<agg::AggFuncDef>&
                    >())
        .def("next", [](agg::SingleNumericalHashAggregate &self,
                        py::handle py_batch) {
                auto batch = arrow::py::unwrap_batch(
                    py_batch.ptr()).ValueOrDie();
                return self.Next(batch);
            }
        )
        .def("result", [](agg::SingleNumericalHashAggregate &self) {
                return py::handle(arrow::py::wrap_batch(self.Result()));
            }
        )
        ;

    py::class_<agg::MultiNumericalHashAggregate>(m, "MultiNumericalHashAggregate")
        .def(py::init<
                    const std::vector<std::string>&,
                    const std::vector<std::string>&,
                    const std::vector<agg::AggFuncDef>&
                    >())
        .def("next", [](agg::MultiNumericalHashAggregate &self,
                        py::handle py_batch) {
                auto batch = arrow::py::unwrap_batch(
                    py_batch.ptr()).ValueOrDie();
                return self.Next(batch);
            }
        )
        .def("result", [](agg::MultiNumericalHashAggregate &self) {
                return py::handle(arrow::py::wrap_batch(self.Result()));
            }
        )
        ;

    py::class_<agg::GenericHashAggregate>(m, "GenericHashAggregate")
        .def(py::init<
                    const std::vector<std::string>&,
                    const std::vector<std::string>&,
                    const std::vector<agg::AggFuncDef>&
                    >())
        .def("next", [](agg::GenericHashAggregate &self,
                        py::handle py_batch) {
                auto batch = arrow::py::unwrap_batch(
                    py_batch.ptr()).ValueOrDie();
                return self.Next(batch);
            }
        )
        .def("result", [](agg::GenericHashAggregate &self) {
                return py::handle(arrow::py::wrap_batch(self.Result()));
            }
        )
        ;

    py::class_<agg::OneGroupAggregate>(m, "OneGroupAggregate")
        .def(py::init<const std::vector<agg::AggFuncDef>&>())
        .def("next", [](agg::OneGroupAggregate &self,
                        py::handle py_batch) {
                auto batch = arrow::py::unwrap_batch(
                    py_batch.ptr()).ValueOrDie();
                return self.Next(batch);
            }
        )
        .def("result", [](agg::OneGroupAggregate &self) {
                return py::handle(arrow::py::wrap_batch(self.Result()));
            }
        )
        ;

    py::class_<sort::Sort>(m, "Sort")
        .def(py::init<
                    const std::vector<std::string>&,
                    const std::vector<sort::SortOrder>&
                    >())
        .def("next", [](sort::Sort &self,
                        py::handle py_batch) {
                auto batch = arrow::py::unwrap_batch(
                    py_batch.ptr()).ValueOrDie();
                return self.Next(batch);
            }
        )
        .def("sorted", [](sort::Sort &self) {
                return py::handle(arrow::py::wrap_batch(self.Sorted()));
            }
        )
        ;

    py::class_<vinum::operators::TableBatchReader>(m, "TableBatchReader")
        .def(py::init([](py::handle table_handle) {
                auto table = arrow::py::unwrap_table(
                    table_handle.ptr()).ValueOrDie();
                return new vinum::operators::TableBatchReader(table);
            }
        ))
        .def("next", [](vinum::operators::TableBatchReader &self) {
                auto next_batch = self.Next();
                if (next_batch != nullptr) {
                    return py::handle(arrow::py::wrap_batch(next_batch));
                } else {
                    return py::handle(py::cast<py::none>(Py_None));
                }
            }
        )
        .def("set_batch_size", [](vinum::operators::TableBatchReader &self,
                                  int64_t batch_size) {
                self.SetBatchSize(batch_size);
            }
        )
        ;

}


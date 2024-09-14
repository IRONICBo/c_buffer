#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>
#include "datenlord.h"

namespace py = pybind11;
using namespace datenlord;
using namespace pybind11::literals;

std::string handle_error(datenlord_error *err) {
    if (err == nullptr) {
        return "Success";
    }
    std::string message((const char *)err->message.data, err->message.len);
    free(err);
    return message;
}

PYBIND11_MODULE(datenlord, m) {
    m.doc() = "Python bindings for datenlord SDK";

    m.attr("ROOT_ID") = ROOT_ID;
    m.attr("NEED_CHECK_PERM") = NEED_CHECK_PERM;

    py::class_<datenlord_sdk>(m, "DatenlordSDK")
        .def(py::init<>());

    m.def("init", [](const std::string &config) -> datenlord_sdk* {
        datenlord_sdk *sdk = datenlord::init(config.c_str());
        return sdk;
    }, py::return_value_policy::reference);

    m.def("free_sdk", [](datenlord_sdk *sdk) {
        free_sdk(sdk);
    });

    m.def("exists", [](datenlord_sdk *sdk, const std::string &dir_path) -> bool {
        return exists(sdk, dir_path.c_str());
    });

    m.def("mkdir", [](datenlord_sdk *sdk, const std::string &dir_path) -> std::string {
        datenlord_error *err = datenlord::mkdir(sdk, dir_path.c_str());
        return handle_error(err);
    });

    m.def("deldir", [](datenlord_sdk *sdk, const std::string &dir_path, bool recursive) -> std::string {
        datenlord_error *err = datenlord::deldir(sdk, dir_path.c_str(), recursive);
        return handle_error(err);
    });

    m.def("rename_path", [](datenlord_sdk *sdk, const std::string &src_path, const std::string &dest_path) -> std::string {
        datenlord_error *err = datenlord::rename_path(sdk, src_path.c_str(), dest_path.c_str());
        return handle_error(err);
    });

    m.def("copy_from_local_file", [](datenlord_sdk *sdk, bool overwrite, const std::string &local_file_path, const std::string &dest_file_path) -> std::string {
        datenlord_error *err = datenlord::copy_from_local_file(sdk, overwrite, local_file_path.c_str(), dest_file_path.c_str());
        return handle_error(err);
    });

    m.def("copy_to_local_file", [](datenlord_sdk *sdk, const std::string &src_file_path, const std::string &local_file_path) -> std::string {
        datenlord_error *err = datenlord::copy_to_local_file(sdk, src_file_path.c_str(), local_file_path.c_str());
        return handle_error(err);
    });

    m.def("create_file", [](datenlord_sdk *sdk, const std::string &file_path) -> std::string {
        datenlord_error *err = datenlord::create_file(sdk, file_path.c_str());
        return handle_error(err);
    });

    m.def("stat", [](datenlord_sdk *sdk, const std::string &file_path) -> py::dict {
        datenlord_file_stat stat;
        datenlord_error *err = datenlord::stat(sdk, file_path.c_str(), &stat);
        if (err != nullptr) {
            throw std::runtime_error(handle_error(err));
        }
        return py::dict(
            "ino"_a = stat.ino,
            "size"_a = stat.size,
            "blocks"_a = stat.blocks,
            "perm"_a = stat.perm,
            "nlink"_a = stat.nlink,
            "uid"_a = stat.uid,
            "gid"_a = stat.gid,
            "rdev"_a = stat.rdev
        );
    });

    m.def("write_file", [](datenlord_sdk *sdk, const std::string &file_path, const std::string &content) -> std::string {
        datenlord_bytes bytes = { reinterpret_cast<const uint8_t *>(content.c_str()), content.size() };
        datenlord_error *err = datenlord::write_file(sdk, file_path.c_str(), bytes);
        return handle_error(err);
    });

    m.def("read_file", [](datenlord_sdk *sdk, const std::string &file_path) -> py::memoryview {
        datenlord_file_stat stat;
        datenlord_error *err = datenlord::stat(sdk, file_path.c_str(), &stat);
        if (err != nullptr) {
            throw std::runtime_error(handle_error(err));
        }

        py::array_t<uint8_t> out_content(stat.size);
        uint8_t* buf = out_content.mutable_data();
        datenlord_bytes out_content_struct = {
            buf,
            stat.size
        };

        err = datenlord::read_file(sdk, file_path.c_str(), &out_content_struct);
        if (err != nullptr) {
            throw std::runtime_error(handle_error(err));
        }

        return py::memoryview(out_content);
    });
}

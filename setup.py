import os
import sys
import sysconfig
import subprocess
from shutil import copyfile

from setuptools import setup, find_packages

from pybind11.setup_helpers import Pybind11Extension, build_ext

import pyarrow as pa
pa.create_library_symlinks()

is_cibuildwheel = bool(os.environ.get('CIBUILDWHEEL', False))

with open("README.rst", "r") as f:
    long_description = f.read()

NAME = "vinum"
VERSION = "0.2.0"
AUTHOR = "Dmitry Koval"
AUTHOR_EMAIL = "dima@koval.space"
DESCRIPTION = (
    "Vinum is a SQL processor written in pure Python, "
    "designed for data analysis workflows and in-memory analytics. "
)
URL = "https://github.com/dmitrykoval/vinum"
PROJECT_URLS = {
    "Bug Tracker": "https://github.com/dmitrykoval/vinum/issues",
    "Documentation": "https://vinum.readthedocs.io/en/latest/index.html",
    "Source Code": "https://github.com/dmitrykoval/vinum",
}
INSTALL_REQUIRES = [
    "pyarrow >= 3.0.0",
    "numpy >= 1.19.0",
    "moz_sql_parser == 3.32.20026"
]
VINUM_CPP_LIB_NAME = 'vinum_cpp'


# CMakeExtension and CMakeBuild are adapted from:
# https://github.com/pybind/cmake_example
class CMakeExtension(Pybind11Extension):
    def __init__(self, name, source, *args, **kwargs):
        Pybind11Extension.__init__(self, name, source, *args, **kwargs)
        self.cmake_sourcedir = os.path.abspath(kwargs['cmake_sourcedir'])
        self.cmake_target_name = kwargs['cmake_target_name']
        self.cmake_cxx_flags = (
            kwargs['cmake_cxx_flags']
            if kwargs.get('cmake_cxx_flags')
            else []
        )


class CMakeBuild(build_ext):
    """
    CMakeBuild builds the core cpp library with Cmake first.
    Second, it build the Pybind11 wrapper and publishes it as a shared lib.
    """

    # Convert distutils Windows platform specifiers to CMake -A arguments
    MSVC_CMAKE_PLATFORM = {
        "win32": "Win32",
        "win-amd64": "x64",
        "win-arm32": "ARM",
        "win-arm64": "ARM64",
    }

    def build_extension(self, ext):
        extdir = os.path.abspath(
            os.path.dirname(self.get_ext_fullpath(ext.name))
        )

        # required for auto-detection of auxiliary "native" libs
        if not extdir.endswith(os.path.sep):
            extdir += os.path.sep

        cfg = "Debug" if self.debug else "Release"

        cmake_args = [
            f"-DCMAKE_LIBRARY_OUTPUT_DIRECTORY={extdir}",
            f"-DCMAKE_ARCHIVE_OUTPUT_DIRECTORY={extdir}",
            f"-DCMAKE_BUILD_TYPE={cfg}",  # not used on MSVC, but no harm
            f"-DARROW_INCLUDE_DIR={pa.get_include()}",
            f"-DCMAKE_VERBOSE_MAKEFILE:BOOL=ON",
        ]
        if ext.cmake_cxx_flags:
            cmake_args.append(
                f"-DCMAKE_CXX_FLAGS={' '.join(ext.cmake_cxx_flags)}"
            )

        build_args = [
            "--target",
            ext.cmake_target_name
        ]

        if self.compiler.compiler_type == "msvc":
            # CMake lets you override the generator - we need to check this.
            # Can be set with Conda-Build, for example.
            cmake_generator = os.environ.get("CMAKE_GENERATOR", "")

            # Single config generators are handled "normally"
            single_config = any(
                x in cmake_generator for x in {"NMake", "Ninja"}
            )

            # CMake allows an arch-in-generator style for backward compatibility
            contains_arch = any(
                x in cmake_generator for x in {"ARM", "Win64"}
            )

            # Specify the arch if using MSVC generator, but only if it doesn't
            # contain a backward-compatibility arch spec already in the
            # generator name.
            if not single_config and not contains_arch:
                cmake_args += ["-A",
                               CMakeBuild.MSVC_CMAKE_PLATFORM[self.plat_name]]

            # Multi-config generators have a different way to specify configs
            if not single_config:
                cmake_args += [
                    f"-DCMAKE_LIBRARY_OUTPUT_DIRECTORY_{cfg.upper()}={extdir}"
                    f"-DCMAKE_ARCHIVE_OUTPUT_DIRECTORY_{cfg.upper()}={extdir}"
                ]
                build_args += ["--config", cfg]

        # Set CMAKE_BUILD_PARALLEL_LEVEL to control the parallel build level
        # across all generators.
        if "CMAKE_BUILD_PARALLEL_LEVEL" not in os.environ:
            # self.parallel is a Python 3 only way to set parallel jobs by hand
            # using -j in the build_ext call, not supported by pip or PyPA-build.
            if hasattr(self, "parallel") and self.parallel:
                # CMake 3.12+ only.
                build_args += [f"-j{self.parallel}"]

        if not os.path.exists(self.build_temp):
            os.makedirs(self.build_temp)

        # Following two calls build the Cmake vinum_cpp project
        subprocess.check_call(
            ["cmake", ext.cmake_sourcedir] + cmake_args, cwd=self.build_temp
        )
        subprocess.check_call(
            ["cmake", "--build", ".", "--clean-first"] + build_args, cwd=self.build_temp
        )

        # This call builds the python wrapper around vinum_cpp library
        super().build_extension(ext)


def _get_distutils_build_directory():
    """
    Returns the directory distutils uses to build its files.
    We need this directory since we build extensions which have to link
    other ones.
    """
    pattern = "lib.{platform}-{major}.{minor}"
    return os.path.join(
        'build',
        pattern.format(platform=sysconfig.get_platform(),
                       major=sys.version_info[0],
                       minor=sys.version_info[1])
    )


def _copy_arrow_libs():
    files = []
    print('**> in _copy_arrow_libs')
    if is_cibuildwheel:
        copied = {lib: False for lib in pa.get_libraries()}
        for libdir in pa.get_library_dirs():
            print(f'**> libdir: {libdir}')
            if all(copied.values()):
                break
            for lib in pa.get_libraries():
                print(f'**> lib: {lib}')
                if not copied[lib]:
                    fname = f"lib{lib}.so.{pa.__version__.replace('.', '')}"
                    print(f'**> lib fname: {fname}')
                    src_path = os.path.join(libdir, fname)
                    print(f'**> testing source path: {src_path}')
                    if os.path.exists(src_path):
                        dst_path = os.path.abspath(os.path.join('.', fname))
                        copyfile(src_path, dst_path)
                        copied[lib] = True
                        files.append(fname)
                        print(f'**> copied to {dst_path}')
    return files


def create_extensions():
    cpp_lib_cxx_flags = ['-fPIC']
    python_lib_cxx_flags = []
    include_dirs = [
        pa.get_include(),
        'vinum_cpp/src/operators/aggregate',
        'vinum_cpp/src/operators/sort',
        'vinum_cpp/src/operators',
        'vinum_cpp/src/',
    ]
    library_dirs = [
        _get_distutils_build_directory()
    ]
    library_dirs.extend(pa.get_library_dirs())

    libraries = [
        VINUM_CPP_LIB_NAME, 'arrow', 'arrow_python'
    ]
    python_lib_linker_args = []
    python_lib_macros = None

    if sys.platform == 'darwin':
        python_lib_cxx_flags.append('--std=c++17')
        python_lib_cxx_flags.append('--stdlib=libc++')
        python_lib_cxx_flags.append('-mmacosx-version-min=10.9')
        python_lib_cxx_flags.append('-fvisibility=hidden')
        python_lib_linker_args.append('-Wl,-rpath,@loader_path/pyarrow')
    elif sys.platform == 'linux':
        python_lib_cxx_flags.append('--std=c++17')
        python_lib_cxx_flags.append('-fvisibility=hidden')
        if not is_cibuildwheel:
            python_lib_linker_args.append("-Wl,-rpath,$ORIGIN")
            python_lib_linker_args.append("-Wl,-rpath,$ORIGIN/pyarrow")
        python_lib_macros = ('_GLIBCXX_USE_CXX11_ABI', '0')
        cpp_lib_cxx_flags.append('-D_GLIBCXX_USE_CXX11_ABI=0')

    cpp_lib = CMakeExtension(
        "vinum_lib",
        ["vinum/core/vinum_lib.cpp"],
        cmake_sourcedir='vinum_cpp',
        cmake_target_name=VINUM_CPP_LIB_NAME,
        cmake_cxx_flags=cpp_lib_cxx_flags,
    )
    cpp_lib.include_dirs.extend(include_dirs)
    cpp_lib.libraries.extend(libraries)
    cpp_lib.library_dirs.extend(library_dirs)
    cpp_lib.extra_compile_args.extend(python_lib_cxx_flags)
    cpp_lib.extra_link_args.extend(python_lib_linker_args)
    if python_lib_macros:
        cpp_lib.define_macros.append(python_lib_macros)

    return [cpp_lib]


cmdclass = {
    "build_ext": CMakeBuild,
}

package_data = {}
if sys.platform == 'linux' and is_cibuildwheel:
    package_data['arrow'] = _copy_arrow_libs()

setup(
    name=NAME,
    version=VERSION,
    cmdclass=cmdclass,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type="text/x-rst",
    url=URL,
    project_urls=PROJECT_URLS,
    keywords=["sql", "python", "numpy", "data analysis", "olap"],
    packages=find_packages(),
    python_requires=">=3.7",
    install_requires=INSTALL_REQUIRES,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Operating System :: OS Independent",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: SQL",
        "Topic :: Scientific/Engineering",
    ],
    ext_modules=create_extensions(),
    zip_safe=False,
    package_data=package_data,
)

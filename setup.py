import os
import subprocess

from setuptools import setup, find_packages, Extension

from pybind11.setup_helpers import Pybind11Extension, build_ext

import numpy as np
import pyarrow as pa

import versioneer

pa.create_library_symlinks()

with open("README.rst", "r") as f:
    long_description = f.read()

NAME = "vinum"
VERSION = versioneer.get_version()
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
# Convert distutils Windows platform specifiers to CMake -A arguments
PLAT_TO_CMAKE = {
    "win32": "Win32",
    "win-amd64": "x64",
    "win-arm32": "ARM",
    "win-arm64": "ARM64",
}


# CMakeExtension example taken from:
# https://github.com/pybind/cmake_example

class CMakeExtension(Extension):
    def __init__(self, name, sourcedir="", target_name="",):
        Extension.__init__(self, name, sources=[])
        self.sourcedir = os.path.abspath(sourcedir)
        self.target_name = target_name


class CMakeBuild(build_ext):
    def build_extension(self, ext):
        if isinstance(ext, Pybind11Extension):
            if ext.libdirs:
                for ldir in ext.libdirs:
                    ext.library_dirs.append(
                        os.path.abspath(
                            os.path.join(self.build_temp, ldir)
                        )
                    )
            super().build_extension(ext)
            return

        extdir = os.path.abspath(os.path.dirname(self.get_ext_fullpath(ext.name)))

        # required for auto-detection of auxiliary "native" libs
        if not extdir.endswith(os.path.sep):
            extdir += os.path.sep

        cfg = "Debug" if self.debug else "Release"

        # CMake lets you override the generator - we need to check this.
        # Can be set with Conda-Build, for example.
        cmake_generator = os.environ.get("CMAKE_GENERATOR", "")

        # Set Python_EXECUTABLE instead if you use PYBIND11_FINDPYTHON
        # EXAMPLE_VERSION_INFO shows you how to pass a value into the C++ code
        # from Python.
        cmake_args = [
            f"-DCMAKE_LIBRARY_OUTPUT_DIRECTORY={extdir}",
            f"-DCMAKE_BUILD_TYPE={cfg}",  # not used on MSVC, but no harm
        ]
        build_args = [
            "--target",
            ext.target_name
        ]

        if self.compiler.compiler_type != "msvc":
            # Using Ninja-build since it a) is available as a wheel and b)
            # multithreads automatically. MSVC would require all variables be
            # exported for Ninja to pick it up, which is a little tricky to do.
            # Users can override the generator with CMAKE_GENERATOR in CMake
            # 3.15+.
            if not cmake_generator:
                cmake_args += ["-GNinja"]

        else:

            # Single config generators are handled "normally"
            single_config = any(x in cmake_generator for x in {"NMake", "Ninja"})

            # CMake allows an arch-in-generator style for backward compatibility
            contains_arch = any(x in cmake_generator for x in {"ARM", "Win64"})

            # Specify the arch if using MSVC generator, but only if it doesn't
            # contain a backward-compatibility arch spec already in the
            # generator name.
            if not single_config and not contains_arch:
                cmake_args += ["-A", PLAT_TO_CMAKE[self.plat_name]]

            # Multi-config generators have a different way to specify configs
            if not single_config:
                cmake_args += [
                    f"-DCMAKE_LIBRARY_OUTPUT_DIRECTORY_{cfg.upper()}={extdir}"
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

        subprocess.check_call(
            ["cmake", ext.sourcedir] + cmake_args, cwd=self.build_temp
        )
        subprocess.check_call(
            ["cmake", "--build", "."] + build_args, cwd=self.build_temp
        )


vinum_ext = Pybind11Extension(
    "vinum_lib",
    ["vinum/core/vinum_lib.cpp"],
    # Example: passing in the version to the compiled code
    define_macros=[('VERSION_INFO', VERSION)],
)
# Vinum dependecies for library only
vinum_ext.include_dirs.append('vinum_cpp/src/operators/aggregate')
vinum_ext.include_dirs.append('vinum_cpp/src/operators/sort')
vinum_ext.include_dirs.append('vinum_cpp/src/operators')
vinum_ext.include_dirs.append('vinum_cpp/src/')
vinum_ext.libraries.append('vinum')
vinum_ext.libdirs = ['src']


ext_modules = [
    CMakeExtension("vinum_lib", sourcedir="vinum_cpp", target_name="vinum"),
    vinum_ext,
]

# Arrow and Numpy headers/libs for all the extensions
for ext in ext_modules:
    # The Numpy C headers are currently required
    ext.include_dirs.append(np.get_include())
    ext.include_dirs.append(pa.get_include())
    ext.libraries.extend(pa.get_libraries())
    ext.library_dirs.extend(pa.get_library_dirs())

    if os.name == 'posix':
        ext.extra_compile_args.append('-std=c++17')

    # Try uncommenting the following line on Linux
    # if you get weird linker errors or runtime crashes
    # ext.define_macros.append(("_GLIBCXX_USE_CXX11_ABI", "0"))

cmdclass = versioneer.get_cmdclass()
cmdclass["build_ext"] = CMakeBuild

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
    python_requires=">=3.7.0",
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
    ext_modules=ext_modules,
    zip_safe=False,
)

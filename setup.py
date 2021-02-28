import os
import sys
import sysconfig
import subprocess

from setuptools import setup, find_packages

from pybind11.setup_helpers import Pybind11Extension, build_ext

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
VINUM_CPP_LIB_NAME = 'vinum_cpp'


# CMakeExtension example taken from:
# https://github.com/pybind/cmake_example
class CMakeExtension(Pybind11Extension):
    def __init__(self, name, source, *args, **kwargs):
        Pybind11Extension.__init__(self, name, source, *args, **kwargs)
        self.cmake_sourcedir = os.path.abspath(kwargs['cmake_sourcedir'])
        self.cmake_target_name = kwargs['cmake_target_name']


class CMakeBuild(build_ext):
    def build_extension(self, ext):
        extdir = os.path.abspath(os.path.dirname(self.get_ext_fullpath(ext.name)))

        # required for auto-detection of auxiliary "native" libs
        if not extdir.endswith(os.path.sep):
            extdir += os.path.sep
        print(f'**** LIUBDR to write: {extdir}')

        cfg = "Debug" if self.debug else "Release"

        # CMake lets you override the generator - we need to check this.
        # Can be set with Conda-Build, for example.
        cmake_generator = os.environ.get("CMAKE_GENERATOR", "")

        # Set Python_EXECUTABLE instead if you use PYBIND11_FINDPYTHON
        # EXAMPLE_VERSION_INFO shows you how to pass a value into the C++ code
        # from Python.
        cmake_args = [
            f"-DCMAKE_LIBRARY_OUTPUT_DIRECTORY={extdir}",
            f"-DCMAKE_ARCHIVE_OUTPUT_DIRECTORY={extdir}",
            # f"-DCMAKE_SHARED_LINKER_FLAGS={ext.extra_link_args}",
            # f"-DCMAKE_STATIC_LINKER_FLAGS={ext.extra_link_args}",
            f"-DCMAKE_BUILD_TYPE={cfg}",  # not used on MSVC, but no harm
            f"-DARROW_INCLUDE_DIR={pa.get_include()}",
            f"-DARROW_LIB_DIR={pa.get_library_dirs()}",
            f"-DCMAKE_VERBOSE_MAKEFILE:BOOL=ON",
        ]
        build_args = [
            "--target",
            ext.cmake_target_name
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
            ["cmake", ext.cmake_sourcedir] + cmake_args, cwd=self.build_temp
        )
        subprocess.check_call(
            ["cmake", "--build", "."] + build_args, cwd=self.build_temp
        )

        super().build_extension(ext)


def _get_distutils_build_directory():
    """
    Returns the directory distutils uses to build its files.
    We need this directory since we build extensions which have to link
    other ones.
    """
    pattern = "lib.{platform}-{major}.{minor}"
    return os.path.join('build', pattern.format(platform=sysconfig.get_platform(),
                                                major=sys.version_info[0],
                                                minor=sys.version_info[1]))

extra_compile_args = []
hidden_visibility_args = []
include_dirs = [
    pa.get_include(),
]
library_dirs = [
    _get_distutils_build_directory()
]
library_dirs.extend(pa.get_library_dirs())

libraries = [
    'arrow', 'arrow_python'
]
python_module_link_args = []
base_library_link_args = []

if sys.platform == 'darwin':
    extra_compile_args.append('--std=c++17')
    extra_compile_args.append('--stdlib=libc++')
    extra_compile_args.append('-mmacosx-version-min=10.9')
    hidden_visibility_args.append('-fvisibility=hidden')

    from distutils import sysconfig
    vars = sysconfig.get_config_vars()
    vars['LDSHARED'] = vars['LDSHARED'].replace('-bundle', '')
    python_module_link_args.append('-bundle')
    # builder = build_ext.build_ext(Distribution())
    # full_name = builder.get_ext_filename(VINUM_CPP_LIB_NAME)
    # base_library_link_args.append('-Wl,-dylib_install_name,@loader_path/{}'.format(full_name))
    # base_library_link_args.append('-dynamiclib')
elif sys.platform == 'win32':
    extra_compile_args.append('-DNOMINMAX')
else:
    extra_compile_args.append('--std=c++17')
    hidden_visibility_args.append('-fvisibility=hidden')
    python_module_link_args.append("-Wl,-rpath,$ORIGIN")


def create_extensions():
    extension_modules = []

    # For now, assume that we build against bundled pyarrow releases.
    if sys.platform == "win32":
        pass
    elif sys.platform == "darwin":
        python_module_link_args.append('-Wl,-rpath,@loader_path/pyarrow')
    else:
        python_module_link_args.append("-Wl,-rpath,$ORIGIN/pyarrow")

    cpp_lib_bindings = CMakeExtension(
        "vinum_lib",
        ["vinum/core/vinum_lib.cpp"],
        cmake_sourcedir='vinum_cpp',
        cmake_target_name=VINUM_CPP_LIB_NAME
    )
    # Vinum dependecies for library only
    cpp_lib_bindings.include_dirs.extend(
        include_dirs
        + [
            'vinum_cpp/src/operators/aggregate',
            'vinum_cpp/src/operators/sort',
            'vinum_cpp/src/operators',
            'vinum_cpp/src/'
        ]
    )
    cpp_lib_bindings.libraries.extend([VINUM_CPP_LIB_NAME] + libraries)
    cpp_lib_bindings.library_dirs.extend(library_dirs)
    print(f'**** LIBS: {cpp_lib_bindings.libraries}')
    print(f'**** LIBDIRS: {cpp_lib_bindings.library_dirs}')
    # cpp_lib_bindings.libdirs = ['src']
    cpp_lib_bindings.extra_compile_args.extend(
        extra_compile_args + hidden_visibility_args
    )
    cpp_lib_bindings.extra_link_args.extend(python_module_link_args)

    extension_modules.append(cpp_lib_bindings)

    return extension_modules

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
    ext_modules=create_extensions(),
    zip_safe=False,
)

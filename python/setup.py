#!/usr/bin/env python
import os
import sys
import json
import shutil
import glob
import setuptools
import pybind11
import platform

static_library_option = '--static-library'
static_library_native_option = '--static-library-native'
static_library_avx2_option = '--static-library-avx2'
included_library_option = '--included-library'
shared_library_without_avx_option = '--shared-library-without-avx'
shared_library_option = '--shared-library'
shared_library_avx2_option = '--shared-library-avx2'
version_file = 'VERSION'
package = 'ngt'
module = 'ngtpy'

static_library = False
if static_library_option in sys.argv:
    print('use the NGT static library')
    sys.argv.remove(static_library_option)
    static_library = True

static_library_native = False
if static_library_native_option in sys.argv:
    print('use the NGT static library with native')
    sys.argv.remove(static_library_native_option)
    static_library_native = True

static_library_avx2 = False
if static_library_avx2_option in sys.argv:
    print('use the NGT static library with avx2')
    sys.argv.remove(static_library_avx2_option)
    static_library_avx2 = True
    package = 'ngt_avx2'
    module = 'ngtpy_avx2'

included_library = False
if included_library_option in sys.argv:
    print('use the NGT included library')
    sys.argv.remove(included_library_option)
    included_library = True

shared_library_without_avx = False
if shared_library_without_avx_option in sys.argv:
    print('use the shared library without avx')
    sys.argv.remove(shared_library_without_avx_option)
    shared_library_without_avx = True

shared_library = False
if shared_library_option in sys.argv:
    print('use the shared library')
    sys.argv.remove(shared_library_option)
    shared_library = True

shared_library_avx2 = False
if shared_library_avx2_option in sys.argv:
    print('use the shared library with avx2')
    sys.argv.remove(shared_library_avx2_option)
    shared_library_avx2 = True
    package = 'ngt_avx2'
    module = 'ngtpy_avx2'

if sys.version_info.major >= 3:
    from setuptools import Extension

if os.path.isfile('../' + version_file):
    shutil.copyfile('../' + version_file, version_file)

with open(version_file, 'r') as fh:
    version = fh.read().rstrip('\n')

basedir = os.path.abspath(os.path.dirname(__file__))

gcc_compiler = True
if platform.system() == 'Darwin':
    gcc_compiler = False
    if 'CC' in os.environ:
        if 'gcc' in os.environ['CC']:
            gcc_compiler = True

if gcc_compiler:
    openmplib = 'gomp'
else:
    openmplib = 'omp'

openmp_root = None
if 'OpenMP_ROOT' in os.environ:
    openmp_root = os.environ['OpenMP_ROOT']

with open('README.md', 'r', encoding='utf-8') as fh:
    long_description = fh.read()

args = {
    'name': package,
    'version': version,
    'author': 'Yahoo! JAPAN research',
    'author_email': 'miwasaki@yahoo-corp.jp',
    'url': 'https://github.com/yahoojapan/NGT',
    'description': 'python NGT',
    'long_description': long_description,
    'long_description_content_type': 'text/markdown',
    'license': 'Apache License Version 2.0',
    'install_requires': ['numpy']
}

if sys.version_info.major >= 3:
    if static_library or included_library or shared_library_without_avx:
        params = {
            'include_dirs': ['/usr/local/include',
                             pybind11.get_include(True),
                             pybind11.get_include(False)],
            'extra_compile_args': ['-std=c++17', '-Ofast',
                                   '-march=x86-64' if platform.processor() == 'x86-64' or
                                   platform.processor() == 'x86_64' else '',
                                   '-DNDEBUG'],
            'sources': ['src/ngtpy.cpp']
        }
    elif static_library_avx2 or shared_library_avx2:
        params = {
            'include_dirs': ['/usr/local/include',
                             pybind11.get_include(True),
                             pybind11.get_include(False)],
            'extra_compile_args': ['-std=c++17', '-Ofast',
                                   '-march=haswell' if platform.processor() == 'x86-64' or
                                   platform.processor() == 'x86_64' else '',
                                   '-DNDEBUG'],
            'sources': ['src/ngtpy_avx2.cpp']
        }
    else:
        params = {
            'include_dirs': ['/usr/local/include',
                             pybind11.get_include(True),
                             pybind11.get_include(False)],
            'extra_compile_args': ['-std=c++17', '-Ofast', '-march=native', '-DNDEBUG', '-fvisibility-inlines-hidden', '-flto'],
            'sources': ['src/ngtpy.cpp']
        }
    if gcc_compiler:
        params['extra_compile_args'].append('-fopenmp')
        params['extra_compile_args'].append('-lrt')
    else:
        params['extra_compile_args'].append('-Xpreprocessor')
        params['extra_compile_args'].append('-fopenmp')

    shared_lib_params = {
        'library_dirs': ['/usr/local/lib', '/usr/local/lib64'],
        'libraries': ['ngt', openmplib, 'blas', 'lapack'],
        'extra_link_args': ['-flto']
    }
    included_lib_params = {
        'library_dirs': ['/usr/local/lib', '/usr/local/lib64'],
        'libraries': ['ngt', openmplib, 'blas', 'lapack'],
        'extra_link_args': ['-static-libstdc++']
    }
    static_lib_params = {
        'library_dirs': ['/usr/local/lib', '/usr/local/lib64'],
        'extra_objects': ['../build-ngtpy-release/lib/NGT/libngt.a'],
        'libraries': [openmplib, 'blas', 'lapack'],
    }
    if static_library or static_library_native or static_library_avx2:
        params.update(static_lib_params)
    elif included_library:
        params.update(included_lib_params)
    else:
        params.update(shared_lib_params)

    if openmp_root:
        params['include_dirs'].append(openmp_root + '/include')
        params['library_dirs'].append(openmp_root + '/lib')

    module1 = Extension(module, **params)
    args['ext_modules'] = [module1]

setup_arguments = args

if os.path.isdir('scripts'):
    setup_arguments['scripts'] = [
        os.path.join('scripts', f) for f in os.listdir('scripts')
    ]

if __name__ == '__main__':
    setuptools.setup(**setup_arguments)

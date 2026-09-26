//
// Copyright (C) 2015 Yahoo Japan Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "NGT/FileSystem.h"
#include "NGT/Common.h"

#ifdef _WIN32
#include <direct.h>
#else
#include <sys/stat.h>
#endif

namespace NGT {
namespace FileSystem {

void makeDirectory(const std::string &path) {
#ifdef _WIN32
    if (_mkdir(path.c_str()) != 0) {
#else
    if (::mkdir(path.c_str(), S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH) != 0) {
#endif
        std::stringstream msg;
        msg << "NGT::FileSystem::makeDirectory: Cannot make the specified directory. " << path;
        NGTThrowException(msg);
    }
}

} // namespace FileSystem
} // namespace NGT

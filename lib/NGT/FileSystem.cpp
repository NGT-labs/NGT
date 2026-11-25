#include "NGT/FileSystem.h"

#include <filesystem>
#include <sstream>
#include <system_error>

#include "NGT/Common.h"

namespace fs = std::filesystem;

void NGT::FileSystem::copyDirectory(const std::string &source, const std::string &destination) {
  std::error_code ec;
  if (fs::exists(destination, ec)) {
    std::stringstream msg;
    msg << "FileSystem::copyDirectory: The destination already exists. " << destination;
    NGTThrowException(msg);
  }
  fs::copy(source, destination, fs::copy_options::recursive, ec);
  if (ec) {
    std::stringstream msg;
    msg << "FileSystem::copyDirectory: Cannot copy " << source << " to " << destination << ". "
        << ec.message();
    NGTThrowException(msg);
  }
}

void NGT::FileSystem::copyFile(const std::string &source, const std::string &destination) {
  std::error_code ec;
  fs::copy_file(source, destination, fs::copy_options::overwrite_existing, ec);
  if (ec) {
    std::stringstream msg;
    msg << "FileSystem::copyFile: Cannot copy " << source << " to " << destination << ". " << ec.message();
    NGTThrowException(msg);
  }
}

void NGT::FileSystem::removeAll(const std::string &path) {
  std::error_code ec;
  fs::remove_all(path, ec);
  if (ec) {
    std::stringstream msg;
    msg << "FileSystem::removeAll: Cannot remove " << path << ". " << ec.message();
    NGTThrowException(msg);
  }
}

std::string NGT::FileSystem::createTemporaryDirectory(const std::string &prefix) {
  std::error_code ec;
  const fs::path base = fs::temp_directory_path(ec);
  if (ec) {
    std::stringstream msg;
    msg << "FileSystem::createTemporaryDirectory: Cannot find the temporary directory. " << ec.message();
    NGTThrowException(msg);
  }
  for (int trial = 0; trial < 100; trial++) {
    std::stringstream name;
    name << prefix << std::hex << NGT::threadRandomEngine()() << NGT::threadRandomEngine()();
    const fs::path dir = base / name.str();
    // create_directory() returns false without an error when the directory already exists, so an existing
    // directory is never reused.
    if (fs::create_directory(dir, ec)) {
      fs::permissions(dir, fs::perms::owner_all, fs::perm_options::replace, ec);
      if (ec) {
        fs::remove(dir, ec);
        std::stringstream msg;
        msg << "FileSystem::createTemporaryDirectory: Cannot restrict the permissions of " << dir.string()
            << ".";
        NGTThrowException(msg);
      }
      return dir.string();
    }
    if (ec) {
      std::stringstream msg;
      msg << "FileSystem::createTemporaryDirectory: Cannot create " << dir.string() << ". " << ec.message();
      NGTThrowException(msg);
    }
  }
  std::stringstream msg;
  msg << "FileSystem::createTemporaryDirectory: Cannot create a unique directory in " << base.string() << ".";
  NGTThrowException(msg);
}

#pragma once

#include <string>

// File operations without a shell, so that paths with spaces work and no cp, rm or /tmp is needed.
// std::filesystem is used only in FileSystem.cpp: this header is also compiled into the Python
// extension, whose macOS deployment target may be older than the 10.15 that std::filesystem requires.
// Every function throws NGT::Exception with the reason on failure.
namespace NGT {
namespace FileSystem {
// Copy a directory and its contents. The destination must not exist yet.
void copyDirectory(const std::string &source, const std::string &destination);
// Copy a file, replacing the destination if it exists.
void copyFile(const std::string &source, const std::string &destination);
// Remove a file or a directory and its contents. A missing path is not an error.
void removeAll(const std::string &path);
// Create a new directory that only the owner can access under the system temporary directory
// ($TMPDIR on Unix, %TEMP% on Windows), as mkdtemp() does, and return its path.
std::string createTemporaryDirectory(const std::string &prefix = "ngt-");
} // namespace FileSystem
} // namespace NGT

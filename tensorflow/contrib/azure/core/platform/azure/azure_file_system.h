/* Copyright 2017 The TensorFlow Authors. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
==============================================================================*/

#ifndef _TURN_OFF_PLATFORM_STRING
#define _TURN_OFF_PLATFORM_STRING

#ifndef TENSORFLOW_CORE_PLATFORM_AZURE_AZURE_FILE_SYSTEM_H
#define TENSORFLOW_CORE_PLATFORM_AZURE_AZURE_FILE_SYSTEM_H

#include <cstdlib>

#include <was/blob.h>
#include <was/common.h>
#include <was/storage_account.h>

#include <cpprest/containerstream.h>
#include <cpprest/filestream.h>

#include "tensorflow/core/lib/io/path.h"
#include "tensorflow/core/platform/env.h"
#include "tensorflow/core/platform/posix/error.h"

namespace tensorflow {

class AzureFileSystem : public FileSystem {
 public:
  AzureFileSystem();
  ~AzureFileSystem();

  Status NewRandomAccessFile(
      const string& fname, std::unique_ptr<RandomAccessFile>* result) override;

  Status NewWritableFile(const string& fname,
                         std::unique_ptr<WritableFile>* result) override;

  Status NewAppendableFile(const string& fname,
                           std::unique_ptr<WritableFile>* result) override;

  Status NewReadOnlyMemoryRegionFromFile(
      const string& fname,
      std::unique_ptr<ReadOnlyMemoryRegion>* result) override;

  Status FileExists(const string& fname) override;

  Status GetChildren(const string& dir, std::vector<string>* result) override;

  Status DeleteFile(const string& fname) override;

  Status CreateDir(const string& name) override;

  Status DeleteDir(const string& name) override;

  Status GetFileSize(const string& fname, uint64* size) override;

  Status RenameFile(const string& src, const string& target) override;

  Status Stat(const string& fname, FileStatistics* stat) override;

  Status GetContainer(const string& containerName,
                      azure::storage::cloud_blob_container*);

  string GetConnectionString();

 private:
  Status ContainerExists(azure::storage::cloud_blob_container* container);

  Status InitializeAzureFile(StringPiece fname, string* blob,
                             azure::storage::cloud_blob_container* container);

  Status ParseAzurePath(StringPiece fname, string* container, string* blob);

  Status CreateDirKeeper(const string& dir_name,
                         azure::storage::cloud_blob_container& container);

  bool IsADirectory(azure::storage::cloud_blob_container* container,
                    const string* blob_name);
};

}  // namespace tensorflow

#endif  // TENSORFLOW_CORE_PLATFORM_AZURE_AZURE_FILE_SYSTEM_H
#endif  //_TURN_OFF_PLATFORM_STRING

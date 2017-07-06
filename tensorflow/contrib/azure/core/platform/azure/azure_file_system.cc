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

#include "tensorflow/contrib/azure/core/platform/azure/azure_file_system.h"

namespace tensorflow {

  class AzureBlockBlobRandomAccessFile : public RandomAccessFile {
  public:
    AzureBlockBlobRandomAccessFile(
        const azure::storage::cloud_blob_container m_container,
        const azure::storage::cloud_block_blob m_blockblob,
        const size_t read_ahead_bytes)
        : container(m_container),
          blockblob(m_blockblob),
          read_ahead_bytes_(read_ahead_bytes) {}

    mutable mutex mu_;
    mutable concurrency::streams::container_buffer<std::vector<uint8_t>> buffer_
    GUARDED_BY(mu_);
    mutable size_t buffer_start_offset_ GUARDED_BY(mu_) = 0;
    const azure::storage::cloud_blob_container container;
    mutable azure::storage::cloud_block_blob blockblob;
    const size_t read_ahead_bytes_;

    Status Read(uint64 offset, size_t n, StringPiece* result,
                char* scratch) const override {
      mutex_lock lock(mu_);
      auto blobSize = static_cast<uint64>(blockblob.properties().size());
      concurrency::streams::container_buffer<std::vector<uint8_t>> lbuffer_;
      const bool range_start_included = offset >= buffer_start_offset_;
      const bool range_end_included =
          offset + n <= buffer_start_offset_ + buffer_.size();
      size_t real_n = n;

      if (range_start_included && range_end_included) {
        // The requested range can be filled from the buffer.
        const size_t offset_in_buffer =
            std::min<uint64>(offset - buffer_start_offset_, buffer_.size());
        const auto copy_size = std::min(n, buffer_.size() - offset_in_buffer);
        std::copy(buffer_.collection().begin() + offset_in_buffer,
                  buffer_.collection().begin() + offset_in_buffer + copy_size,
                  scratch);
        *result = StringPiece(scratch, copy_size);
      } else {
        if (offset + n > blobSize - 1) {
          real_n = blobSize - offset;
        }
        if (real_n > 0) {
          buffer_ = lbuffer_;
          // Update the buffer content based on the new requested range.
          const size_t desired_buffer_size = real_n + read_ahead_bytes_;
          if (real_n > buffer_.size() ||
              desired_buffer_size > 2 * buffer_.size()) {
            // Re-allocate only if buffer capacity increased significantly.
            buffer_.alloc(desired_buffer_size);
          }
          buffer_start_offset_ = offset;

          blockblob.download_range_to_stream(
              buffer_.create_ostream(), static_cast<utility::size64_t>(offset),
              static_cast<utility::size64_t>(desired_buffer_size));

          // Set the results.
          std::memcpy(scratch, buffer_.collection().data(),
                      std::min(buffer_.size(), real_n));
          *result = StringPiece(scratch, std::min(buffer_.size(), real_n));
        } else {
          *result = StringPiece();
        }
      }
      if (real_n < n) {
        // This is not an error per se. The RandomAccessFile interface expects
        // that Read returns OutOfRange if fewer bytes were read than requested.
        return errors::OutOfRange("EOF reached, ", result->size(),
                                  " bytes were read out of ", n,
                                  " bytes requested.");
      }
      return Status::OK();
    }
  };

  const std::string keep_name = ".azurekeep";
  constexpr size_t kReadAppendableFileBufferSize = 1024 * 1024;  // In bytes.

  Status GetTmpFilename(string* filename) {
    if (!filename) {
      return errors::Internal("'filename' cannot be nullptr.");
    }
    char buffer[] = "/tmp/azure_filesystem_XXXXXX";
    int fd = mkstemp(buffer);
    if (fd < 0) {
      return errors::Internal("Failed to create a temporary file.");
    }
    close(fd);
    *filename = buffer;
    return Status::OK();
  }

  class AzureWritableFile : public WritableFile {
  public:
    AzureWritableFile(const azure::storage::cloud_blob_container& container,
                      const string& blobname)
        : container_(container), blobname_(blobname), sync_needed_(true) {
      if (GetTmpFilename(&tmp_content_filename_).ok()) {
        outfile_.open(tmp_content_filename_,
                      std::ofstream::binary | std::ofstream::app);
      }
    }

    AzureWritableFile(const azure::storage::cloud_blob_container& container,
                      const string& blobname, const string& tmp_content_filename)
        : container_(container), blobname_(blobname), sync_needed_(true) {
      tmp_content_filename_ = tmp_content_filename;
      outfile_.open(tmp_content_filename_,
                    std::ofstream::binary | std::ofstream::app);
    }

    ~AzureWritableFile() override { Close().IgnoreError(); }

    Status Append(const StringPiece& data) override {
      TF_RETURN_IF_ERROR(CheckWritable());
      sync_needed_ = true;
      outfile_ << data;
      if (!outfile_.good()) {
        return errors::Internal(
            "Could not append to the internal temporary file.");
      }
      return Status::OK();
    }

    Status Close() override {
      if (outfile_.is_open()) {
        TF_RETURN_IF_ERROR(Sync());
        outfile_.close();
        std::remove(tmp_content_filename_.c_str());
      }
      return Status::OK();
    }

    Status Flush() override { return Sync(); }

    Status Sync() override {
      TF_RETURN_IF_ERROR(CheckWritable());
      if (!sync_needed_) {
        return Status::OK();
      }
      Status status = SyncImpl();
      if (status.ok()) {
        sync_needed_ = false;
      }
      return status;
    }

  private:
    Status SyncImpl() {
      outfile_.flush();
      if (!outfile_.good()) {
        return errors::Internal(
            "Could not write to the internal temporary file.");
      }
      azure::storage::cloud_block_blob blob =
          container_.get_block_blob_reference(blobname_);
      concurrency::streams::istream input_stream =
          concurrency::streams::file_stream<uint8_t>::open_istream(
              tmp_content_filename_)
              .get();
      blob.upload_from_stream(input_stream);
      input_stream.close().wait();

      return Status::OK();
    }

    Status CheckWritable() const {
      if (!outfile_.is_open()) {
        return errors::FailedPrecondition(
            "The internal temporary file is not writable.");
      }
      return Status::OK();
    }

    azure::storage::cloud_blob_container container_;
    string blobname_;
    string tmp_content_filename_;
    std::ofstream outfile_;
    bool sync_needed_;
  };

  AzureFileSystem::AzureFileSystem() {}
  AzureFileSystem::~AzureFileSystem() {}

  Status AzureFileSystem::NewRandomAccessFile(
      const string& fname, std::unique_ptr<RandomAccessFile>* result) {
    string blob_name;
    azure::storage::cloud_blob_container container;
    TF_RETURN_IF_ERROR(InitializeAzureFile(fname, &blob_name, &container));

    if (ContainerExists(&container).ok()) {
      azure::storage::cloud_block_blob blob =
          container.get_block_blob_reference(blob_name);
      if (blob.exists()) {
        result->reset(new AzureBlockBlobRandomAccessFile(container, blob, 0));
        return Status::OK();
      } else {
        return errors::NotFound(strings::StrCat("The blob doesn't exist "));
      }
    }
    return errors::NotFound(strings::StrCat("The container doesn't exist "));
  }

  Status AzureFileSystem::NewWritableFile(const string& fname,
                                          std::unique_ptr<WritableFile>* result) {
    string blob_name;
    azure::storage::cloud_blob_container container;
    TF_RETURN_IF_ERROR(InitializeAzureFile(fname, &blob_name, &container));
    result->reset(new AzureWritableFile(container, blob_name));
    return Status::OK();
  }

  Status AzureFileSystem::NewAppendableFile(
      const string& fname, std::unique_ptr<WritableFile>* result) {
    std::unique_ptr<RandomAccessFile> reader;
    TF_RETURN_IF_ERROR(NewRandomAccessFile(fname, &reader));
    std::unique_ptr<char[]> buffer(new char[kReadAppendableFileBufferSize]);
    Status status;
    uint64 offset = 0;
    StringPiece read_chunk;

    // Read the file from Azure in chunks and save it to a tmp file.
    string old_content_filename;
    TF_RETURN_IF_ERROR(GetTmpFilename(&old_content_filename));
    std::ofstream old_content(old_content_filename, std::ofstream::binary);
    while (true) {
      status = reader->Read(offset, kReadAppendableFileBufferSize, &read_chunk,
                            buffer.get());
      if (status.ok()) {
        old_content << read_chunk;
        offset += kReadAppendableFileBufferSize;
      } else if (errors::IsOutOfRange(status)) {
        // Expected, this means we reached EOF.
        old_content << read_chunk;
        break;
      } else {
        return status;
      }
    }
    old_content.close();

    // Create a writable file and pass the old content to it.
    string blob_name;
    azure::storage::cloud_blob_container container;
    TF_RETURN_IF_ERROR(InitializeAzureFile(fname, &blob_name, &container));
    result->reset(
        new AzureWritableFile(container, blob_name, old_content_filename));
    return Status::OK();
  }

  Status AzureFileSystem::NewReadOnlyMemoryRegionFromFile(
      const string& fname, std::unique_ptr<ReadOnlyMemoryRegion>* result) {
    return errors::Unimplemented(
        "Azure filesystem does not support ReadOnlyMemoryRegion");
  }

  Status AzureFileSystem::FileExists(const string& fname) {
    string blob_name;
    azure::storage::cloud_blob_container container;
    TF_RETURN_IF_ERROR(InitializeAzureFile(fname, &blob_name, &container));
    if (blob_name.size() == 0) {
      return Status::OK();
    }
    azure::storage::cloud_block_blob blob =
        container.get_block_blob_reference(blob_name);
    if (blob.exists()) {
      return Status::OK();
    } else {
      azure::storage::cloud_blob_directory directory =
          container.get_directory_reference(blob_name);
      azure::storage::list_blob_item_iterator end_of_results;
      for (auto it = directory.list_blobs(); it != end_of_results;) {
        return Status::OK();
      }
    }
    return errors::NotFound("The specified path ", fname, " was not found.");
  }

  Status AzureFileSystem::GetChildren(const string& dir,
                                      std::vector<string>* result) {
    string dir_name;
    azure::storage::cloud_blob_container container;
    TF_RETURN_IF_ERROR(InitializeAzureFile(dir, &dir_name, &container));
    TF_RETURN_IF_ERROR(FileSystem::IsDirectory(dir));

    azure::storage::cloud_blob_directory directory =
        container.get_directory_reference(dir_name);
    std::string parent_name = directory.get_parent_reference().prefix();

    azure::storage::list_blob_item_iterator end_of_results;
    result->push_back(".");  // some tensorflow examples expect the root directory first

    for (auto it = directory.list_blobs(); it != end_of_results; ++it) {
      if (it->is_blob()) {
        string name = it->as_blob().name();
        result->push_back(name.substr(name.find_last_of("/") + 1));
      } else {
        string name = it->as_directory().prefix();
        name = name.substr(0, name.length() - 1);
        result->push_back(name.substr(name.find_last_of("/") + 1));
      }
    }

    return Status::OK();
  }

  Status AzureFileSystem::DeleteFile(const string& fname) {
    string blob_name;
    azure::storage::cloud_blob_container container;
    TF_RETURN_IF_ERROR(InitializeAzureFile(fname, &blob_name, &container));

    azure::storage::cloud_block_blob blob =
        container.get_block_blob_reference(blob_name);

    if (blob.exists()) {
      azure::storage::cloud_blob_directory parent_directory =
          blob.get_parent_reference();
      CreateDir(parent_directory.prefix().substr(
          0, parent_directory.prefix().size() - 1));
      blob.delete_blob();
    }

    return Status::OK();
  }

  Status AzureFileSystem::CreateDir(const string& name) {
    string dir_name;
    azure::storage::cloud_blob_container container;
    TF_RETURN_IF_ERROR(InitializeAzureFile(name, &dir_name, &container));
    return CreateDirKeeper(dir_name, container);
  }

  Status AzureFileSystem::CreateDirKeeper(
      const string& dir_name, azure::storage::cloud_blob_container& container) {
    azure::storage::cloud_block_blob blob =
        container.get_block_blob_reference(dir_name + "/" + keep_name);
    blob.upload_text("");  // if you don't do that the file will not be created
    return Status::OK();
  }

  Status AzureFileSystem::DeleteDir(const string& name) {
    string dir_name;
    azure::storage::cloud_blob_container container;
    TF_RETURN_IF_ERROR(InitializeAzureFile(name, &dir_name, &container));

    azure::storage::cloud_blob_directory directory =
        container.get_directory_reference(dir_name);
    std::string parent_name = directory.get_parent_reference().prefix();

    if (parent_name.size() > 0) {
      CreateDirKeeper(parent_name.substr(0, parent_name.size() - 1),
                      container);  // create the parent directory, because if it
      // contains no blob it will not be visible
    } else {
      return Status::OK();
    }

    azure::storage::list_blob_item_iterator end_of_results;
    for (auto it = directory.list_blobs(
        true, azure::storage::blob_listing_details::none, 0,
        azure::storage::blob_request_options(),
        azure::storage::operation_context());
         it != end_of_results; ++it) {
      if (it->is_blob()) {
        it->as_blob().delete_blob();
      }
    }

    return Status::OK();
  }

  Status AzureFileSystem::InitializeAzureFile(
      StringPiece fname, string* blob,
      azure::storage::cloud_blob_container* container) {
    string containername;
    TF_RETURN_IF_ERROR(ParseAzurePath(fname, &containername, blob));
    GetContainer(containername, container);
    return ContainerExists(container);
  }

  string AzureFileSystem::GetConnectionString() {
    std::stringstream connection_string;
    connection_string << "DefaultEndpointsProtocol=https;AccountName="
                      << std::getenv("AZURE_ACCOUNT_NAME")
                      << ";AccountKey=" << std::getenv("AZURE_ACCOUNT_KEY");
    std::cout << connection_string.str();
    return connection_string.str();
  }

  Status AzureFileSystem::GetContainer(
      const string& containerName,
      azure::storage::cloud_blob_container* container) {
    azure::storage::cloud_storage_account storage_account =
        azure::storage::cloud_storage_account::parse(
            AzureFileSystem::GetConnectionString());
    azure::storage::cloud_blob_client blob_client =
        storage_account.create_cloud_blob_client();
    *container = blob_client.get_container_reference(containerName);
    return Status::OK();
  }

  Status AzureFileSystem::ParseAzurePath(StringPiece fname, string* container,
                                         string* blob) {
    if (!container || !blob) {
      return errors::Internal("container and blob cannot be null.");
    }
    StringPiece scheme, containerp, blobp;
    io::ParseURI(fname, &scheme, &containerp, &blobp);
    if (scheme != "az") {
      return errors::InvalidArgument("Azure path doesn't start with 'az://': ",
                                     fname);
    }
    *container = containerp.ToString();
    if (container->empty() || *container == ".") {
      return errors::InvalidArgument(
          "Azure path doesn't contain a container name: ", fname);
    }
    blobp.Consume("/");
    *blob = blobp.ToString();
    return Status::OK();
  }

  Status AzureFileSystem::GetFileSize(const string& filename, uint64* size) {
    string blob_name;
    azure::storage::cloud_blob_container container;
    TF_RETURN_IF_ERROR(InitializeAzureFile(filename, &blob_name, &container));

    if (ContainerExists(&container) == Status::OK()) {
      azure::storage::cloud_block_blob blob =
          container.get_block_blob_reference(blob_name);
      if (blob.exists()) {
        *size = static_cast<uint64>(blob.properties().size());
        return Status::OK();
      } else {
        return errors::NotFound(strings::StrCat("The blob doesn't exist "));
      }
    }
    return errors::NotFound(strings::StrCat("The container doesn't exist "));
  }

  Status AzureFileSystem::RenameFile(const string& src, const string& target) {
    string containername_src, containername_dest, blobname_src, blobname_dest;
    azure::storage::cloud_blob_container container;
    TF_RETURN_IF_ERROR(ParseAzurePath(src, &containername_src, &blobname_src));
    TF_RETURN_IF_ERROR(
        ParseAzurePath(target, &containername_dest, &blobname_dest));

    if (containername_dest != containername_src) {
      return errors::Internal(strings::StrCat(
          "The container of src and target file should be the same"));
    }
    TF_RETURN_IF_ERROR(GetContainer(containername_src, &container));

    azure::storage::cloud_block_blob blobSource =
        container.get_block_blob_reference(blobname_src);
    azure::storage::cloud_block_blob blobCopy =
        container.get_block_blob_reference(blobname_dest);

    if (blobSource.exists() && !blobCopy.exists()) {
      blobCopy.start_copy_async(blobSource);
      blobSource.delete_blob_if_exists();
      return Status::OK();
    } else {
      return errors::NotFound(strings::StrCat("The blob doesn't exist "));
    }
  }

  Status AzureFileSystem::Stat(const string& filename, FileStatistics* stat) {
    string blob_name;
    azure::storage::cloud_blob_container container;
    TF_RETURN_IF_ERROR(InitializeAzureFile(filename, &blob_name, &container));

    azure::storage::cloud_block_blob blob =
        container.get_block_blob_reference(blob_name);

    if (blob.exists()) {
      stat->length = static_cast<int64>(blob.properties().size());
      stat->mtime_nsec =
          static_cast<int64>(blob.properties().last_modified().utc_timestamp());
      stat->is_directory = AzureFileSystem::IsADirectory(&container, &blob_name);

      return Status::OK();
    } else {
      auto isDir = AzureFileSystem::IsADirectory(&container, &blob_name);
      if (isDir) {
        stat->is_directory = isDir;
        return Status::OK();
      } else {
        return errors::NotFound(strings::StrCat("The blob doesn't exist "));
      }
    }
  }

  Status AzureFileSystem::ContainerExists(
      azure::storage::cloud_blob_container* container) {
    if (container->exists()) {
      return Status::OK();
    }
    return errors::Internal(
        strings::StrCat("The container ", container->name(), " doesn't exist "));
  }

  bool AzureFileSystem::IsADirectory(
      azure::storage::cloud_blob_container* container, const string* blob_name) {
    azure::storage::cloud_blob_directory directory =
        container->get_directory_reference(*blob_name);
    azure::storage::list_blob_item_iterator end_of_results;
    for (auto it = directory.list_blobs(); it != end_of_results;) {
      return true;
    }
    return false;
  }

  REGISTER_FILE_SYSTEM("az", AzureFileSystem);
}
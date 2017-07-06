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
#include "tensorflow/core/lib/core/status_test_util.h"

namespace tensorflow {
namespace {
const string container_name = "az-test-container";
std::vector<string> result;
uint64 file_size = 0;
FileStatistics stat;
std::unique_ptr<WritableFile> file;

class AzureFileSystemTest : public ::testing::Test {
 protected:
  AzureFileSystem afs;
  azure::storage::cloud_blob_container container;

  AzureFileSystemTest() {
    // Create a container for the test files
    // you should set the env variables AZURE_ACCOUNT_NAME and AZURE_ACCOUNT_KEY
    azure::storage::cloud_storage_account storage_account =
        azure::storage::cloud_storage_account::parse(afs.GetConnectionString());
    azure::storage::cloud_blob_client blob_client =
        storage_account.create_cloud_blob_client();
    container = blob_client.get_container_reference(container_name);
    container.create_if_not_exists();
  }
};

TEST_F(AzureFileSystemTest, CreateDir) {
  TF_EXPECT_OK(
      afs.CreateDir("az://"+container_name+"/CreateDir/subdir/subsubdir"));
  EXPECT_TRUE(
      container
          .get_block_blob_reference("CreateDir/subdir/subsubdir/.azurekeep")
          .exists());
}

TEST_F(AzureFileSystemTest, DeleteDir) {
  container.get_block_blob_reference("DeleteDir/subdir/subsubdir/.azurekeep")
      .upload_text("");

  TF_EXPECT_OK(
      afs.DeleteDir("az://"+container_name+"/DeleteDir/subdir/subsubdir"));
  EXPECT_FALSE(
      container
          .get_block_blob_reference("DeleteDir/subdir/subsubdir/.azurekeep")
          .exists());
  EXPECT_TRUE(container.get_block_blob_reference("DeleteDir/subdir/.azurekeep")
                  .exists());
}

TEST_F(AzureFileSystemTest, FileExists) {
  container.get_block_blob_reference("FileExists/1.txt")
      .upload_text("azertyuiop");

  TF_EXPECT_OK(afs.FileExists("az://"+container_name+"/FileExists/1.txt"));
}

TEST_F(AzureFileSystemTest, FileNotExists) {
  EXPECT_TRUE(errors::IsNotFound(
      afs.FileExists("az://"+container_name+"/FileExists/2.txt")));
}

TEST_F(AzureFileSystemTest, GetFileSize) {
  container.get_block_blob_reference("GetFileSize/1.txt")
      .upload_text("azertyuiopazertyuiop");

  TF_EXPECT_OK(
      afs.GetFileSize("az://"+container_name+"/GetFileSize/1.txt", &file_size));
  EXPECT_EQ(file_size, 20);
}

TEST_F(AzureFileSystemTest, GetFileSizeWithNonExistFile) {
  EXPECT_TRUE(errors::IsNotFound(
      afs.GetFileSize("az://"+container_name+"/GetFileSize/2.txt", &file_size)));
}

TEST_F(AzureFileSystemTest, GetChildren) {
  container.get_block_blob_reference("GetChildren/subdir/subsubdir/.azurekeep")
      .upload_text("");
  container.get_block_blob_reference("GetChildren/subdir/.azurekeep")
      .upload_text("");

  TF_EXPECT_OK(
      afs.GetChildren("az://"+container_name+"/GetChildren/subdir", &result));
  std::sort(result.begin(), result.end());
  EXPECT_EQ(std::vector<string>({".", ".azurekeep", "subsubdir"}), result);
}

TEST_F(AzureFileSystemTest, GetChildrenNotExistingDir) {
  EXPECT_TRUE(errors::IsNotFound(afs.GetChildren(
      "az://"+container_name+"/GetChildrenNotExistingDir/subdir", &result)));
}

TEST_F(AzureFileSystemTest, DeleteFile) {
  container.get_block_blob_reference("DeleteFile/subdir/1.txt")
      .upload_text("azerty");

  TF_EXPECT_OK(
      afs.DeleteFile("az://"+container_name+"/DeleteFile/subdir/1.txt"));
  EXPECT_FALSE(
      container.get_block_blob_reference("DeleteFile/subdir/1.txt").exists());
}

TEST_F(AzureFileSystemTest, RenameFile) {
  container.get_block_blob_reference("RenameFile/torename.txt")
      .upload_text("azert");

  TF_EXPECT_OK(afs.RenameFile("az://"+container_name+"/RenameFile/torename.txt",
                              "az://"+container_name+"/RenameFile/renamed.txt"));

  EXPECT_FALSE(
      container.get_block_blob_reference("RenameFile/torename.txt").exists());

  azure::storage::cloud_block_blob block_blob =
      container.get_block_blob_reference("RenameFile/renamed.txt");
  EXPECT_TRUE(block_blob.exists());
  EXPECT_EQ(block_blob.properties().size(), 5);
}

TEST_F(AzureFileSystemTest, RenameFileNotExist) {
  EXPECT_TRUE(errors::IsNotFound(
      afs.RenameFile("az://"+container_name+"/RenameFile/wrong.jpg",
                     "az://"+container_name+"/RenameFile/renamed.jpg")));
}

TEST_F(AzureFileSystemTest, Stat) {
  container.get_block_blob_reference("Stat/1.txt")
      .upload_text("azertyuiopazertyuiopazertyuiopazertyuiopazertyuiop");
  TF_EXPECT_OK(afs.Stat("az://"+container_name+"/Stat/1.txt", &stat));
  EXPECT_FALSE(stat.is_directory);
  EXPECT_EQ(stat.length, 50);
}

TEST_F(AzureFileSystemTest, StatOnDir) {
  container.get_block_blob_reference("Stat/subdir/.azurekeep").upload_text("");

  TF_EXPECT_OK(afs.Stat("az://"+container_name+"/Stat/subdir", &stat));
  EXPECT_TRUE(stat.is_directory);
}

TEST_F(AzureFileSystemTest, StatFileNotExist) {
  EXPECT_TRUE(
      errors::IsNotFound(afs.Stat("az://"+container_name+"/Stat/2.txt", &stat)));
}

TEST_F(AzureFileSystemTest, NewWritableFile) {
  TF_EXPECT_OK(afs.NewWritableFile(
      "az://"+container_name+"/NewWritableFile/subdir/myfile.txt", &file));

  TF_EXPECT_OK(file->Append("content1,"));
  TF_EXPECT_OK(file->Append("content2,"));
  TF_EXPECT_OK(file->Flush());
  TF_EXPECT_OK(file->Append("content3,"));
  TF_EXPECT_OK(file->Append("content4"));
  TF_EXPECT_OK(file->Close());

  azure::storage::cloud_block_blob block_blob =
      container.get_block_blob_reference("NewWritableFile/subdir/myfile.txt");
  EXPECT_TRUE(block_blob.exists());
  EXPECT_EQ(block_blob.properties().size(), 35);
}

TEST_F(AzureFileSystemTest, NewAppendableFile) {
  container.get_block_blob_reference("NewAppendableFile/subdir/myfile.txt")
      .upload_text("content1,content2,content3,content4");

  TF_EXPECT_OK(afs.NewAppendableFile(
      "az://"+container_name+"/NewAppendableFile/subdir/myfile.txt", &file));
  TF_EXPECT_OK(file->Append(",content5"));
  TF_EXPECT_OK(file->Close());

  azure::storage::cloud_block_blob block_blob =
      container.get_block_blob_reference("NewAppendableFile/subdir/myfile.txt");
  EXPECT_TRUE(block_blob.exists());
  EXPECT_EQ(block_blob.properties().size(), 44);
}

TEST_F(AzureFileSystemTest, NewRandomAccessFile) {
  std::unique_ptr<RandomAccessFile> reader;
  char scratch[6];
  StringPiece result;

  container.get_block_blob_reference("NewRandomAccessFile/subdir/test.txt")
      .upload_text("0123456789");

  TF_EXPECT_OK(afs.NewRandomAccessFile(
      "az://"+container_name+"/NewRandomAccessFile/subdir/test.txt", &reader));

  // Read the first chunk.
  TF_EXPECT_OK(reader->Read(0, sizeof(scratch), &result, scratch));
  EXPECT_EQ("012345", result);

  // Read the second chunk.
  EXPECT_TRUE(
      errors::IsOutOfRange(reader->Read(sizeof(scratch), 6, &result, scratch)));
  EXPECT_EQ("6789", result);

  // Read in same range
  TF_EXPECT_OK(reader->Read(6, 2, &result, scratch));
  EXPECT_EQ("67", result);

  TF_EXPECT_OK(reader->Read(6, 4, &result, scratch));
  EXPECT_EQ("6789", result);

  EXPECT_TRUE(errors::IsOutOfRange(reader->Read(7, 4, &result, scratch)));
  EXPECT_EQ("789", result);

  // Read outside EOF
  EXPECT_TRUE(errors::IsOutOfRange(reader->Read(10, 10, &result, scratch)));
  EXPECT_EQ("", result);
}

TEST_F(AzureFileSystemTest, DeleteTestContainer) {
  container.delete_container();
}
}
}

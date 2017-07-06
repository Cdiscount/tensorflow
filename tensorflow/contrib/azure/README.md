# Azure Blob Storage filesystem implementation

This contrib is a filesystem implementation for Microsoft Azure blob storage.

##Features

- create/read/update/rename/delete block blobs
- create/delete directory (directory are virtual in Azure, it exists only when files are in the directory path,
so an empty .azurekeep file is created to materialize the directory)
- list directory
- stats of a block blob/directory

Does not handle ReadOnlyMemoryRegion.
 
##Usage

###Prerequisites

####Bazel

Install the latest version of Bazel.
Instructions are available [on the Bazel website](https://bazel.build/versions/master/docs/install.html).

####Azure

Install the latest version of Azure Storage Client Library for C++. 
Instructions are available [on the Azure Storage C++ Github page](https://github.com/Azure/azure-storage-cpp).

###Build

```bash
bazel build -c opt //tensorflow/contrib/azure/core/platform/azure:azure_file_system
```

###Tests

You should have an Azure Storage Blob container available to launch the tests.

```bash
bazel test //tensorflow/contrib/core/platform/azure:azure_file_system_test \
    --action_env=AZURE_ACCOUNT_NAME=xxxxxyourazureaccountnamexxxxx  \
    --action_env=AZURE_ACCOUNT_KEY=xxxxxyourazureaccountkeyxxxxx

```

###Usage

Refer to [the Tensorflow filesystem documentation](https://www.tensorflow.org/extend/add_filesys)
for the usage of the filesystem implementation.

In Python for example :

```python
    import os
    from tensorflow.python.framework import load_library
    from tensorflow.python.platform import gfile
    from tensorflow.python.platform import resource_loader
    
    file_system_library = os.path.join(resource_loader.get_data_files_path(), 
      '/yourpath/tensorflow/bazel-bin/tensorflow/contrib/azure/core/platform/azure/libazure_file_system.so')
    load_library.load_file_system_library(file_system_library)
    
    gfile.ListDirectory('az://test-container/directory')
    imgbyte = gfile.FastGFile('az://test-container/directory/image.jpg', 'rb').read()
```

Do not forget to export your Azure credentials before running the python file :

```bash
export AZURE_ACCOUNT_NAME=xxxxxyourazureaccountnamexxxxx
export AZURE_ACCOUNT_KEY=xxxxxyourazureaccountkeyxxxxx
python my_test.py
```



r"""
Trying to find and illustrate the bug, 
which is explained here: 
https://www.logilab.org/blogentry/17873
"""

import os
import tempfile

import psutil
from boto.s3.connection import S3Connection
from aws import AWSDetails
from aws.S3Service import downloadFileFromBucket


def simpleBoto(names, bucketName):
    # prepare folder to store files from S3
    outputFolder = os.path.join('/tmp', 'output')
    if not os.path.isdir(outputFolder):
        os.makedirs(outputFolder)

    # prepare connection with S3 bucket
    accessKey = AWSDetails.getS3AccessKey()
    secretKey = AWSDetails.getS3SecretKey()
    connection = S3Connection(accessKey, secretKey)
    bucket = connection.get_bucket(bucketName)

    # download files, one after another
    paths = []
    for name in names:
        key = bucket.get_key(name)
        path = os.path.join(outputFolder, name)
        key.get_contents_to_filename(path)
        paths.append(path)
    return paths


def partialInHouseCode(names, bucketName):
    # prepare folder to store files from S3
    outputFolder = os.path.join('/tmp', 'output')
    if not os.path.isdir(outputFolder):
        os.makedirs(outputFolder)
    paths = []
    for name in names:
        path = os.path.join(outputFolder, name)
        downloadFileFromBucket(bucketName,
                               name,
                               path)
        paths.append(path)
    return paths


def fullInHouseDownloadCode(names, bucketName):
    """This function shows that the misuse of the function mkstemp leads
    to the leak of open files."""
    paths = []
    for name in names:
        path = tempfile.mkstemp(suffix='.jpeg')[1]
        downloadFileFromBucket(bucketName,
                               name,
                               path)
        paths.append(path)
    return paths


def fixedfullInHouseDownloadCode(names, bucketName):
    """This function shows how to solve the problem."""
    paths = []
    for name in names:
        tempFileDescriptor, path = tempfile.mkstemp(suffix='.jpeg')
        downloadFileFromBucket(bucketName,
                               name,
                               path)
        paths.append(path)
        os.close(tempFileDescriptor)
    return paths


for func in [simpleBoto, partialInHouseCode, fullInHouseDownloadCode, 
             fixedfullInHouseDownloadCode]:
    print('\nTesting new function...')

    # information about blobs to download
    bucketName = 'issuefiles'
    names = ['{}.txt'.format(i) for i in range(6)]

    paths = func(names, bucketName)
    print('Done downloading all files.')
    proc = psutil.Process()
    openFiles = proc.open_files()
    if len(openFiles) == 0 and len(paths) == len(names):
        print('OK')
    else:
        print('{} / {} were downloaded'.format(len(paths), len(names)))
        print('LEAK:')
        print('{} opened files: {}'.format(
            len(openFiles),
            '\n'.join(v1 for v1,_ in openFiles[:10])))
    for path in paths:
        os.remove(path)
    print('')

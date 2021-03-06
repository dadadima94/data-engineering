# Author Davide Di Matteo
# davidee.di.matteo@persgroep.nl

import boto3
import datetime
import os

def get_matching_s3_objects(bucket, prefix='', suffix=''):
    """
    Generate objects in an S3 bucket.
    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    s3 = boto3.client('s3')
    kwargs = {'Bucket': bucket}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix

    while True:

        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = s3.list_objects_v2(**kwargs)

        try:
            contents = resp['Contents']
        except KeyError:
            return

        for obj in contents:
            key = obj['Key']
            if key.startswith(prefix) and key.endswith(suffix):
                yield obj

        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

def get_matching_s3_keys(bucket, prefix='', suffix=''):
    """
    Generate the keys in an S3 bucket.
    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    for obj in get_matching_s3_objects(bucket, prefix, suffix):
        yield obj['Key']

# configs to change by the user
s3 = boto3.resource('s3')
bucket_name = 'your_bucket' # source and destination bucket (in case of renaming)
key_prefix = 'your_key' # key prefix to take only the directories we need (goes recursively down to the file level)

for each in get_matching_s3_keys(bucket = bucket_name, prefix=key_prefix):
    print(each)
    new_file =  each.replace(':', '.').replace('[UTC]', 'UTC')  # change file name
    print(new_file)
    copy_source = {'Bucket': bucket_name, 'Key': each}
    print(copy_source)
    s3.meta.client.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=new_file)
    s3.meta.client.delete_object(Bucket = bucket_name, Key = each ) 
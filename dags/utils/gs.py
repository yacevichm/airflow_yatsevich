import logging
import re
import os
from airflow.exceptions import AirflowException


def enable_conn_caching(hook):
    orig_get_conn = hook.get_conn

    def caching_get_conn():
        if hook._conn is None:
            hook._conn = orig_get_conn()
        return hook._conn

    hook._conn = None
    hook.get_conn = caching_get_conn


def delete_by_prefix(hook, bucket, prefix, delimiter=None):
    """
        Delete list objects with such prefix in name
        if wildcard presents it will delimit name by right side

        :param hook: the GS hook from airflow should operator be passed here (templated)
        :type hook: GoogleCloudStorageHook

        :param bucket: The Google cloud storage bucket where
             objects are. (templated)
        :type bucket: string

        :param prefix:  prefix for files in the Google cloud
            storage. (templated)
        you can use it with wildcard then list will be delimit with right side
        for example 'test_path/inner_path/something*.txt' it will take everything that starts from this
        'test_path/inner_path/something*' and have then '.txt'
        :type prefix: string

        :param delimiter: custom delimiter if you want to limit list from by presented prefix(templated)
        :type delimiter: string

        **Examples**:
        the following command delete all files from bucket - 'some_bucket' files with next prefix - 'some_prefix'
            delete_list(hook, 'some_bucket', 'some_prefix/')
        the following command will delete only files which starts from - 'some_prefix/*.csv' and has then '.csv'
            delete_list(hook, 'some_bucket', 'some_prefix/*.csv')
    """
    wildcard = '*'
    if wildcard in prefix:
        prefix, delimiter = prefix.split(wildcard, 1)

    objects = hook.list(bucket=bucket, prefix=prefix, delimiter=delimiter)

    logging.info("Removing %s objects from '%s/%s'" % (len(objects), bucket, prefix))

    for gs_object in objects:
        hook.delete(bucket=bucket, object=gs_object)


def copy_by_pattern(hook, source_bucket, source_prefix, pattern, destination_bucket=None,
                    destination_prefix=None, delete_sources=False):
    """
        This command copy files by pattern from one GS path to another.

        :param hook: the GS hook from airflow should operator be passed here (templated)
        :type hook: GoogleCloudStorageHook

        :param source_bucket: The source Google cloud storage bucket where
             objects are. (templated)
        :type source_bucket: string

        :param source_prefix: The source name of objects to copy in the Google cloud
            storage bucket. (templated)
        :type source_prefix: string

        :param pattern: pattern which will used for search files (templated)
        :type pattern: string

        :param destination_bucket: The destination bucket where objects copy in the Google cloud
            storage bucket. If destination_bucket is None source_bucket will be used as destination (templated)
        :type destination_bucket: string

        :param destination_prefix: The destination path where objects to copy in the Google cloud
            storage bucket. If destination_prefix is None source_prefix will be used as destination (templated)
        :type destination_prefix: string

        :param delete_sources: Delete source file after the source file was copied to destination path
        :type delete_sources: boolean

        **Examples**:
        the following command copy files with pattern from gs place to another
        files like gs://source_bucket/source_prefix/some_awful_pattern_54.txt':
            copy_files_by_pattern(hooks, 'source_bucket', 'source_prefix/','some_awful_pattern_[0-9]{2}.txt')
    """
    logging.info("Copy objects from '%s/%s' to '%s/%s' by pattern '%s'"
                 % (source_bucket, source_prefix, destination_bucket, destination_prefix, pattern))

    files = hook.list(source_bucket, prefix=source_prefix)

    logging.info("%s object(s) found in '%s/%s'" % (len(files), source_bucket, source_prefix))

    logging.info("List for copy %s, pattern '%s'" % (files, pattern))

    flag = True
    for gs_object in files:
        name_file = gs_object.split('/')[-1]
        match = re.search(pattern, name_file)
        if match:
            copy_file_to_dir(hook, source_bucket=source_bucket,
                             source_prefix=os.path.join(source_prefix,name_file),
                             destination_bucket=destination_bucket,
                             destination_prefix=destination_prefix,
                             delete_sources=delete_sources)
            flag = False
    if flag:
        raise AirflowException("Copy objects was failed")


def copy_file_to_dir(hook, source_bucket, source_prefix, destination_bucket=None, destination_prefix=None, delete_sources=False):
    if destination_bucket is None and destination_prefix is None:
        raise AirflowException("Illegal state of method")

    if destination_bucket is None:
        destination_bucket = source_bucket

    if destination_prefix is None:
        destination_prefix = source_prefix

    name_file = source_prefix.split('/')[-1]
    logging.info("Copy from '%s/%s', to '%s/%s %s'"
                 % (source_bucket, source_prefix, destination_bucket, destination_prefix, name_file))
    hook.copy(source_bucket=source_bucket,
              source_object=source_prefix,
              destination_bucket=destination_bucket,
              destination_object=os.path.join(destination_prefix, name_file))

    if delete_sources is True:
        hook.delete(bucket=source_bucket, object=source_prefix)


def get_bucket_from_name(gs_object):
    #TODO hack sometimes test doesnt see that urlparse was imported
    from six.moves.urllib.parse import urlparse
    parse_url = urlparse(gs_object)
    if parse_url.hostname:
        return parse_url.hostname
    else:
        raise AirflowException(
            "Bucket name was not found from gs_object: {} ".format(gs_object))


def get_path_without_bucket_from_name(gs_object):
    #TODO hack sometimes test doesnt see that urlparse was imported
    from six.moves.urllib.parse import urlparse
    parse_url = urlparse(gs_object)
    if parse_url.path:

        return parse_url.path[1:]
    else:
        raise AirflowException("cant extract path without bucket from url: {} ".format(gs_object))

import datetime
import json

from tornado import web

import boto3

from IPython.html.services.contents.manager import ContentsManager
import  IPython.nbformat
from IPython.utils.traitlets import Unicode

class S3ContentsManager(ContentsManager):

    s3_bucket = Unicode('', config=True, help='Bucket name for contents.')
    s3_prefix = Unicode('', config=True, help='Key prefix for contents')

    def __init__(self, **kwargs):
        super(S3ContentsManager, self).__init__(**kwargs)
        # Configuration of aws access keys should be handled with awscli with
        # boto3, e.g. 'aws configure'
        self.s3 = boto3.resource('s3')
        self.bucket = self.s3.Bucket(self.s3_bucket)
        self.mapping = {}

    def is_hidden(self, path):
        """S3 does not support hidden files"""
        return False

    def file_exists(self, path):
        """Returns True if the file exists, else returns False."""
        key = self.s3_prefix + path
        objs = list(self.bucket.objects.filter(Prefix=key).limit(1))
        if len(objs) > 0:
            return objs[0].key == key
        else:
            return False

    def dir_exists(self, path):
        """Returns True if key prefix exists"""
        key = self.s3_prefix + path
        if key.endswith('/'):
            # true if key ends in '/' and subkeys exist
            objs = list(self.bucket.objects.filter(Prefix=key))
            print(path, key, objs, len(objs) > 0)
            return len(objs) > 0
        else:
            return False

    def exists(self, path):
        """Check for file or directory existence"""
        if path.endswith('/'):
            return self.dir_exists(path)
        else:
            return self.file_exists(path)

    def _base_model(self, path):
        """Build the common base of a contents model"""
        key = self.s3_prefix + path
        objs = list(self.bucket.objects.filter(Prefix=key).limit(1))
        if objs:
            # TODO: figure out a more elegant solution for last-modified for
            # folders
            last_modified = objs[0].last_modified
            model = {}
            model['bucket'] = self.s3_bucket
            model['key'] = key
            model['name'] = path.rsplit('/', 1)[-1]
            model['path'] = path
            model['last_modified'] = last_modified
            model['created'] = last_modified
            model['content'] = None
            model['format'] = None
            model['mimetype'] = None
            return model

    def _write_file(self, path, content):
        key = self.s3_prefix + path
        self.s3.Object(self.s3_bucket, key).put(Body=content)

    def _read_file(self, path):
        key = self.s3_prefix + path
        obj = self.s3.Object(self.s3_buckt, key).get()
        return obj['Body'].read()

    def _file_model(self, path, content=True):
        """Build a model for a file"""
        model = self._base_model(path)
        model['type'] = 'file'

        if content:
            model['content'] = self._read_file(path)

        return model

    def _notebook_model(self, path, content=True):
        """Build a notebook model

        if content is requested, the notebook content will be populated
        as a JSON structure (not double-serialized)
        """
        model = self._base_model(path)
        model['type'] = 'notebook'
        if content:
            contents = self._read_file(path)
            notebook = nbformat.read(contents, as_version=4)
            self.mark_trusted_cells(notebook, path)
            model['content'] = notebook
            model['format'] = 'json'
            self.validate_notebook_model(model)
        return model

    def _dir_model(self, path, content=True):
        """Build a model for a directory"""
        model = self._base_model(path)
        model['type'] = 'directory'
        if content:
            key = self.s3_prefix + path
            objects = list(self.bucket.objects.filter(Prefix=key))
            model['contents'] = \
                [self.get(obj.key, content=False) for obj in objects]
            model['format'] = 'json'
            print(path, key, model['contents'])
        return model

    def get(self, path, content=True, type=None, format=None):

        if not self.exists(path):
            raise web.HTTPError(404, u'No such file or directory: %s' % path)

        if path.endswith('/') or path == "":
            if type not in (None, 'directory'):
                raise web.HTTPError(400,
                    u'%s is a directory, not a %s' % (path, type),
                    reason='bad type')
            model = self._dir_model(path, content=content)
            print(model)
        elif type == 'notebook' or (type is None and path.endswith('.ipynb')):
            model = self._notebook_model(path, content=content)
        else:
            if type == 'directory':
                raise web.HTTPError(400,
                                u'%s is not a directory' % path, reason='bad type')
            model = self._file_model(path, content=content, format=format)
        return model

    def delete(self, path):
        """Remove notebook from S3 Bucket"""
        obj = self.bucket.Object(self.s3_prefix + path)

    def copy(self, from_path, to_path=None):
        pass

    def info_string(self):
        """Description of S3 Notebook Manager"""
        return "Serving notebooks from s3. bucket name: %s" % self.s3_bucket

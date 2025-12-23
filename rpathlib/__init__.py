import re
import time
import json
import uuid
import shutil
import pathlib
import asyncio
import typing
import aiohttp
import logging
import tempfile
import contextlib

import rpathlib.utils

logger = logging.getLogger(__name__)
rclone = shutil.which('rclone')

class RPath:
  ''' Like pathlib's Path but supporting rclone-facilitated remote operation
  '''
  remote: str
  path: pathlib.PurePosixPath
  client: typing.Optional[typing.Callable] = None
  a_client: typing.Optional[typing.Callable] = None

  def __init__(self, path='', remote=None):
    '''
    Create an RPath object. Behaves mostly like a pathlib Path object
    
    :param path: rclone-style location, e.g. remote:some/path
    :param remote: internal, do not set directly
    '''
    if RPath.client is None: raise RuntimeError('RClone client not running')
    if isinstance(path, RPath):
      self.path = path.path
      self.remote = path.remote
    else:
      if remote is not None:
        self.remote = remote
        self.path = pathlib.PurePosixPath(path)
      elif isinstance(path, pathlib.Path):
        self.path = pathlib.PurePosixPath(path.as_posix())
        self.remote = ':local:'
      elif isinstance(path, str):
        self.remote, _, path = re.match(r'''^((:?[^:"]+|"[^"]*")+:)?(.*)$''', path).groups()
        self.path = pathlib.PurePosixPath(path)
        if self.remote is None: self.remote = ':local:'
      else:
        raise NotImplementedError(type(path))

  @property
  def name(self):
    '''
    The name of the final path component
    '''
    return self.path.name
  
  @property
  def stem(self):
    '''
    The the final path component (without the postfix)
    '''
    return self.path.stem
  
  @property
  def parent(self):
    '''
    The parent to this directory
    '''
    return RPath(self.path.parent, remote=self.remote)

  @property
  def _fs(self):
    if self.remote == ':local:':
      return '/' if self.path.is_absolute() else ':local:'
    return self.remote
  @property
  def _remote(self):
    return '' if self.path == pathlib.PurePosixPath() else str(self.path)

  def __str__(self):
    return f"{self._fs}{self._remote}"
  
  def __repr__(self):
    return f"RPath({str(self)})"

  def __hash__(self):
    return hash((self.remote, self.path))

  def __eq__(self, value):
    return repr(self) == repr(RPath(value) if not isinstance(value, RPath) else value)

  def __truediv__(self, value):
    return RPath(self.path/value, remote=self.remote)
  
  async def a_stat(self):
    ret = await RPath.a_client('operations/stat', fs=self._fs, remote=self._remote, opt=json.dumps(dict(recurse=False, showHash=False)))
    if not ret.get('item'): raise FileNotFoundError(self._remote)
    return ret['item']

  def stat(self):
    '''
    Information about this file/directory path
    '''
    ret = RPath.client('operations/stat', fs=self._fs, remote=self._remote, opt=json.dumps(dict(recurse=False, showHash=False)))
    if not ret.get('item'): raise FileNotFoundError(self._remote)
    return ret['item']

  async def a_exists(self):
    try: await self.a_stat()
    except FileNotFoundError: return False
    else: return True

  def exists(self):
    try: self.stat()
    except FileNotFoundError: return False
    else: return True

  async def a_is_file(self):
    stat = await self.a_stat()
    return not stat['IsDir']

  def is_file(self):
    stat = self.stat()
    return not stat['IsDir']

  @contextlib.asynccontextmanager
  async def a_mount(self, vfsOpt={'CacheMode': 'writes', 'WriteBack': '100ms'}):
    '''
    Use rclone to mount this, this effectively "promotes" the RPath to a temporary Path existing on your system by fuse mounting
    '''
    async with rpathlib.utils.awith_with(tempfile.TemporaryDirectory) as tmpdir:
      ret = await RPath.a_client('mount/mount', fs=str(self), mountPoint=tmpdir, vfsOpt=json.dumps(vfsOpt))
      try:
        yield pathlib.Path(tmpdir)
      finally:
        while True:
          ret = await RPath.a_client('vfs/stats', fs=str(self))
          if ret['diskCache']['uploadsInProgress'] or ret['diskCache']['uploadsQueued']:
            time.sleep(0.1)
          else:
            break
        time.sleep(0.1)
        await RPath.a_client('mount/unmount', mountPoint=tmpdir)

  @contextlib.contextmanager
  def mount(self, vfsOpt={'CacheMode': 'writes', 'WriteBack': '100ms'}):
    '''
    Use rclone to mount this, this effectively "promotes" the RPath to a temporary Path existing on your system by fuse mounting
    '''
    with tempfile.TemporaryDirectory() as tmpdir:
      ret = RPath.client('mount/mount', fs=str(self), mountPoint=tmpdir, vfsOpt=json.dumps(vfsOpt))
      try:
        yield pathlib.Path(tmpdir)
      finally:
        while True:
          ret = RPath.client('vfs/stats', fs=str(self))
          if ret['diskCache']['uploadsInProgress'] or ret['diskCache']['uploadsQueued']:
            time.sleep(0.1)
          else:
            break
        time.sleep(0.1)
        ret = RPath.client('mount/unmount', mountPoint=tmpdir)

  @contextlib.contextmanager
  def open(self, mode, *args, **kwargs):
    '''
    Fully featured "open" functionality powered by rclone mount
    '''
    # TODO: change cache mode depending on open mode
    with self.parent.mount() as path:
      with (path/self.name).open(mode, *args, **kwargs) as fh:
        yield fh

  async def a_unlink(self):
    await RPath.a_client('operations/deletefile', fs=self._fs, remote=self._remote)
  
  def unlink(self):
    RPath.client('operations/deletefile', fs=self._fs, remote=self._remote)
  
  async def a_mkdir(self, parents=False, exist_ok=False):
    if await self.a_exists():
      if not exist_ok:
        raise FileExistsError(self.path)
      else:
        return
    await RPath.a_client('operations/mkdir', fs=self._fs, remote=self._remote)

  def mkdir(self, parents=False, exist_ok=False):
    if self.exists():
      if not exist_ok:
        raise FileExistsError(self.path)
      else:
        return
    RPath.client('operations/mkdir', fs=self._fs, remote=self._remote)

  async def a_rmdir(self):
    if not await self.a_exists():
      raise FileNotFoundError(self.path)
    ret = await RPath.a_client('operations/rmdir', fs=self._fs, remote=self._remote)
    if 'error' in ret:
      if 'no such file or directory' in ret['error']:
        raise FileNotFoundError(self.path)
      else:
        raise RuntimeError(ret['error'])

  def rmdir(self):
    if not self.exists():
      raise FileNotFoundError(self.path)
    ret = RPath.client('operations/rmdir', fs=self._fs, remote=self._remote)
    if 'error' in ret:
      if 'no such file or directory' in ret['error']:
        raise FileNotFoundError(self.path)
      else:
        raise RuntimeError(ret['error'])

  async def a_copyfile(self, other):
    if isinstance(other, str):
      other_remote, _, other_path = re.match(r'''^((:?[^:"]+|"[^"]*")+:)?(.*)$''', other).groups()
      if other_remote is None:
        other_remote = self.remote
      if other_path.startswith('/'):
        other = RPath(other_path, other_remote)
      else:
        other = self.parent / pathlib.PurePath(other_path)
    if not isinstance(other, RPath): raise NotImplementedError(type(other))
    await RPath.a_client('operations/copyfile', srcFs=self._fs, srcRemote=self._remote, dstFs=other._fs, dstRemote=other._remote)

  def copyfile(self, other):
    if isinstance(other, str):
      other_remote, _, other_path = re.match(r'''^((:?[^:"]+|"[^"]*")+:)?(.*)$''', other).groups()
      if other_remote is None:
        other_remote = self.remote
      if other_path.startswith('/'):
        other = RPath(other_path, other_remote)
      else:
        other = self.parent / pathlib.PurePath(other_path)
    if not isinstance(other, RPath): raise NotImplementedError(type(other))
    RPath.client('operations/copyfile', srcFs=self._fs, srcRemote=self._remote, dstFs=other._fs, dstRemote=other._remote)

  async def a_rename(self, other):
    if isinstance(other, str):
      other_remote, _, other_path = re.match(r'''^((:?[^:"]+|"[^"]*")+:)?(.*)$''', other).groups()
      if other_remote is None:
        other_remote = self.remote
      if other_path.startswith('/'):
        other = RPath(other_path, other_remote)
      else:
        other = self.parent / pathlib.PurePath(other_path)
    if not isinstance(other, RPath): raise NotImplementedError(type(other))
    await RPath.a_client('operations/movefile', srcFs=self._fs, srcRemote=self._remote, dstFs=other._fs, dstRemote=other._remote)

  def rename(self, other):
    if isinstance(other, str):
      other_remote, _, other_path = re.match(r'''^((:?[^:"]+|"[^"]*")+:)?(.*)$''', other).groups()
      if other_remote is None:
        other_remote = self.remote
      if other_path.startswith('/'):
        other = RPath(other_path, other_remote)
      else:
        other = self.parent / pathlib.PurePath(other_path)
    if not isinstance(other, RPath): raise NotImplementedError(type(other))
    RPath.client('operations/movefile', srcFs=self._fs, srcRemote=self._remote, dstFs=other._fs, dstRemote=other._remote)

  async def a_iterdir(self):
    # TODO: suppose the directory is very large?
    ret = await RPath.a_client('operations/list', fs=self._fs, remote=self._remote, opt=json.dumps(dict(recurse=False, showHash=False)))
    try:
      for file in ret['list']:
        yield self/file['Name']
    except KeyError:
      # TODO: handle other errors like client/server down
      raise FileNotFoundError(self.path)

  def iterdir(self):
    # TODO: suppose the directory is very large?
    ret = RPath.client('operations/list', fs=self._fs, remote=self._remote, opt=json.dumps(dict(recurse=False, showHash=False)))
    try:
      for file in ret['list']:
        yield self/file['Name']
    except KeyError:
      # TODO: handle other errors like client/server down
      raise FileNotFoundError(self.path)

  async def a_read_text(self, encoding='utf-8') -> str:
    if not await self.a_is_file():
      raise IsADirectoryError(self.path)
    ret = await RPath.a_client('core/command', command='cat', arg=json.dumps(['--quiet', str(self)]))
    if ret['error']:
      if 'not found' in ret['result']:
        raise FileNotFoundError(self.path)
      else:
        raise Exception(ret['result'])
    return ret['result']

  def read_text(self, encoding='utf-8') -> str:
    if not self.is_file():
      raise IsADirectoryError(self.path)
    ret = RPath.client('core/command', command='cat', arg=json.dumps(['--quiet', str(self)]))
    if ret['error']:
      if 'not found' in ret['result']:
        raise FileNotFoundError(self.path)
      else:
        raise Exception(ret['result'])
    return ret['result']

  async def a_write_text(self, text: str, encoding='utf-8') -> int:
    formData = aiohttp.FormData()
    formData.add_field('file', text, filename=self.name)
    await RPath.a_client('operations/uploadfile', formData, fs=self._fs, remote=self.parent._remote)
    return len(text)
  
  def write_text(self, text: str, encoding='utf-8') -> int:
    formData = aiohttp.FormData()
    formData.add_field('file', text, filename=self.name)
    ret = RPath.client('operations/uploadfile', formData, fs=self._fs, remote=self.parent._remote)
    return len(text)
  
  def glob(self, pattern, *, case_sensitive=False):
    raise NotImplementedError()

  def rglob(self, pattern, *, case_sensitive=False):
    raise NotImplementedError()
    import fnmatch; _fnmatch = fnmatch.fnmatchcase if case_sensitive else fnmatch.fnmatch
    if self.is_dir():
      Q = [p for p in self.iterdir()]
      while Q:
        path = Q.pop()
        if path.is_file():
          if _fnmatch(path.name, pattern):
            yield path
        elif path.is_dir():
          if _fnmatch(path.name, pattern):
            yield path
          Q += [p for p in path.iterdir()]

class RCloneExited(Exception): pass

async def rclone_rcd(socket_path: pathlib.Path):
  '''
  Run rclone rcd on a unix socket
  
  :param socket_path: The unix socket rclone runs on
  :type socket_path: pathlib.Path
  '''
  # TODO stdout/stderr to console?
  proc = await asyncio.create_subprocess_exec(rclone, 'rcd', '--rc-serve', '--rc-no-auth', '--rc-addr', f"unix://{str(socket_path.absolute())}")
  try:
    await proc.wait()
    raise RCloneExited()
  except asyncio.CancelledError:
    import signal
    proc.send_signal(signal.SIGINT)
    await proc.wait()
    raise

async def rclone_rc_bridge(socket_path: pathlib.Path):
  '''
  Facilitate remote calls to rclone over the unix socket
  
  :param socket_path: The unix socket rclone runs on
  :type socket_path: pathlib.Path
  '''
  async def a_client(operation, formData=None, **params):
    async with aiohttp.ClientSession(connector=aiohttp.UnixConnector(path=str(socket_path.absolute()))) as session:
      async with session.post(
        f"http://localhost/{operation}",
        data=formData,
        params=params,
      ) as resp:
        return await resp.json()
  RPath.a_client = lambda operation, formData=None, a_client=a_client, **params: asyncio.create_task(a_client(operation, formData=formData, **params))
  def client(operation, formData=None, **params):
    ret = asyncio.run_coroutine_threadsafe(a_client(operation, formData=formData, **params), asyncio.get_event_loop()).result()
    logger.debug(f"{operation=} {formData=} {params=} {ret=}")
    return ret
  RPath.client = client
  try:
    await asyncio.Event().wait()
  finally:
    RPath.a_client = None
    RPath.client = None

@contextlib.asynccontextmanager
async def awith_rclone():
  socket_path = pathlib.Path(f"/tmp/{str(uuid.uuid4())}.sock")
  async with rpathlib.utils.awith_tasks(
    asyncio.create_task(rclone_rcd(socket_path)),
    asyncio.create_task(rclone_rc_bridge(socket_path)),
  ):
    await asyncio.sleep(1)# TODO confirm client is ready
    yield

@contextlib.contextmanager
def with_rclone():
  '''
  Run rclone services for the duration of the contextmanager
  '''
  with rpathlib.utils.with_awith(awith_rclone()):
    yield

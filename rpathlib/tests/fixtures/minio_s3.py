import json
import pytest
import rpathlib

def safe_predicate(predicate):
  try: return predicate()
  except: return False

def wait_for(predicate, interval=0.1, timeout=2.0):
  import time
  while not predicate():
    time.sleep(interval)
    timeout -= interval
    if timeout <= 0: raise TimeoutError()

def rpath():
  import shutil
  # look for the docker command for running an s3 server
  docker = shutil.which('docker')
  if docker is None:
    pytest.skip('docker binary not available')
    return
  import sys
  from subprocess import check_call
  if check_call([
    docker, 'pull', 'minio/minio'], 
    stderr=sys.stderr,
    stdout=sys.stdout,
  ) != 0:
    pytest.skip('dockerized minio not available')
    return
  import sys
  import uuid
  import socket
  import functools
  from urllib.request import Request, urlopen
  from subprocess import Popen
  # generate credentials for minio
  MINIO_ROOT_USER, MINIO_ROOT_PASSWORD = str(uuid.uuid4()), str(uuid.uuid4())
  # find a free port to run minio
  with socket.socket() as s:
    s.bind(('', 0))
    host, port = s.getsockname()
  # actually run minio
  proc = Popen(
    [
      docker, 'run',
      '-e', f"MINIO_ROOT_USER={MINIO_ROOT_USER}",
      '-e', f"MINIO_ROOT_PASSWORD={MINIO_ROOT_PASSWORD}",
      '-p', f"{port}:9000",
      '-i', 'minio/minio',
      'server', '/data'
    ],
    stderr=sys.stderr,
    stdout=sys.stdout,
  )
  # wait for minio to be running & ready
  wait_for(functools.partial(safe_predicate, lambda: urlopen(Request(f"http://localhost:{port}/minio/health/live", method='HEAD')).status == 200))
  try:
    with rpathlib.with_rclone():
      p = rpathlib.RPath(f":s3,provider=Minio,endpoint={json.dumps(f"http://localhost:{port}")},access_key_id={json.dumps(MINIO_ROOT_USER)},secret_access_key={json.dumps(MINIO_ROOT_PASSWORD)},directory_markers=true:")
      (p/'test').mkdir()
      yield p/'test'
  finally:
    proc.terminate()
    proc.wait()

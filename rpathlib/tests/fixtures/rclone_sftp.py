import socket
import json
import tempfile
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

def nc_z(host: str, port: int, timeout: int = 1):
  ''' Like nc -z but in python -- i.e. check if a tcp connection gets established
  (i.e. port open) but do nothing else.
  '''
  try:
    with socket.create_connection((host, port), timeout=timeout):
      return True
  except KeyboardInterrupt:
    raise
  except:
    return False

def rpath():
  import sys
  import uuid
  import socket
  from subprocess import Popen, check_output
  # generate credentials for rclone sftp
  SFTP_USER, SFTP_PASS = str(uuid.uuid4()), str(uuid.uuid4())
  # obtain rclone obscured pass
  SFTP_PASS_OBSCURED = check_output([rpathlib.rclone, 'obscure', '-'], input=SFTP_PASS.encode()).decode().strip()
  # find a free port to run rclone sftp
  with socket.socket() as s:
    s.bind(('', 0))
    host, port = s.getsockname()
  host = 'localhost'
  # actually run rclone sftp
  with tempfile.TemporaryDirectory() as tmpdir:
    proc = Popen(
      [
        rpathlib.rclone,
        'serve',
        'sftp',
        str(tmpdir),
        '--user', SFTP_USER,
        '--pass', SFTP_PASS,
        '--addr', f"{host}:{port}",
      ],
      stderr=sys.stderr,
      stdout=sys.stdout,
    )
    wait_for(lambda: nc_z(host, port))
    try:
      with rpathlib.with_rclone():
        yield rpathlib.RPath(f":sftp,host={host},port={port},user={json.dumps(SFTP_USER)},pass={json.dumps(SFTP_PASS_OBSCURED)}:")
    finally:
      proc.terminate()
      proc.wait()

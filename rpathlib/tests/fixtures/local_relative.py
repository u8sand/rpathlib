import rpathlib
import pathlib
import tempfile

def rpath():
  with rpathlib.with_rclone():
    with tempfile.TemporaryDirectory(dir=str(pathlib.Path.cwd())) as tmpdir:
      yield rpathlib.RPath(pathlib.Path(tmpdir).relative_to(pathlib.Path.cwd()))

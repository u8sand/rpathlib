import rpathlib
import tempfile

def rpath():
  with rpathlib.with_rclone():
    with tempfile.TemporaryDirectory() as tmpdir:
      yield rpathlib.RPath(tmpdir)

async def a_rpath():
  async with rpathlib.awith_rclone():
    with tempfile.TemporaryDirectory() as tmpdir:
      yield rpathlib.RPath(tmpdir)

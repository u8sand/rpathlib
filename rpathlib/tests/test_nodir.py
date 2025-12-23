import pytest
import rpathlib
from rpathlib.tests.fixtures import rpath

def test_nodir(rpath: rpathlib.RPath):
  with pytest.raises(FileNotFoundError):
    (rpath/'b').stat()
  (rpath/'b').write_text('hi\n')
  assert (rpath/'b').is_file()
  with rpath.mount() as p:
    assert {'b'} == {f.name for f in p.iterdir()}, p
  with (rpath/'b').open('rb+') as fh:
    assert fh.read() == b'hi\n'
    fh.seek(-1, 2)
    fh.write(b'!')
  assert (rpath/'b').read_text() == 'hi!'
  assert {'b'} == {f.name for f in rpath.iterdir()}
  (rpath/'b').rename('c')
  with pytest.raises(FileNotFoundError):
    (rpath/'b').read_text()
  assert {'c'} == {f.name for f in rpath.iterdir()}
  (rpath/'c').unlink()
  assert not (rpath/'c').exists()

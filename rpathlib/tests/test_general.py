import pytest
import rpathlib
from rpathlib.tests.fixtures import rpath

def test_general(rpath: rpathlib.RPath):
  rpath.mkdir(parents=True, exist_ok=True)
  (rpath/'a').mkdir()
  assert (rpath/'a').stat()['IsDir']
  with pytest.raises(FileNotFoundError):
    (rpath/'b').stat()
  (rpath/'b').write_text('hi\n')
  assert (rpath/'b').is_file()
  with (rpath/'b').open('rb+') as fh:
    assert fh.read() == b'hi\n'
    fh.seek(-1, 2)
    fh.write(b'!')
  assert (rpath/'b').read_text() == 'hi!'
  assert {'a', 'b'} == set(rpath.iterdir())
  (rpath/'a').rmdir()
  with pytest.raises(FileNotFoundError):
    (rpath/'a').rmdir()
  assert not (rpath/'a').exists()
  (rpath/'b').rename('c')
  # TODO: test rename to other remote
  with pytest.raises(FileNotFoundError):
    (rpath/'b').read_text()
  assert {'c'} == set(rpath.iterdir())
  (rpath/'c').unlink()
  assert not (rpath/'c').exists()

import pytest
import asyncio
import rpathlib
from rpathlib.tests.fixtures import a_rpath

@pytest.mark.asyncio
async def test_async(a_rpath: rpathlib.RPath):
  with pytest.raises(FileNotFoundError):
    await (a_rpath/'b').a_stat()
  await (a_rpath/'b').a_write_text('hi\n')
  assert await (a_rpath/'b').a_is_file()
  async with a_rpath.a_mount() as p:
    assert {'b'} == {f.name for f in await asyncio.to_thread(lambda p=p: list(p.iterdir()))}, p
  # with (a_rpath/'b').open('rb+') as fh:
  #   assert fh.read() == b'hi\n'
  #   fh.seek(-1, 2)
  #   fh.write(b'!')
  assert await (a_rpath/'b').a_read_text() == 'hi\n'
  assert {'b'} == {f.name async for f in a_rpath.a_iterdir()}
  await (a_rpath/'b').a_rename('c')
  with pytest.raises(FileNotFoundError):
    await (a_rpath/'b').a_read_text()
  assert {'c'} == {f.name async for f in a_rpath.a_iterdir()}
  await (a_rpath/'c').a_unlink()
  assert not await (a_rpath/'c').a_exists()

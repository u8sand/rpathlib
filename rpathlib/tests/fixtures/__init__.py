import pytest
import pytest_asyncio
import pathlib

@pytest.fixture(params=[
  p.stem
  for p in pathlib.Path(__file__).parent.glob('[!_]*.py')
])
def rpath(request):
  ''' Load different implementations from fixtures directory to be tested uniformly
  '''
  import importlib
  yield from importlib.import_module(f"rpathlib.tests.fixtures.{request.param}").rpath()

@pytest_asyncio.fixture(params=[
  p.stem
  for p in pathlib.Path(__file__).parent.glob('[!_]*.py')
])
async def a_rpath(request):
  ''' Load different implementations from fixtures directory to be tested uniformly
  '''
  import importlib
  mod = importlib.import_module(f"rpathlib.tests.fixtures.{request.param}")
  if getattr(mod, 'a_rpath', None) is None:
    import contextlib
    import rpathlib.utils
    async with rpathlib.utils.awith_with(contextlib.contextmanager(mod.rpath)) as rp:
      yield rp
  else:
    async for rpath in mod.a_rpath():
      yield rpath

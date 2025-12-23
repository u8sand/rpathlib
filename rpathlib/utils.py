import asyncio
import functools
import contextlib
import threading

@contextlib.asynccontextmanager
async def awith_tasks(*tasks):
  '''
  Run several tasks concurrently for the duration of an async context manager
  
  :param tasks: The asyncio tasks
  '''
  try:
    yield
  finally:
    for task in tasks:
      task.cancel()
      with contextlib.suppress(asyncio.CancelledError):
        await task

async def arun_until_done(asyn_ctx_mgr_coro, ready: asyncio.Event, done: asyncio.Event):
  '''
  Convert an async context manager into something that can be run in the background.
  
  :param asyn_ctx_mgr_coro: The async context manager coroutine
  :param ready: Ready event -- we set when "yield" is called
  :type ready: asyncio.Event
  :param done: Done even -- you set when "yield" returns
  :type done: asyncio.Event
  '''
  async with asyn_ctx_mgr_coro:
    ready.set()
    await done.wait()

def run_event_loop_until_done(loop: asyncio.BaseEventLoop, done: asyncio.Event):
  '''
  Run the loop as long as the done event is not set
  
  :param loop: The event loop
  :type loop: asyncio.BaseEventLoop
  :param done: An event object to mark when the loop should be closed
  :type done: asyncio.Event
  '''
  asyncio.set_event_loop(loop)
  loop.run_until_complete(done.wait())

@contextlib.contextmanager
def ensure_event_loop_thread():
  '''
  This ensures there is an event loop running in a dedicated thread
   for the duration of the context manager. If one already exists it
   uses that one.
  '''
  try: yield asyncio.get_running_loop()
  except RuntimeError: pass
  else: return
  #
  loop = asyncio.new_event_loop()
  asyncio.set_event_loop(loop)
  done = asyncio.Event()
  event_loop_thread = threading.Thread(target=run_event_loop_until_done, args=(loop, done))
  event_loop_thread.start()
  try:
    yield loop
  finally:
    loop.call_soon_threadsafe(done.set)
    event_loop_thread.join()
    loop.close()
    asyncio.set_event_loop(None)

@contextlib.contextmanager
def with_awith(awith):
  '''
  Run async context manager in an eventloop for the duration of the sync context manager
  
  :param awith: An async context manager coroutine
  '''
  with ensure_event_loop_thread() as loop:
    ready = asyncio.Event()
    done = asyncio.Event()
    services = asyncio.run_coroutine_threadsafe(arun_until_done(awith, ready, done), loop)
    try:
      asyncio.run_coroutine_threadsafe(ready.wait(), loop).result()
      yield
    finally:
      loop.call_soon_threadsafe(done.set)
      services.result()

def with_thread(loop: asyncio.AbstractEventLoop, ready: asyncio.Queue, done: asyncio.Event, withfn):
  with withfn() as ret:
    asyncio.run_coroutine_threadsafe(ready.put(ret), loop).result()
    asyncio.run_coroutine_threadsafe(done.wait(), loop).result()

@contextlib.asynccontextmanager
async def awith_with(withfn):
  '''
  Run context manager in an eventloop for the duration of the sync context manager
  
  :param awith: An context manager
  '''
  loop = asyncio.get_running_loop()
  ready = asyncio.Queue()
  done = asyncio.Event()
  task = asyncio.create_task(asyncio.to_thread(functools.partial(with_thread, loop, ready, done, withfn)))
  try:
    ctx = await ready.get()
    ready.task_done()
    yield ctx
  finally:
    done.set()
    await task

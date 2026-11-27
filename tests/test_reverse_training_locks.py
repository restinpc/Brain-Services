"""Exercise the actual ReverseStore constructor/lock method without DB/ML work."""
import ast
import asyncio
import gc
import os
from pathlib import Path
import weakref

path = Path(os.environ.get('BRAIN_TEST_REVERSE_FILE', '/brain/Brain-Services/shared/reverse_learning.py'))
tree = ast.parse(path.read_text())
original = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'ReverseStore')
methods = [n for n in original.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in ('__init__', '_get_train_lock')]
assert len(methods) == 2
cls = ast.ClassDef(name='ReverseStore', bases=[], keywords=[], body=methods, decorator_list=[])
unit = ast.fix_missing_locations(ast.Module(body=[cls], type_ignores=[]))
namespace = {'asyncio': asyncio, 'weakref': weakref}
exec(compile(unit, str(path), 'exec', flags=__import__('__future__').annotations.compiler_flag), namespace)
Store = namespace['ReverseStore']


async def main():
    store = Store(object(), port=8942)
    key = ('same-universe',)
    owner = await store._get_train_lock(key)
    await owner.acquire()
    waiter_entered = asyncio.Event()

    async def queued():
        lock = await store._get_train_lock(key)
        assert lock is owner
        async with lock:
            waiter_entered.set()

    waiter = asyncio.create_task(queued())
    await asyncio.sleep(0)
    try:
        for i in range(4200):
            await store._get_train_lock(('other', i))
        late = await store._get_train_lock(key)
        assert late is owner, 'Active training lock evicted; duplicate training can enter'
        assert late.locked() and not waiter_entered.is_set()
        print('PASS active owner + queued waiter retain the same lock beyond 4096 keys')
    finally:
        owner.release()
        await asyncio.wait_for(waiter, 1)

    # Cancellation of one request must not erase another request's ownership.
    await owner.acquire()
    cancelled = asyncio.create_task(queued())
    await asyncio.sleep(0)
    cancelled.cancel()
    try:
        await cancelled
    except asyncio.CancelledError:
        pass
    assert await store._get_train_lock(key) is owner
    assert owner.locked()
    owner.release()
    print('PASS cancelled waiter preserves the active owner lock')

    ref = weakref.ref(owner)
    del late, owner, waiter, cancelled
    gc.collect()
    await asyncio.sleep(0)
    gc.collect()
    assert ref() is None, 'Unused training locks retained'
    assert len(store._train_locks) == 0
    print('PASS unused locks released without a manual eviction race')


asyncio.run(main())

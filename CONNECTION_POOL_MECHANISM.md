# encode/databases 连接池生命周期机制分析报告

## 目录
1. [概述](#概述)
2. [连接池初始化机制](#连接池初始化机制)
3. [acquire 与 release 配合机制](#acquire-与-release-配合机制)
4. [事务机制深度分析](#事务机制深度分析)
5. [线程安全保障机制](#线程安全保障机制)
6. [完整生命周期流程图](#完整生命周期流程图)

---

## 概述

encode/databases 是一个异步数据库连接库，其连接池管理机制采用了分层设计：

- **核心层** (`core.py`): 提供 `Database`、`Connection`、`Transaction` 等高层抽象
- **后端层** (`backends/`): 封装不同数据库驱动的连接池实现（如 asyncpg、aiomysql）
- **接口层** (`interfaces.py`): 定义 `DatabaseBackend`、`ConnectionBackend` 等标准接口

这种设计使得高层 API 与具体数据库驱动解耦，同时保持了连接池管理的一致性。

---

## 连接池初始化机制

### 1. Database 实例化

当创建 `Database` 实例时，系统会：

1. 解析数据库 URL，识别后端类型
2. 动态导入对应的后端类（如 `PostgresBackend`、`MySQLBackend`）
3. 初始化后端实例，此时连接池尚未创建

```python
# databases/core.py:56-73
def __init__(
    self,
    url: typing.Union[str, "DatabaseURL"],
    *,
    force_rollback: bool = False,
    **options: typing.Any,
):
    self.url = DatabaseURL(url)
    self.options = options
    self.is_connected = False
    self._connection_map = weakref.WeakKeyDictionary()

    self._force_rollback = force_rollback

    backend_str = self._get_backend()
    backend_cls = import_from_string(backend_str)
    assert issubclass(backend_cls, DatabaseBackend)
    self._backend = backend_cls(self.url, **self.options)
```

**关键设计点**：
- `_connection_map`: 使用 `WeakKeyDictionary` 存储任务与连接的映射关系
- 后端类通过字符串路径动态导入，实现了运行时的后端选择

### 2. connect() 方法 - 连接池创建

当调用 `Database.connect()` 时，连接池才真正被创建：

```python
# databases/core.py:104-128
async def connect(self) -> None:
    """
    Establish the connection pool.
    """
    if self.is_connected:
        logger.debug("Already connected, skipping connection")
        return None

    await self._backend.connect()
    logger.info(
        "Connected to database %s", self.url.obscure_password, extra=CONNECT_EXTRA
    )
    self.is_connected = True
    # ... force_rollback 特殊处理
```

### 3. 后端连接池实现

不同数据库后端使用各自的连接池实现：

#### PostgreSQL (asyncpg)

```python
# databases/backends/postgres.py:64-74
async def connect(self) -> None:
    assert self._pool is None, "DatabaseBackend is already running"
    kwargs = dict(
        host=self._database_url.hostname,
        port=self._database_url.port,
        user=self._database_url.username,
        password=self._database_url.password,
        database=self._database_url.database,
    )
    kwargs.update(self._get_connection_kwargs())
    self._pool = await asyncpg.create_pool(**kwargs)
```

#### MySQL (aiomysql)

```python
# databases/backends/mysql.py:66-77
async def connect(self) -> None:
    assert self._pool is None, "DatabaseBackend is already running"
    kwargs = self._get_connection_kwargs()
    self._pool = await aiomysql.create_pool(
        host=self._database_url.hostname,
        port=self._database_url.port or 3306,
        user=self._database_url.username or getpass.getuser(),
        password=self._database_url.password,
        db=self._database_url.database,
        autocommit=True,
        **kwargs,
    )
```

**连接池配置参数**：
- `min_size` / `minsize`: 连接池最小连接数
- `max_size` / `maxsize`: 连接池最大连接数
- `pool_recycle`: 连接回收时间（MySQL 特有）
- `ssl`: SSL 连接配置

### 4. disconnect() 方法 - 连接池销毁

```python
# databases/core.py:129-154
async def disconnect(self) -> None:
    """
    Close all connections in the connection pool.
    """
    if not self.is_connected:
        logger.debug("Already disconnected, skipping disconnection")
        return None

    if self._force_rollback:
        # ... force_rollback 特殊处理
    else:
        self._connection = None

    await self._backend.disconnect()
    logger.info(
        "Disconnected from database %s",
        self.url.obscure_password,
        extra=DISCONNECT_EXTRA,
    )
    self.is_connected = False
```

后端的 `disconnect` 实现：

```python
# PostgreSQL
async def disconnect(self) -> None:
    assert self._pool is not None, "DatabaseBackend is not running"
    await self._pool.close()
    self._pool = None

# MySQL
async def disconnect(self) -> None:
    assert self._pool is not None, "DatabaseBackend is not running"
    self._pool.close()
    await self._pool.wait_closed()
    self._pool = None
```

---

## acquire 与 release 配合机制

### 1. 接口定义

`ConnectionBackend` 接口定义了标准的连接获取/释放方法：

```python
# databases/interfaces.py:18-23
class ConnectionBackend:
    async def acquire(self) -> None:
        raise NotImplementedError()

    async def release(self) -> None:
        raise NotImplementedError()
```

### 2. 后端实现

#### PostgreSQL 实现

```python
# databases/backends/postgres.py:91-100
async def acquire(self) -> None:
    assert self._connection is None, "Connection is already acquired"
    assert self._database._pool is not None, "DatabaseBackend is not running"
    self._connection = await self._database._pool.acquire()

async def release(self) -> None:
    assert self._connection is not None, "Connection is not acquired"
    assert self._database._pool is not None, "DatabaseBackend is not running"
    self._connection = await self._database._pool.release(self._connection)
    self._connection = None
```

#### MySQL 实现

```python
# databases/backends/mysql.py:100-109
async def acquire(self) -> None:
    assert self._connection is None, "Connection is already acquired"
    assert self._database._pool is not None, "DatabaseBackend is not running"
    self._connection = await self._database._pool.acquire()

async def release(self) -> None:
    assert self._connection is not None, "Connection is not acquired"
    assert self._database._pool is not None, "DatabaseBackend is not running"
    await self._database._pool.release(self._connection)
    self._connection = None
```

### 3. Connection 类的上下文管理

`Connection` 类实现了异步上下文管理器协议，通过引用计数机制管理连接的获取与释放：

```python
# databases/core.py:246-258
class Connection:
    def __init__(self, database: Database, backend: DatabaseBackend) -> None:
        self._database = database
        self._backend = backend

        self._connection_lock = asyncio.Lock()
        self._connection = self._backend.connection()
        self._connection_counter = 0

        self._transaction_lock = asyncio.Lock()
        self._transaction_stack: typing.List[Transaction] = []

        self._query_lock = asyncio.Lock()
```

#### `__aenter__` 方法 - 进入上下文

```python
# databases/core.py:259-268
async def __aenter__(self) -> "Connection":
    async with self._connection_lock:
        self._connection_counter += 1
        try:
            if self._connection_counter == 1:
                await self._connection.acquire()
        except BaseException as e:
            self._connection_counter -= 1
            raise e
    return self
```

**关键逻辑**：
1. 使用 `_connection_lock` 确保计数器操作的原子性
2. 只有当 `_connection_counter` 从 0 变为 1 时，才真正调用 `acquire()` 获取连接
3. 如果 `acquire()` 失败，回滚计数器并重新抛出异常

#### `__aexit__` 方法 - 退出上下文

```python
# databases/core.py:270-281
async def __aexit__(
    self,
    exc_type: typing.Optional[typing.Type[BaseException]] = None,
    exc_value: typing.Optional[BaseException] = None,
    traceback: typing.Optional[TracebackType] = None,
) -> None:
    async with self._connection_lock:
        assert self._connection is not None
        self._connection_counter -= 1
        if self._connection_counter == 0:
            await self._connection.release()
            self._database._connection = None
```

**关键逻辑**：
1. 同样使用 `_connection_lock` 保护计数器
2. 只有当 `_connection_counter` 从 1 变为 0 时，才真正调用 `release()` 释放连接
3. 释放连接后，清除 `Database` 实例中该任务的连接引用

### 4. 嵌套上下文支持

这种引用计数机制天然支持嵌套的 `async with` 上下文：

```python
async def example():
    async with database.connection() as conn1:
        # _connection_counter = 1, acquire() 被调用
        async with database.connection() as conn2:
            # _connection_counter = 2, acquire() 不被调用
            pass
        # _connection_counter = 1, release() 不被调用
    # _connection_counter = 0, release() 被调用
```

**设计优势**：
- 避免了重复获取/释放连接的开销
- 同一任务内的嵌套上下文共享同一个物理连接
- 简化了用户代码，无需担心嵌套导致的连接泄漏

### 5. Database.connection() 方法

```python
# databases/core.py:216-223
def connection(self) -> "Connection":
    if self._global_connection is not None:
        return self._global_connection

    if not self._connection:
        self._connection = Connection(self, self._backend)

    return self._connection
```

**任务隔离机制**：
- `_connection` 属性通过 `_connection_map` 实现任务级别的隔离
- 每个 `asyncio.Task` 有自己独立的 `Connection` 实例
- `_global_connection` 用于 `force_rollback` 模式（测试场景）

---

## 事务机制深度分析

### 1. Transaction 类的核心设计

#### 1.1 两种 transaction() 入口

`Database` 和 `Connection` 都提供了 `transaction()` 方法，但实现略有不同：

**Database.transaction()**:
```python
# databases/core.py:225-228
def transaction(
    self, *, force_rollback: bool = False, **kwargs: typing.Any
) -> "Transaction":
    return Transaction(self.connection, force_rollback=force_rollback, **kwargs)
```

**Connection.transaction()**:
```python
# databases/core.py:338-344
def transaction(
    self, *, force_rollback: bool = False, **kwargs: typing.Any
) -> "Transaction":
    def connection_callable() -> Connection:
        return self

    return Transaction(connection_callable, force_rollback, **kwargs)
```

**关键差异**：
- `Database.transaction()` 传入的是 `self.connection` **方法**（延迟调用）
- `Connection.transaction()` 通过闭包返回自身

这种设计允许 `Transaction` 类在需要时才获取连接，提高了灵活性。

#### 1.2 Transaction 类结构

```python
# databases/core.py:367-408
class Transaction:
    def __init__(
        self,
        connection_callable: typing.Callable[[], Connection],
        force_rollback: bool,
        **kwargs: typing.Any,
    ) -> None:
        self._connection_callable = connection_callable
        self._force_rollback = force_rollback
        self._extra_options = kwargs

    @property
    def _transaction(self) -> typing.Optional["TransactionBackend"]:
        transactions = _ACTIVE_TRANSACTIONS.get()
        if transactions is None:
            return None
        return transactions.get(self, None)

    @_transaction.setter
    def _transaction(
        self, transaction: typing.Optional["TransactionBackend"]
    ) -> typing.Optional["TransactionBackend"]:
        transactions = _ACTIVE_TRANSACTIONS.get()
        if transactions is None:
            transactions = weakref.WeakKeyDictionary()
        else:
            transactions = transactions.copy()  # 注意：每次修改都会创建副本

        if transaction is None:
            transactions.pop(self, None)
        else:
            transactions[self] = transaction

        _ACTIVE_TRANSACTIONS.set(transactions)
        return transactions.get(self, None)
```

**关键设计点**：
1. `_ACTIVE_TRANSACTIONS`: 使用 `ContextVar` + `WeakKeyDictionary` 跟踪当前上下文中的活跃事务
2. **Copy-on-write**: setter 中使用 `transactions.copy()`，确保 ContextVar 的正确更新
3. 弱引用避免内存泄漏

### 2. 嵌套事务处理机制

#### 2.1 事务栈 (`_transaction_stack`)

`Connection` 类维护一个事务栈来跟踪嵌套事务：

```python
# databases/core.py:238
self._transaction_stack: typing.List[Transaction] = []
```

#### 2.2 `start()` 方法 - 事务启动流程

```python
# databases/core.py:448-458
async def start(self) -> "Transaction":
    # 1. 创建后端事务对象
    self._transaction = self._connection._connection.transaction()

    async with self._connection._transaction_lock:
        # 2. 判断是否为根事务（栈为空则是根事务）
        is_root = not self._connection._transaction_stack
        
        # 3. 关键：pin 住连接！调用 Connection.__aenter__()
        await self._connection.__aenter__()
        
        # 4. 启动后端事务，传入 is_root 参数
        await self._transaction.start(
            is_root=is_root, extra_options=self._extra_options
        )
        
        # 5. 将当前事务压入栈
        self._connection._transaction_stack.append(self)
    return self
```

**核心逻辑**：
- `is_root = not self._connection._transaction_stack`: 栈为空表示这是第一个事务（根事务）
- `await self._connection.__aenter__()`: 增加连接引用计数，**pin 住连接不释放**

#### 2.3 `commit()` 方法 - 事务提交流程

```python
# databases/core.py:460-467
async def commit(self) -> None:
    async with self._connection._transaction_lock:
        # 1. 断言：必须是当前最内层事务（栈顶）
        assert self._connection._transaction_stack[-1] is self
        
        # 2. 从栈中弹出
        self._connection._transaction_stack.pop()
        
        # 3. 提交后端事务
        assert self._transaction is not None
        await self._transaction.commit()
        
        # 4. 减少连接引用计数（可能释放连接）
        await self._connection.__aexit__()
        
        # 5. 清理上下文变量
        self._transaction = None
```

#### 2.4 `rollback()` 方法 - 事务回滚流程

```python
# databases/core.py:469-476
async def rollback(self) -> None:
    async with self._connection._transaction_lock:
        # 1. 断言：必须是当前最内层事务
        assert self._connection._transaction_stack[-1] is self
        
        # 2. 从栈中弹出
        self._connection._transaction_stack.pop()
        
        # 3. 回滚后端事务
        assert self._transaction is not None
        await self._transaction.rollback()
        
        # 4. 减少连接引用计数
        await self._connection.__aexit__()
        
        # 5. 清理上下文变量
        self._transaction = None
```

#### 2.5 嵌套事务执行流程图

```
场景：双层嵌套事务

async with db.transaction():  # 外层（根事务）
    # 步骤：
    # 1. start() 被调用
    # 2. _transaction_stack 为空，is_root = True
    # 3. __aenter__() → _connection_counter = 1, acquire()
    # 4. 后端事务 start(is_root=True)
    # 5. _transaction_stack = [tx1]
    
    async with db.transaction():  # 内层（嵌套事务）
        # 步骤：
        # 1. start() 被调用
        # 2. _transaction_stack 非空，is_root = False
        # 3. __aenter__() → _connection_counter = 2, 不 acquire
        # 4. 后端事务 start(is_root=False) → 创建 SAVEPOINT
        # 5. _transaction_stack = [tx1, tx2]
        
        pass  # 内层 commit
        # 步骤：
        # 1. commit() 被调用
        # 2. 断言 _transaction_stack[-1] is tx2 ✓
        # 3. _transaction_stack.pop() → [tx1]
        # 4. 后端 commit() → RELEASE SAVEPOINT
        # 5. __aexit__() → _connection_counter = 1, 不 release
        
    pass  # 外层 commit
    # 步骤：
    # 1. commit() 被调用
    # 2. 断言 _transaction_stack[-1] is tx1 ✓
    # 3. _transaction_stack.pop() → []
    # 4. 后端 commit() → COMMIT
    # 5. __aexit__() → _connection_counter = 0, release()
```

### 3. 连接 Pin 住机制

#### 3.1 核心发现：事务期间连接被 Pin 住

**关键机制**：

| 操作 | 调用 | 连接计数器变化 |
|------|------|----------------|
| 事务启动 | `Transaction.start()` → `Connection.__aenter__()` | +1 |
| 事务提交 | `Transaction.commit()` → `Connection.__aexit__()` | -1 |
| 事务回滚 | `Transaction.rollback()` → `Connection.__aexit__()` | -1 |

这意味着：
1. **事务期间**：连接引用计数器 ≥ 1，连接**不会**被释放回池子
2. **所有嵌套事务完成后**：计数器归零，连接才真正 `release()`

#### 3.2 对比：普通查询 vs 事务查询

**普通查询（无事务）**：
```python
await db.fetch_all("SELECT * FROM users")
# 内部流程：
# 1. async with db.connection() as conn:
# 2.   __aenter__() → acquire (计数器 0→1)
# 3.   执行查询
# 4.   __aexit__() → release (计数器 1→0)
# 连接立即返回池子！
```

**事务内查询**：
```python
async with db.transaction():
    await db.fetch_all("SELECT * FROM users")  # 查询 1
    await db.fetch_all("SELECT * FROM orders")  # 查询 2
# 事务启动时：
# start() → __aenter__() → 计数器 0→1, acquire

# 查询 1 执行时：
# async with db.connection() → __aenter__() → 计数器 1→2
# 执行查询
# __aexit__() → 计数器 2→1 (不 release!)

# 查询 2 执行时：
# 同样：计数器 1→2→1

# 事务提交时：
# commit() → __aexit__() → 计数器 1→0, release
# 连接才真正返回池子！
```

#### 3.3 设计优势

1. **性能优化**：避免事务期间频繁获取/释放连接
2. **事务完整性**：确保同一事务内的所有操作使用同一个物理连接
3. **隔离性保证**：不同事务使用不同连接，互不干扰

### 4. Savepoint 机制详解

#### 4.1 MySQL Savepoint 实现

```python
# databases/backends/mysql.py:249-291
class MySQLTransaction(TransactionBackend):
    def __init__(self, connection: MySQLConnection):
        self._connection = connection
        self._is_root = False
        self._savepoint_name = ""

    async def start(
        self, is_root: bool, extra_options: typing.Dict[typing.Any, typing.Any]
    ) -> None:
        self._is_root = is_root
        if self._is_root:
            # 根事务：执行 BEGIN
            await self._connection._connection.begin()
        else:
            # 嵌套事务：创建 SAVEPOINT
            id = str(uuid.uuid4()).replace("-", "_")
            self._savepoint_name = f"STARLETTE_SAVEPOINT_{id}"
            cursor = await self._connection._connection.cursor()
            try:
                await cursor.execute(f"SAVEPOINT {self._savepoint_name}")
            finally:
                await cursor.close()

    async def commit(self) -> None:
        if self._is_root:
            # 根事务：执行 COMMIT
            await self._connection._connection.commit()
        else:
            # 嵌套事务：释放 SAVEPOINT
            cursor = await self._connection._connection.cursor()
            try:
                await cursor.execute(f"RELEASE SAVEPOINT {self._savepoint_name}")
            finally:
                await cursor.close()

    async def rollback(self) -> None:
        if self._is_root:
            # 根事务：执行 ROLLBACK
            await self._connection._connection.rollback()
        else:
            # 嵌套事务：回滚到 SAVEPOINT
            cursor = await self._connection._connection.cursor()
            try:
                await cursor.execute(f"ROLLBACK TO SAVEPOINT {self._savepoint_name}")
            finally:
                await cursor.close()
```

#### 4.2 Savepoint 命名规则

**命名格式**：
```
STARLETTE_SAVEPOINT_{uuid4_with_underscores}
```

**生成规则**：
1. 使用 `uuid.uuid4()` 生成唯一标识符
2. 将 `-` 替换为 `_`（SQL 标识符规范）
3. 前缀为 `STARLETTE_SAVEPOINT_`

**示例**：
```
STARLETTE_SAVEPOINT_a1b2c3d4_e5f6_7890_abcd_ef1234567890
```

**设计考虑**：
- UUID 确保全局唯一性，避免命名冲突
- 使用 `_` 而非 `-` 符合 SQL 标识符规范
- 前缀便于识别和调试

#### 4.3 PostgreSQL Savepoint 实现

PostgreSQL 的实现与 MySQL 不同，它直接使用 asyncpg 内置的事务 API：

```python
# databases/backends/postgres.py:200-218
class PostgresTransaction(TransactionBackend):
    def __init__(self, connection: PostgresConnection):
        self._connection = connection
        self._transaction: typing.Optional[asyncpg.transaction.Transaction] = None

    async def start(
        self, is_root: bool, extra_options: typing.Dict[typing.Any, typing.Any]
    ) -> None:
        # asyncpg 的 connection.transaction() 会自动处理嵌套
        self._transaction = self._connection._connection.transaction(**extra_options)
        await self._transaction.start()

    async def commit(self) -> None:
        await self._transaction.commit()

    async def rollback(self) -> None:
        await self._transaction.rollback()
```

**asyncpg 内部机制**：
- asyncpg 的 `connection.transaction()` 方法会自动检测是否已有活跃事务
- 如果已有活跃事务，它会自动创建 savepoint 而非新事务
- 这种设计对上层代码透明，简化了 API

### 5. 回滚时连接状态恢复

#### 5.1 MySQL 回滚恢复

| 事务类型 | 回滚操作 | 连接状态变化 |
|----------|----------|--------------|
| **根事务** | `ROLLBACK` | 回滚所有修改，回到自动提交状态 |
| **嵌套事务** | `ROLLBACK TO SAVEPOINT` | 只回滚到 savepoint，外层事务继续活跃 |

**根事务回滚示例**：
```python
async with db.transaction():  # 根事务
    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    
    # 触发异常
    raise Exception("Something went wrong")
    
# __aexit__ 检测到异常，调用 rollback()
# MySQL 执行: ROLLBACK
# 连接状态：所有修改被撤销，回到自动提交
```

**嵌套事务回滚示例**：
```python
async with db.transaction():  # 根事务 (is_root=True)
    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    
    async with db.transaction():  # 嵌套事务 (is_root=False)
        await db.execute("INSERT INTO users (name) VALUES ('Bob')")
        
        # 内层触发异常
        raise Exception("Inner error")
    
    # 内层回滚后，外层继续
    # 注意：如果内层异常未被捕获，外层也会回滚
    
# 如果内层异常被捕获：
# - 内层: ROLLBACK TO SAVEPOINT → Bob 被撤销
# - 外层: COMMIT → Alice 被保留
```

#### 5.2 PostgreSQL 回滚恢复

PostgreSQL 使用 asyncpg 的内置事务管理：

```python
# asyncpg 的 transaction.rollback() 内部逻辑：
# - 如果是根事务：执行 ROLLBACK
# - 如果是嵌套事务（savepoint）：执行 ROLLBACK TO SAVEPOINT
```

**与 MySQL 的差异**：
- PostgreSQL 不需要手动判断 `is_root`，asyncpg 内部处理
- 但 `is_root` 参数仍然传递，用于可能的扩展选项

#### 5.3 连接状态一致性保障

无论 PostgreSQL 还是 MySQL，回滚后都确保：

1. **连接引用计数正确**：`__aexit__()` 被调用，计数器递减
2. **事务栈清理**：`_transaction_stack.pop()` 确保栈状态正确
3. **上下文变量清理**：`self._transaction = None` 确保 ContextVar 更新
4. **锁保护**：所有操作在 `_transaction_lock` 保护下执行

### 6. 事务异常处理

#### 6.1 `__aexit__` 中的异常检测

```python
# databases/core.py:416-428
async def __aexit__(
    self,
    exc_type: typing.Optional[typing.Type[BaseException]] = None,
    exc_value: typing.Optional[BaseException] = None,
    traceback: typing.Optional[TracebackType] = None,
) -> None:
    """
    Called when exiting `async with database.transaction()`
    """
    if exc_type is not None or self._force_rollback:
        await self.rollback()
    else:
        await self.commit()
```

**回滚触发条件**：
1. `exc_type is not None`: 上下文中发生异常
2. `self._force_rollback`: 强制回滚模式（测试用）

#### 6.2 异常场景流程图

```
场景：内层事务异常，外层捕获

async with db.transaction():  # 外层 T1
    await db.execute("INSERT INTO users VALUES (1)")  # 操作 A
    
    try:
        async with db.transaction():  # 内层 T2
            await db.execute("INSERT INTO users VALUES (2)")  # 操作 B
            raise ValueError("Oops!")  # 异常
    except ValueError:
        pass  # 捕获异常，继续执行
    
    await db.execute("INSERT INTO users VALUES (3)")  # 操作 C

# 执行流程：
# 1. T1.start(): _transaction_stack=[T1], counter=1
# 2. 操作 A: counter=1→2→1 (不 release)
# 3. T2.start(): _transaction_stack=[T1,T2], counter=2
# 4. 操作 B: counter=2→3→2
# 5. 异常抛出 → 进入 T2.__aexit__
# 6. exc_type is not None → T2.rollback()
#    - MySQL: ROLLBACK TO SAVEPOINT (操作 B 撤销)
#    - counter=2→1
#    - _transaction_stack=[T1]
# 7. 异常被 except 捕获
# 8. 操作 C: counter=1→2→1
# 9. T1.__aexit__: 无异常 → T1.commit()
#    - MySQL: COMMIT (操作 A、C 保留)
#    - counter=1→0, release()

# 最终结果：
# - 操作 A: 已提交 ✓
# - 操作 B: 已回滚 ✗
# - 操作 C: 已提交 ✓
```

### 7. 事务机制总结

| 特性 | 实现方式 | 关键代码位置 |
|------|----------|--------------|
| **嵌套事务支持** | 事务栈 + Savepoint | `core.py:448-476` |
| **连接 Pin 住** | 引用计数 + `__aenter__/__aexit__` | `core.py:453, 466, 475` |
| **Savepoint 命名** | UUID + 固定前缀 | `mysql.py:263-264` |
| **异常自动回滚** | `__aexit__` 检测 `exc_type` | `core.py:425-428` |
| **线程安全** | `_transaction_lock` | `core.py:451, 461, 470` |

---

## 线程安全保障机制

### 1. 多锁分层保护

`Connection` 类使用三把不同的锁来保护不同层面的操作：

| 锁名称 | 保护对象 | 用途 |
|--------|----------|------|
| `_connection_lock` | `_connection_counter` + 连接获取/释放 | 确保连接引用计数和获取/释放操作的原子性 |
| `_transaction_lock` | `_transaction_stack` + 事务操作 | 确保事务嵌套和回滚点的正确性 |
| `_query_lock` | 数据库查询执行 | 确保同一连接上的查询串行执行 |

### 2. 锁的具体应用

#### 连接获取/释放锁

```python
# databases/core.py:259-268
async def __aenter__(self) -> "Connection":
    async with self._connection_lock:
        self._connection_counter += 1
        try:
            if self._connection_counter == 1:
                await self._connection.acquire()
        except BaseException as e:
            self._connection_counter -= 1
            raise e
    return self
```

**保护点**：
- 计数器递增
- 条件判断 (`_connection_counter == 1`)
- `acquire()` 调用
- 异常时的计数器回滚

#### 事务操作锁

```python
# databases/core.py:448-458
async def start(self) -> "Transaction":
    self._transaction = self._connection._connection.transaction()

    async with self._connection._transaction_lock:
        is_root = not self._connection._transaction_stack
        await self._connection.__aenter__()
        await self._transaction.start(
            is_root=is_root, extra_options=self._extra_options
        )
        self._connection._transaction_stack.append(self)
    return self
```

**保护点**：
- 事务栈的检查和修改
- 嵌套事务的层级判断 (`is_root`)
- 事务的启动和提交/回滚

#### 查询执行锁

```python
# databases/core.py:283-290
async def fetch_all(
    self,
    query: typing.Union[ClauseElement, str],
    values: typing.Optional[dict] = None,
) -> typing.List[Record]:
    built_query = self._build_query(query, values)
    async with self._query_lock:
        return await self._connection.fetch_all(built_query)
```

**保护点**：
- 确保同一连接上的查询不会并发执行
- 避免底层数据库驱动的并发问题

### 3. 任务级连接隔离

```python
# databases/core.py:54, 66, 87-102
_connection_map: "weakref.WeakKeyDictionary[asyncio.Task, 'Connection']"

def __init__(self, ...):
    self._connection_map = weakref.WeakKeyDictionary()

@property
def _connection(self) -> typing.Optional["Connection"]:
    return self._connection_map.get(self._current_task)

@_connection.setter
def _connection(
    self, connection: typing.Optional["Connection"]
) -> typing.Optional["Connection"]:
    task = self._current_task

    if connection is None:
        self._connection_map.pop(task, None)
    else:
        self._connection_map[task] = connection

    return self._connection
```

**设计原理**：
1. 使用 `weakref.WeakKeyDictionary` 以 `asyncio.Task` 为键存储连接
2. 每个任务有自己独立的 `Connection` 实例
3. 任务结束时，弱引用会自动清理，避免内存泄漏
4. 不同任务之间的连接完全隔离，互不干扰

### 4. 弱引用的优势

| 特性 | 说明 |
|------|------|
| 自动清理 | 任务结束后，连接引用自动从字典中移除 |
| 内存安全 | 不会因为字典持有引用而阻止任务被垃圾回收 |
| 并发友好 | 不同任务操作不同的键，天然避免了竞争条件 |

### 5. 并发场景分析

假设有 100 个并发请求同时到达：

1. **连接池层面**：
   - 后端连接池（如 asyncpg Pool）有自己的并发控制机制
   - 当连接数达到 `max_size` 时，新的 `acquire()` 会等待
   - 这种等待是在底层驱动层面实现的

2. **应用层面**：
   - 每个请求对应一个 `asyncio.Task`
   - 每个任务通过 `_connection_map` 获取自己的 `Connection` 实例
   - 不同任务的 `Connection` 实例是独立的，互不干扰

3. **锁的作用**：
   - `_connection_lock` 保护的是**单个任务内**的连接引用计数
   - 不同任务有不同的 `Connection` 实例，所以锁不会跨任务竞争
   - 这意味着并发请求之间不会因为应用层的锁而互相阻塞

---

## 完整生命周期流程图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           连接池完整生命周期                                   │
└─────────────────────────────────────────────────────────────────────────────┘

阶段 1: 初始化
─────────────────────────────────────────────────────────────────────────────

  User Code                    Database                     Backend
     │                            │                            │
     │── Database(url) ──────────>│                            │
     │                            │── _get_backend()           │
     │                            │── import_from_string()     │
     │                            │── backend_cls(...) ───────>│
     │                            │                            │── _pool = None
     │◄───────────────────────────│◄───────────────────────────│
     │                            │                            │

阶段 2: 连接池创建 (connect())
─────────────────────────────────────────────────────────────────────────────

  User Code                    Database                     Backend
     │                            │                            │
     │── await connect() ────────>│                            │
     │                            │── is_connected = False?    │
     │                            │── await _backend.connect()─>│
     │                            │                            │── asyncpg.create_pool()
     │                            │                            │   或 aiomysql.create_pool()
     │                            │                            │── _pool = <Pool实例>
     │                            │── is_connected = True      │
     │◄───────────────────────────│◄───────────────────────────│
     │                            │                            │

阶段 3: 连接获取 (async with connection())
─────────────────────────────────────────────────────────────────────────────

  User Code                    Database                     Connection           Backend Connection
     │                            │                            │                        │
     │── async with db.connection() ──>│                       │                        │
     │                            │── _global_connection?      │                        │
     │                            │── _connection_map[task]?   │                        │
     │                            │── 创建 Connection() ───────>│                        │
     │                            │                            │── _connection = backend.connection()
     │                            │                            │── _connection_counter = 0
     │                            │◄───────────────────────────│                        │
     │                            │                            │                        │
     │── __aenter__() ───────────>│                            │                        │
     │                            │                            │── async with _connection_lock:
     │                            │                            │   ├── _connection_counter += 1 (now 1)
     │                            │                            │   ├── _connection_counter == 1? YES
     │                            │                            │   └── await _connection.acquire() ───>│
     │                            │                            │                        │── await _pool.acquire()
     │                            │                            │                        │── _connection = <实际连接>
     │                            │                            │◄───────────────────────│
     │                            │◄───────────────────────────│                        │
     │◄───────────────────────────│                            │                        │
     │                            │                            │                        │

阶段 4: 执行查询
─────────────────────────────────────────────────────────────────────────────

  User Code                    Connection           Backend Connection
     │                            │                        │
     │── await fetch_all(query) ─>│                        │
     │                            │── _build_query()        │
     │                            │── async with _query_lock:
     │                            │   └── await _connection.fetch_all() ──>│
     │                            │                        │── 实际数据库查询
     │                            │◄───────────────────────│
     │◄───────────────────────────│                        │
     │                            │                        │

阶段 5: 连接释放 (退出 async with)
─────────────────────────────────────────────────────────────────────────────

  User Code                    Connection           Backend Connection
     │                            │                        │
     │── __aexit__() ────────────>│                        │
     │                            │── async with _connection_lock:
     │                            │   ├── assert _connection is not None
     │                            │   ├── _connection_counter -= 1 (now 0)
     │                            │   ├── _connection_counter == 0? YES
     │                            │   ├── await _connection.release() ─────>│
     │                            │                        │── await _pool.release(_connection)
     │                            │                        │── _connection = None
     │                            │   └── _database._connection = None
     │                            │                            │ (从 _connection_map 中移除)
     │                            │◄───────────────────────│
     │◄───────────────────────────│                        │
     │                            │                        │

阶段 6: 连接池销毁 (disconnect())
─────────────────────────────────────────────────────────────────────────────

  User Code                    Database                     Backend
     │                            │                            │
     │── await disconnect() ─────>│                            │
     │                            │── is_connected = True?     │
     │                            │── _connection = None       │
     │                            │── await _backend.disconnect()─>│
     │                            │                            │── await _pool.close()
     │                            │                            │   (MySQL: await _pool.wait_closed())
     │                            │                            │── _pool = None
     │                            │── is_connected = False     │
     │◄───────────────────────────│◄───────────────────────────│
     │                            │                            │
```

---

## 关键设计总结

### 1. 分层架构

```
┌─────────────────────────────────────────────────────────────┐
│                      Database (核心层)                        │
│  - 任务级连接隔离 (_connection_map)                           │
│  - 连接引用计数 (_connection_counter)                         │
│  - 高层 API (fetch_all, execute, transaction 等)              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Connection (连接层)                          │
│  - 异步上下文管理器 (__aenter__/__aexit__)                    │
│  - 多锁保护 (_connection_lock, _query_lock, _transaction_lock)│
│  - 连接代理 (委托给 ConnectionBackend)                         │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              Backend (后端层 - 如 PostgresBackend)            │
│  - 连接池创建/销毁 (connect/disconnect)                        │
│  - 连接工厂 (connection() 方法)                                │
│  - 驱动特定配置                                                 │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│        ConnectionBackend (后端连接层 - 如 PostgresConnection)  │
│  - 实际连接获取/释放 (acquire/release)                         │
│  - 数据库操作执行 (fetch_all, execute 等)                      │
│  - 直接调用底层驱动 (asyncpg, aiomysql 等)                     │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              底层驱动连接池 (asyncpg.Pool, aiomysql.Pool)      │
│  - 物理连接管理                                                 │
│  - 连接复用                                                     │
│  - 并发控制 (max_size 限制)                                     │
└─────────────────────────────────────────────────────────────┘
```

### 2. 核心机制对比

| 机制 | 实现位置 | 关键技术 |
|------|----------|----------|
| 连接池创建/销毁 | Backend | `asyncpg.create_pool()` / `aiomysql.create_pool()` |
| 连接获取/释放 | ConnectionBackend | `pool.acquire()` / `pool.release()` |
| 引用计数管理 | Connection | `_connection_counter` + `_connection_lock` |
| 任务隔离 | Database | `WeakKeyDictionary[Task, Connection]` |
| 查询串行化 | Connection | `_query_lock` |
| 事务嵌套 | Connection + Transaction | `_transaction_stack` + `_transaction_lock` |

### 3. 并发安全保障

1. **任务级隔离**：每个 `asyncio.Task` 有独立的 `Connection` 实例，避免跨任务竞争
2. **细粒度锁**：不同操作使用不同的锁，减少锁竞争范围
3. **原子操作**：计数器操作与连接获取/释放在同一锁保护下执行
4. **异常安全**：获取失败时回滚计数器，避免状态不一致

---

## 使用示例

### 基本使用

```python
from databases import Database

database = Database("postgresql://localhost/mydb")

async def main():
    # 阶段 1-2: 创建连接池
    await database.connect()
    
    try:
        # 阶段 3-5: 获取连接、执行查询、释放连接
        async with database.connection() as conn:
            rows = await conn.fetch_all("SELECT * FROM users")
            print(rows)
        
        # 简化写法（内部自动使用 connection()）
        rows = await database.fetch_all("SELECT * FROM users")
    finally:
        # 阶段 6: 销毁连接池
        await database.disconnect()
```

### 使用异步上下文管理器

```python
async def main():
    # connect() 和 disconnect() 自动调用
    async with Database("postgresql://localhost/mydb") as database:
        rows = await database.fetch_all("SELECT * FROM users")
        print(rows)
```

### 嵌套上下文（共享连接）

```python
async def nested_example(database: Database):
    async with database.connection() as conn1:
        # 连接计数: 1, 已 acquire
        async with database.connection() as conn2:
            # 连接计数: 2, 未重复 acquire
            # conn1 和 conn2 实际上共享同一个物理连接
            pass
        # 连接计数: 1, 未 release
    # 连接计数: 0, 已 release
```

---

## 注意事项

1. **连接池参数配置**：
   - 根据实际并发量调整 `min_size` 和 `max_size`
   - MySQL 可配置 `pool_recycle` 避免连接超时

2. **任务生命周期管理**：
   - 确保任务结束时连接被正确释放
   - 避免在任务间共享 `Connection` 实例

3. **异常处理**：
   - 始终使用 `async with` 或 try/finally 确保连接释放
   - 注意 `acquire()` 失败时的计数器回滚机制

4. **并发控制**：
   - 应用层锁只保护单个任务内的操作
   - 真正的并发限制由底层连接池的 `max_size` 控制

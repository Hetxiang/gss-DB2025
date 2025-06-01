# RMDB yacc.tab.c 解析器分析文档

## 文件概述

`yacc.tab.c` 是由 GNU Bison 3.5.1 从 `yacc.y` 语法文件自动生成的 LALR(1) 语法解析器实现。这个文件是RMDB数据库系统SQL语句解析的核心组件。

### 文件基本信息
- **生成工具**: GNU Bison 3.5.1
- **解析器类型**: LALR(1) (Look-Ahead Left-to-Right, 1 token lookahead)
- **源语法文件**: `yacc.y`
- **目标语言**: C/C++
- **代码行数**: 2,158 行

## 解析器架构

### 1. 基础配置
```c
#define YYBISON 1           // 标识为Bison生成的解析器
#define YYBISON_VERSION "3.5.1"
#define YYSKELETON_NAME "yacc.c"
#define YYPURE 2            // 纯解析器（可重入）
#define YYFINAL 34          // 终止状态号
#define YYLAST 101          // 状态表最后索引
#define YYNTOKENS 43        // 终结符数量
#define YYNNTS 24           // 非终结符数量
#define YYNRULES 56         // 语法规则数量
#define YYNSTATES 111       // 状态数量
```

### 2. Token定义与映射

#### 关键字Token (Keywords)
```c
// SQL DDL关键字
SHOW, TABLES, CREATE, TABLE, DROP, DESC, INDEX
// SQL DML关键字  
INSERT, INTO, VALUES, DELETE, FROM, WHERE, UPDATE, SET, SELECT
// 数据类型关键字
INT, CHAR, FLOAT
// 逻辑运算符
AND, JOIN
// 系统命令
EXIT, HELP
// 事务控制
TXN_BEGIN, TXN_COMMIT, TXN_ABORT, TXN_ROLLBACK
// 排序和优化
ASC, ORDER, BY, ENABLE_NESTLOOP, ENABLE_SORTMERGE
```

#### 运算符Token
```c
LEQ (<=), NEQ (!=), GEQ (>=)  // 比较运算符
'=', '<', '>', '*'            // 基本运算符
'(', ')', ',', '.', ';'       // 分隔符
```

#### 值类型Token
```c
IDENTIFIER      // 标识符（表名、列名等）
VALUE_STRING    // 字符串字面量
VALUE_INT       // 整数字面量
VALUE_FLOAT     // 浮点数字面量
VALUE_BOOL      // 布尔值字面量
```

### 3. 语法规则结构

#### 语法规则层次
```
start          → stmt ';' | HELP | EXIT | T_EOF
stmt           → dbStmt | ddl | dml | txnStmt | setStmt
dbStmt         → SHOW TABLES
ddl            → CREATE/DROP TABLE | CREATE/DROP INDEX | DESC
dml            → INSERT | DELETE | UPDATE | SELECT
txnStmt        → TXN_BEGIN | TXN_COMMIT | TXN_ABORT | TXN_ROLLBACK
setStmt        → SET 配置参数
```

#### 主要语法规则分析

**1. DDL（数据定义语言）规则**
```yacc
// 创建表
CREATE TABLE tbName '(' fieldList ')'
// 删除表  
DROP TABLE tbName
// 描述表结构
DESC tbName
// 创建/删除索引
CREATE INDEX tbName '(' colNameList ')'
DROP INDEX tbName '(' colNameList ')'
```

**2. DML（数据操作语言）规则**
```yacc
// 插入数据
INSERT INTO tbName VALUES '(' valueList ')'
// 删除数据
DELETE FROM tbName optWhereClause
// 更新数据
UPDATE tbName SET setClauses optWhereClause  
// 查询数据
SELECT selector FROM tableList optWhereClause opt_order_clause
```

**3. 条件表达式规则**
```yacc
condition     → col op expr
whereClause   → condition | whereClause AND condition
optWhereClause → ε | WHERE whereClause
```

**4. 数据类型规则**
```yacc
type → INT | CHAR '(' VALUE_INT ')' | FLOAT
```

### 4. 语义动作 (Semantic Actions)

每个语法规则都对应一个语义动作，负责构建AST节点：

#### AST节点创建示例
```c
// Case 10: CREATE TABLE语句
{
    $$ = std::make_shared<CreateTable>($3, $5);
}

// Case 15: INSERT语句  
{
    $$ = std::make_shared<InsertStmt>($3, $6);
}

// Case 16: DELETE语句
{
    $$ = std::make_shared<DeleteStmt>($3, $4);  
}

// Case 17: UPDATE语句
{
    $$ = std::make_shared<UpdateStmt>($2, $4, $5);
}

// Case 18: SELECT语句
{
    $$ = std::make_shared<SelectStmt>($2, $4, $5, $6);
}
```

### 5. 错误处理机制

#### 错误报告函数
```c
void yyerror(YYLTYPE *locp, const char* s) {
    std::cerr << "Parser Error at line " << locp->first_line 
              << " column " << locp->first_column 
              << ": " << s << std::endl;
}
```

#### 错误恢复策略
- **错误检测**: 当遇到无法匹配的token时触发错误
- **错误恢复**: 通过状态栈回退和token丢弃机制恢复解析
- **错误报告**: 提供精确的行号和列号信息

### 6. 解析器状态机

#### 状态转换表
- **yypact[]**: 状态-动作表，决定在当前状态下看到特定token时的动作
- **yytable[]**: 转换表，存储状态转换和归约信息
- **yycheck[]**: 检查表，验证状态转换的有效性
- **yydefact[]**: 默认动作表，当没有明确动作时使用的默认归约

#### 解析算法流程
1. **Token读取**: 从词法分析器获取下一个token
2. **状态查询**: 根据当前状态和token查找动作
3. **动作执行**: 
   - **移入(Shift)**: 将token压入栈，转换状态
   - **归约(Reduce)**: 根据语法规则归约，创建AST节点
   - **接受(Accept)**: 解析成功完成
   - **错误(Error)**: 触发错误处理

### 7. 内存管理

#### 栈管理
```c
// 三个并行栈
yy_state_t *yyss;    // 状态栈
YYSTYPE *yyvs;       // 值栈（存储AST节点）
YYLTYPE *yyls;       // 位置栈（存储位置信息）
```

#### 动态内存分配
- **初始栈大小**: YYINITDEPTH (200)
- **最大栈大小**: YYMAXDEPTH (10000)  
- **栈扩展**: 当栈满时自动扩展为原来的2倍

### 8. 集成接口

#### 外部接口函数
```c
int yylex(YYSTYPE *yylval, YYLTYPE *yylloc);  // 词法分析器接口
void yyerror(YYLTYPE *locp, const char* s);   // 错误报告接口
```

#### 全局变量
```c
extern std::shared_ptr<ast::TreeNode> parse_tree;  // 解析结果AST根节点
extern int yydebug;                                 // 调试模式开关
```

## 解析器特性

### 1. 可重入性
- 使用纯解析器模式 (`%define api.pure full`)
- 所有状态信息通过参数传递，支持多线程环境

### 2. 位置追踪
- 启用位置信息 (`%locations`)
- 每个token和AST节点都包含精确的源码位置

### 3. 详细错误报告
- 启用详细错误信息 (`%define parse.error verbose`)
- 提供上下文相关的错误描述

### 4. 高效解析
- LALR(1)算法提供线性时间复杂度
- 预计算的状态转换表确保高效解析

## SQL语言支持范围

### 1. 支持的SQL语句类型
- **DDL**: CREATE TABLE, DROP TABLE, DESC TABLE, CREATE INDEX, DROP INDEX
- **DML**: INSERT, DELETE, UPDATE, SELECT
- **事务控制**: BEGIN, COMMIT, ABORT, ROLLBACK
- **系统命令**: SHOW TABLES, HELP, EXIT
- **查询优化设置**: SET ENABLE_NESTLOOP, SET ENABLE_SORTMERGE

### 2. 支持的数据类型
- **INT**: 整数类型
- **CHAR(n)**: 固定长度字符串
- **FLOAT**: 浮点数类型
- **BOOL**: 布尔类型

### 3. 支持的操作符
- **比较操作符**: =, <, >, <=, >=, !=
- **逻辑操作符**: AND
- **连接操作符**: JOIN

### 4. 查询特性
- **投影**: SELECT列列表或*
- **选择**: WHERE条件子句  
- **连接**: JOIN多表查询
- **排序**: ORDER BY子句（ASC/DESC）

## 与词法分析器协作

### Token流处理
1. **词法分析器** (`lex.yy.c`) 将SQL文本转换为token流
2. **语法分析器** (`yacc.tab.c`) 根据语法规则解析token流
3. **AST构建器** 在语义动作中创建抽象语法树节点

### 数据流向
```
SQL文本 → 词法分析器 → Token流 → 语法分析器 → AST → 语义分析器
```

## 总结

`yacc.tab.c` 是RMDB数据库系统SQL解析的核心实现，具备以下关键特性：

1. **完整的SQL语法支持**: 涵盖基本的DDL、DML、事务控制和系统命令
2. **健壮的错误处理**: 提供精确的错误定位和恢复机制  
3. **高效的解析算法**: LALR(1)算法确保线性时间复杂度
4. **可维护的架构**: 自动生成的代码保证与语法定义的一致性
5. **完善的AST构建**: 每个语法规则对应明确的AST节点创建逻辑

该解析器为RMDB系统提供了可靠、高效的SQL语句解析能力，是整个数据库查询处理流水线的重要基础组件。

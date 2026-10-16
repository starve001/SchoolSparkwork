# 第 7 章　Pandas 索引基础

> 对应 notebook：`notebooks/07_Pandas索引基础.ipynb`

---

## 一、DataFrame 的创建

```python
import pandas as pd

data = [['Alex', 10], ['Bob', 12], ['Clarke', 13]]
df = pd.DataFrame(data, columns=['Name', 'Age'])
print(df)
```

输出：

```
     Name  Age
0    Alex   10
1     Bob   12
2  Clarke   13
```

> 未指定索引时，Pandas 自动生成 `0, 1, 2, ...` 的**整数位置索引**。

---

## 二、两种索引方式：loc 与 iloc

```python
print(df.loc[1])      # 按标签
print(df.iloc[0])     # 按位置
```

输出：

```
Name    Bob
Age      12
Name: 1, dtype: object

Name    Alex
Age      10
Name: 0, dtype: object
```

| 方法 | 依据 | 说明 |
| --- | --- | --- |
| `loc[]` | **标签（label）** | 闭区间，`loc[0:2]` 含 2 |
| `iloc[]` | **位置（integer position）** | 半开区间，`iloc[0:2]` 不含 2 |
| `at[]` / `iat[]` | 单个元素 | `at` 按标签、`iat` 按位置，速度更快 |

> **易错点**：`loc` 的切片是**闭区间**（含右端点），`iloc` 的切片是**半开区间**（不含右端点）。这是考试高频考点。

---

## 三、设置索引：set_index

```python
# 写法一：原地修改（inplace=True）
df.set_index('Name', inplace=True)
print(df)

# 写法二：返回新对象
df1 = df.set_index('Name')
print(df1)
```

设置后：

```
        Age
Name       
Alex     10
Bob      12
Clarke   13
```

### 3.1 关键坑点

```python
df = pd.DataFrame(data, columns=['Name', 'Age'])
df.set_index('Name', inplace=True)     # 正确：原地修改
df1 = df.set_index('Name')             # ✗ 报错 KeyError: "None of ['Name'] are in the columns"
```

**原因**：第一次 `set_index('Name', inplace=True)` 已经把 `Name` 列**变成索引**，不再是普通列；第二次再对同一个 `df` 调用 `set_index('Name')`，Pandas 在列里找不到 `Name`，于是抛 `KeyError`。

| 写法 | 返回值 | 后续能否复用原 df |
| --- | --- | --- |
| `df.set_index('Name', inplace=True)` | `None` | 是（已原地修改） |
| `df1 = df.set_index('Name')` | 新 DataFrame | 是（原 df 不变） |
| `df = df.set_index('Name', inplace=True)` | **`None`** ✗ | 否（df 被赋值为 None） |

> **结论**：`inplace=True` 与接收返回值**不能同时用**。

---

## 四、索引的常用操作

| 操作 | 写法 |
| --- | --- |
| 重置为默认整数索引 | `df.reset_index()` |
| 查看索引 | `df.index` |
| 修改索引名 | `df.index.name = 'xxx'` |
| 按索引排序 | `df.sort_index()` |
| 判断索引是否唯一 | `df.index.is_unique` |

---

## 五、本章小结与考点

1. 未指定索引时，Pandas 自动生成整数位置索引 `0, 1, 2, ...`；
2. `loc` 按**标签**、`iloc` 按**位置**，二者不可混淆；
3. `loc` 切片是**闭区间**，`iloc` 切片是**半开区间**；
4. `set_index(col, inplace=True)` **原地修改且返回 `None`**；
5. `df = df.set_index(col, inplace=True)` 会把 `df` 变成 `None`，这是最常见的错误；
6. 列一旦变成索引就不再是列，重复 `set_index` 同一列会 `KeyError`；
7. 想恢复用 `reset_index()`。

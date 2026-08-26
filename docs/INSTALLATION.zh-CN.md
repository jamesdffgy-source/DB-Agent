# 安装说明

DBQuill 当前采用源码优先发布，还没有提供经过签名的原生安装器。

## 支持环境

| 组件 | 要求 |
| --- | --- |
| 操作系统 | Windows 10 或 Windows 11，x64 |
| Python | CPython 3.12；CI 选择可用的 3.12 补丁版本 |
| 界面渲染 | Microsoft Edge WebView2 Runtime |
| 网络 | 首次克隆和安装锁定依赖时需要；之后仅在访问已配置模型接口时需要 |
| 数据库 | SQLite、CSV/`.xlsx` 导入，以及 MySQL 8.4/PostgreSQL 17 只读链路和显式启用的受控 DML |

源码流程已经在全新导出目录和 GitHub Windows CI 中验证。其他 Python 版本、Windows on ARM、macOS 和 Linux 暂时不是发布目标。

## 推荐安装

打开 PowerShell 或命令提示符：

```powershell
git clone https://github.com/jamesdffgy-source/DBQuill.git
cd DBQuill
.\scripts\install_and_start.cmd
```

安装脚本会在本机完成三步：

1. 使用 Python 3.12 创建 `.venv`；
2. 强制校验包哈希后安装 `requirements.lock`，并运行 `pip check`；
3. 检查依赖导入、本地目录写入和 WebView2，再启动应用。

安装过程不需要数据库密码或模型密钥。

## 手动安装

```powershell
.\scripts\bootstrap_dev.cmd
.\scripts\doctor.cmd
.\scripts\start_dbquill.cmd
```

如果机器上有多个 Python，可以在当前终端明确指定：

```powershell
$env:DBQUILL_PYTHON = 'C:\Path\To\Python312\python.exe'
.\scripts\bootstrap_dev.cmd
```

## 首次运行

1. 打开“设置”，添加兼容 OpenAI 接口格式的文本模型配置。
2. 通过“添加数据库”接入 SQLite、导入 CSV/`.xlsx`，或创建 MySQL/PostgreSQL 连接。远程连接默认只读；只有确实需要提出数据变更时才选择“受控读写”。
3. 远程受控写入只支持 `INSERT`、带条件的 `UPDATE` 和带条件的 `DELETE`。每条提案先在事务中执行并回滚生成预览，之后仍需按角色显式确认；远程 DDL 不开放。
4. 第一次建议使用合成数据。生成随附演示库：

```powershell
.\scripts\run_python.cmd scripts\create_demo_database.py
```

本地模型配置保存在 `runtime/app/model_profiles.json`。运行数据库、token、上传文件、会话、审计状态和日志也都留在本机，并被 Git 忽略。

## 验证安装

```powershell
.\scripts\doctor.cmd
.\scripts\run_python.cmd scripts\smoke_startup.py
.\scripts\check_project.cmd
```

冒烟测试会在临时端口启动带鉴权的 loopback 服务，访问 `/status` 和桌面页面后关闭进程；不会连接用户数据库或模型服务。

## 常见问题

### 找不到 Python 3.12

安装 CPython 3.12 x64，并在安装时启用 Python Launcher。重新打开终端，运行 `py -3.12 --version` 检查。

### 桌面窗口没有打开

安装或修复 [Microsoft Edge WebView2 Evergreen Runtime](https://developer.microsoft.com/microsoft-edge/webview2/)，然后重新运行 `scripts\doctor.cmd`。本地 bridge 日志位于 `runtime/app/temp/`。

### 依赖下载较慢

安装缓存保存在仓库内的 `.cache/pip`。如果设备开启了代理，请先确认代理可以访问并允许 Python 包下载。

### 离线安装

锁文件固定了版本和哈希，但仓库不会再分发 Python wheel。请在联网的 Windows x64 设备上准备经过批准的 wheelhouse，按锁文件验证每个文件，再随源码一起移入离线环境。

## 卸载

停止桌面应用后删除克隆目录即可。DBQuill 不安装 Windows 服务，也不会把应用凭据写入源码。删除前请备份需要保留的本地数据库或审计导出。

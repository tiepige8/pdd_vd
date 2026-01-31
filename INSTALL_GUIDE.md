# 拼多多视频上传助手（给使用者的安装与使用说明）

本说明适用于 Windows 用户：安装一次 Python 后，双击运行，无需命令行。

---

## 一、准备工作（只做一次）

### 1. 安装 Python（必须）
1) 打开官网：https://www.python.org/downloads/  
2) 下载 **Python 3.10 或 3.11（Windows 版本）**  
3) 安装时务必勾选：`Add Python to PATH`  
4) 安装完成即可

---

## 二、软件使用步骤

### 1. 解压程序
把开发者提供的压缩包解压到任意目录，例如：

`D:\pdd_vd\`

### 2. 确认目录结构
解压后请确认目录中包含以下文件（重点：`BaiduPCS-Go.exe` 和 `run_app.bat`）：

```
pdd_vd
├─ server.py
├─ run_app.bat
├─ BaiduPCS-Go.exe
├─ public
├─ data
└─ video
```

### 3. 双击运行
双击 `run_app.bat`，会自动启动程序并打开浏览器页面。  
运行时请保持黑色窗口不要关闭（关闭即停止服务）。

如果浏览器没自动打开，可手动访问：
`http://127.0.0.1:3000`

---

## 三、常见问题

### 1) 提示找不到 Python
说明未安装 Python，或安装时未勾选 `Add Python to PATH`。  
请重新安装 Python 并勾选该选项。

### 2) 页面打不开
确认 `run_app.bat` 正在运行中。  
浏览器手动输入 `http://127.0.0.1:3000` 再试。

### 3) 想停止程序
关闭黑色窗口即可停止。


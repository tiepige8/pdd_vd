const { app, BrowserWindow, dialog, ipcMain } = require('electron');
const path = require('path');
const fs = require('fs');
const { spawn } = require('child_process');

let pyProcess;
let baiduLoginProcess;

function resolvePython() {
  // packaged path: resources/python-runtime/bin/python or Scripts/python.exe
  const base = path.join(process.resourcesPath, 'python-runtime');
  const candidates = [
    path.join(base, 'bin', 'python3'),
    path.join(base, 'bin', 'python'),
    path.join(base, 'Scripts', 'python.exe'),
  ];
  for (const c of candidates) {
    if (fs.existsSync(c)) return c;
  }
  // dev fallback
  return process.platform === 'win32' ? 'python' : 'python3';
}

function startPython() {
  const python = resolvePython();
  const script = path.join(__dirname, '..', 'server.py');
  pyProcess = spawn(python, [script], {
    cwd: path.join(__dirname, '..'),
    env: { ...process.env, PORT: process.env.PORT || '3000' },
    stdio: 'inherit',
  });
  pyProcess.on('error', (err) => {
    dialog.showErrorBox('Python 启动失败', err.message);
  });
}

function createWindow() {
  const win = new BrowserWindow({
    width: 1100,
    height: 720,
    webPreferences: {
      contextIsolation: true,
      preload: path.join(__dirname, 'preload.js'),
    },
  });
  win.loadURL('http://127.0.0.1:3000');
}

function resolveBaiduCli(customPath) {
  const candidates = [];
  const addCandidate = (p) => {
    if (p && fs.existsSync(p)) {
      candidates.push(p);
    }
  };
  if (customPath) {
    const raw = customPath.trim();
    addCandidate(path.isAbsolute(raw) ? raw : path.join(__dirname, '..', raw));
  }
  addCandidate(path.join(__dirname, '..', 'BaiduPCS-Go.exe'));
  addCandidate(path.join(process.resourcesPath, 'BaiduPCS-Go.exe'));
  addCandidate(path.join(__dirname, '..', 'BaiduPCS-Go'));
  addCandidate(path.join(process.resourcesPath, 'BaiduPCS-Go'));
  const exeName = process.platform === 'win32' ? 'BaiduPCS-Go.exe' : 'BaiduPCS-Go';
  addCandidate(exeName);
  return candidates.find(Boolean) || '';
}

function stopBaiduLogin() {
  if (baiduLoginProcess) {
    baiduLoginProcess.kill('SIGTERM');
    baiduLoginProcess = null;
  }
}

function loginBaiduWithBduss(customPath, bduss, stoken, sender) {
  const cli = resolveBaiduCli(customPath);
  if (!cli) {
    sender.send('baidu-login-event', { type: 'error', message: '未找到 BaiduPCS-Go，可在配置里填写路径' });
    return;
  }
  const safeBduss = (bduss || '').trim();
  const safeStoken = (stoken || '').trim();
  if (!safeBduss) {
    sender.send('baidu-login-event', { type: 'error', message: '请先填写 BDUSS' });
    return;
  }
  if (baiduLoginProcess) {
    baiduLoginProcess.kill('SIGTERM');
  }
  sender.send('baidu-login-event', { type: 'start', message: '正在提交 BDUSS 登录...' });
  const args = ['login', `-bduss=${safeBduss}`];
  if (safeStoken) {
    args.push(`-stoken=${safeStoken}`);
  }
  baiduLoginProcess = spawn(cli, args, {
    cwd: path.join(__dirname, '..'),
  });
  baiduLoginProcess.stdout.on('data', (chunk) => {
    sender.send('baidu-login-event', { type: 'line', message: chunk.toString() });
  });
  baiduLoginProcess.stderr.on('data', (chunk) => {
    sender.send('baidu-login-event', { type: 'line', message: chunk.toString() });
  });
  baiduLoginProcess.on('close', (code) => {
    sender.send('baidu-login-event', { type: 'end', code });
    baiduLoginProcess = null;
  });
  baiduLoginProcess.on('error', (err) => {
    sender.send('baidu-login-event', { type: 'error', message: err.message });
  });
}

app.whenReady().then(() => {
  startPython();
  createWindow();
  ipcMain.on('baidu-login-bduss', (event, payload) => {
    loginBaiduWithBduss(payload?.cliPath, payload?.bduss, payload?.stoken, event.sender);
  });
  ipcMain.on('baidu-login-stop', () => {
    stopBaiduLogin();
  });
  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow();
    }
  });
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

app.on('before-quit', () => {
  if (pyProcess) {
    pyProcess.kill('SIGTERM');
  }
  stopBaiduLogin();
});

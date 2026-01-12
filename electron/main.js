const { app, BrowserWindow, dialog } = require('electron');
const path = require('path');
const fs = require('fs');
const { spawn } = require('child_process');

let pyProcess;

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
    },
  });
  win.loadURL('http://127.0.0.1:3000');
}

app.whenReady().then(() => {
  startPython();
  createWindow();
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
});

const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('electronAPI', {
  startBaiduLogin: (cliPath) => {
    ipcRenderer.send('baidu-login-start', { cliPath: cliPath || '' });
  },
  loginBaiduWithBduss: (cliPath, bduss, stoken) => {
    ipcRenderer.send('baidu-login-bduss', {
      cliPath: cliPath || '',
      bduss: bduss || '',
      stoken: stoken || '',
    });
  },
  stopBaiduLogin: () => {
    ipcRenderer.send('baidu-login-stop');
  },
  onBaiduLoginEvent: (callback) => {
    const listener = (_, data) => callback(data);
    ipcRenderer.on('baidu-login-event', listener);
    return () => ipcRenderer.removeListener('baidu-login-event', listener);
  },
});

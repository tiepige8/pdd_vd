# 拼多多商家后台视频助手 - 授权联调

本仓库提供一个极简的本地站点，用来配置拼多多开放平台的应用信息，发起商家授权并自动换取 access_token/refresh_token，为后续视频上传接口调试做准备。

## 运行

1. 确认本机有 Python 3（建议 3.9+）。无需安装任何依赖。
2. 在仓库根目录执行：

```bash
PORT=3000 python3 server.py
```

启动后打开浏览器访问 `http://localhost:3000`。

## 使用步骤

1. 在页面填写并保存 `client_id`、`client_secret`，以及回调地址 `redirect_uri`（默认 `http://localhost:3000/auth/callback`）。
2. 点击“生成授权链接”，用商家账号登录并完成授权。
3. 回调到 `/auth/callback` 后，服务端会向 `https://open-api.pinduoduo.com/oauth/token` 交换令牌，结果保存在 `data/tokens.json`，页面右侧也会展示。
4. 服务端会自动使用 `refresh_token` 在到期前 5 分钟刷新，无需重复授权（刷新时间写入 `nextRefreshAt`）。若刷新失败会重试并将下次刷新时间提前。
5. 前端首页左侧展示 token 到期时间与下次刷新时间，右侧为配置区；日志区域可查看操作轨迹。
6. 已内置定时上传调度（默认 QJD 店铺，09:00 开始，每 5 分钟一条，每日 50 条），会轮询 `video/店铺_YYYYMMDD` 目录。可在页面右侧调整时间、间隔、每日上限和视频根目录，支持一键“立即扫描上传”。
7. 后续调用开放平台接口时，按官方签名规范带上最新的 `access_token`；如需手动查看或替换，修改 `data/tokens.json` 即可。

## Electron 桌面版（mac 开发 / 打包 Windows）

1. 准备 Python 运行时：在项目根目录创建 `python-runtime`，内容可以是预先准备好的可移植 Python（Windows 可执行 + `Lib` 等）。开发阶段可直接用本机 Python。
2. 安装 Electron 依赖：
   ```bash
   cd electron
   npm install
   ```
3. 开发调试（mac 上窗口加载本地服务）：先在根目录 `python3 server.py`，再 `npm run dev`。
4. 打包 Windows 安装包（mac 交叉打包）：安装 `brew install --cask wine-stable` 和 `brew install mono`，然后：
   ```bash
   cd electron
   npm run dist -- --win
   ```
   产物位于 `dist/`，默认 NSIS 安装包。若要内置 Python，请将可移植 Python 放入 `python-runtime/`，构建时会打进 resources。

## 视频目录准备

- 已在根目录创建示例目录：`video/QJD_YYYYMMDD`（日期为今日），每天的上传文件可放在对应日期店铺目录下，后续批量上传脚本可遍历此目录。
## 批量上传思路（百度网盘 -> 本地 -> 拼多多）

1. 网盘同步到本地：使用百度网盘官方 SDK 或社区工具 BaiduPCS-Go，将 `pdd_vd/YYYYMMDD/店铺名` 目录每日同步到本地固定路径。
2. 本地批处理上传：遍历同步目录，按店铺/日期读取视频，调用拼多多视频分片上传接口（待接入）并记日志。
3. 定时：可用 cron/Windows 计划任务，早上先同步网盘，再调用上传脚本；上传过程中的进度/错误可写入本页日志区域。

可在 `data/config.json` 和 `data/tokens.json` 中查看/替换保存的配置与令牌。若授权地址与默认的商家后台地址不同，可在页面中调整“授权入口”后再生成链接。

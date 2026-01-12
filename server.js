const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const STATIC_DIR = path.join(__dirname, 'public');
const DATA_DIR = path.join(__dirname, 'data');
const CONFIG_PATH = path.join(DATA_DIR, 'config.json');
const TOKEN_PATH = path.join(DATA_DIR, 'tokens.json');

ensureDir(DATA_DIR);

const server = http.createServer(async (req, res) => {
  try {
    const parsedUrl = new URL(req.url, `http://${req.headers.host}`);
    const { pathname, searchParams } = parsedUrl;

    // Simple API router
    if (pathname === '/api/config' && req.method === 'GET') {
      return sendJson(res, 200, loadConfig());
    }

    if (pathname === '/api/config' && req.method === 'POST') {
      const body = await readBody(req);
      const next = {
        clientId: body.clientId || '',
        clientSecret: body.clientSecret || '',
        redirectUri: body.redirectUri || '',
        authBase:
          body.authBase || 'https://mms.pinduoduo.com/open.html', // default merchant auth entry
      };
      saveJson(CONFIG_PATH, next);
      return sendJson(res, 200, { ok: true, config: next });
    }

    if (pathname === '/api/auth/url' && req.method === 'GET') {
      const config = loadConfig();
      if (!config.clientId || !config.redirectUri) {
        return sendJson(res, 400, {
          error: '缺少 clientId 或 redirectUri，请先保存配置。',
        });
      }
      const state = crypto.randomBytes(12).toString('hex');
      const authUrl = buildAuthUrl(config, state);
      // persist last state for quick validation
      const tokenStore = loadTokens();
      tokenStore.lastAuthState = state;
      saveJson(TOKEN_PATH, tokenStore);
      return sendJson(res, 200, { url: authUrl, state });
    }

    if (pathname === '/api/tokens' && req.method === 'GET') {
      return sendJson(res, 200, loadTokens());
    }

    if (pathname === '/auth/callback' && req.method === 'GET') {
      const code = searchParams.get('code');
      const state = searchParams.get('state');
      if (!code) {
        return sendHtml(
          res,
          400,
          renderMessage('缺少授权 code，无法交换访问令牌。'),
        );
      }
      const config = loadConfig();
      if (!config.clientId || !config.clientSecret || !config.redirectUri) {
        return sendHtml(
          res,
          400,
          renderMessage('未找到有效配置，请先在首页填写并保存应用信息。'),
        );
      }
      const tokenStore = loadTokens();
      if (tokenStore.lastAuthState && state && tokenStore.lastAuthState !== state) {
        return sendHtml(
          res,
          400,
          renderMessage('state 校验失败，请重新发起授权。'),
        );
      }

      try {
        const tokenResponse = await exchangeToken({
          clientId: config.clientId,
          clientSecret: config.clientSecret,
          redirectUri: config.redirectUri,
          code,
        });
        tokenStore.lastAuth = {
          ...tokenResponse,
          receivedAt: new Date().toISOString(),
          state,
        };
        saveJson(TOKEN_PATH, tokenStore);
        return sendHtml(
          res,
          200,
          renderMessage('授权成功，已经拿到访问令牌。', tokenResponse),
        );
      } catch (err) {
        console.error('token exchange failed', err);
        return sendHtml(
          res,
          502,
          renderMessage(`换取访问令牌失败：${err.message}`),
        );
      }
    }

    // Static file server
    if (req.method === 'GET') {
      return serveStatic(res, pathname);
    }

    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Not found');
  } catch (err) {
    console.error('Unhandled server error', err);
    res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Internal Server Error');
  }
});

server.listen(PORT, () => {
  console.log(`PDD helper running at http://localhost:${PORT}`);
});

function ensureDir(dir) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

function loadConfig() {
  return readJson(CONFIG_PATH, {
    clientId: '',
    clientSecret: '',
    redirectUri: '',
    authBase: 'https://mms.pinduoduo.com/open.html',
  });
}

function loadTokens() {
  return readJson(TOKEN_PATH, {});
}

function readJson(file, fallback) {
  try {
    const raw = fs.readFileSync(file, 'utf-8');
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

function saveJson(file, data) {
  fs.writeFileSync(file, JSON.stringify(data, null, 2), 'utf-8');
}

function sendJson(res, status, payload) {
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
  });
  res.end(JSON.stringify(payload));
}

function sendHtml(res, status, html) {
  res.writeHead(status, {
    'Content-Type': 'text/html; charset=utf-8',
    'Cache-Control': 'no-store',
  });
  res.end(html);
}

function renderMessage(message, data) {
  const body = data
    ? `<pre>${escapeHtml(JSON.stringify(data, null, 2))}</pre>`
    : '';
  return `<!doctype html>
  <html lang="zh-CN">
    <head>
      <meta charset="utf-8" />
      <title>拼多多授权回调</title>
      <style>
        body { font-family: -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; padding: 32px; line-height: 1.6; }
        .box { max-width: 720px; margin: 0 auto; padding: 24px; border-radius: 12px; background: #f7f7fa; }
        pre { background: #111827; color: #e5e7eb; padding: 16px; border-radius: 8px; overflow: auto; }
        a { color: #2563eb; text-decoration: none; }
      </style>
    </head>
    <body>
      <div class="box">
        <h2>授权结果</h2>
        <p>${escapeHtml(message)}</p>
        ${body}
        <p><a href="/">返回配置页</a></p>
      </div>
    </body>
  </html>`;
}

function escapeHtml(text) {
  return text.replace(/[&<>"']/g, (c) => {
    switch (c) {
      case '&':
        return '&amp;';
      case '<':
        return '&lt;';
      case '>':
        return '&gt;';
      case '"':
        return '&quot;';
      case "'":
        return '&#39;';
      default:
        return c;
    }
  });
}

function serveStatic(res, pathname) {
  const safePath = path.normalize(pathname).replace(/^\/+/, '');
  const target = safePath ? safePath : 'index.html';
  const filePath = path.join(STATIC_DIR, target);
  if (!filePath.startsWith(STATIC_DIR)) {
    res.writeHead(403, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Forbidden');
    return;
  }

  fs.readFile(filePath, (err, content) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Not found');
      return;
    }
    res.writeHead(200, {
      'Content-Type': mimeType(filePath),
      'Cache-Control': 'no-cache',
    });
    res.end(content);
  });
}

function mimeType(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  const map = {
    '.html': 'text/html; charset=utf-8',
    '.js': 'application/javascript; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
  };
  return map[ext] || 'text/plain; charset=utf-8';
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', (chunk) => {
      body += chunk;
      if (body.length > 1e6) {
        req.connection.destroy();
        reject(new Error('Request body too large'));
      }
    });
    req.on('end', () => {
      if (!body) {
        return resolve({});
      }
      try {
        resolve(JSON.parse(body));
      } catch (err) {
        reject(err);
      }
    });
    req.on('error', reject);
  });
}

function buildAuthUrl(config, state) {
  const url = new URL(config.authBase || 'https://mms.pinduoduo.com/open.html');
  url.searchParams.set('response_type', 'code');
  url.searchParams.set('client_id', config.clientId);
  url.searchParams.set('redirect_uri', config.redirectUri);
  url.searchParams.set('state', state);
  // view=web makes the page friendlier in desktop browsers
  url.searchParams.set('view', 'web');
  return url.toString();
}

function exchangeToken({ clientId, clientSecret, redirectUri, code }) {
  const params = new URLSearchParams({
    client_id: clientId,
    client_secret: clientSecret,
    code,
    grant_type: 'authorization_code',
    redirect_uri: redirectUri,
  });
  const options = {
    method: 'POST',
    hostname: 'open-api.pinduoduo.com',
    path: '/oauth/token',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Content-Length': Buffer.byteLength(params.toString()),
    },
  };

  return new Promise((resolve, reject) => {
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => (data += chunk));
      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          if (parsed.error) {
            reject(new Error(parsed.error_description || parsed.error));
            return;
          }
          resolve(parsed);
        } catch (err) {
          reject(err);
        }
      });
    });
    req.on('error', reject);
    req.write(params.toString());
    req.end();
  });
}

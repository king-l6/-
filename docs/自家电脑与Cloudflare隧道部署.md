# 自家电脑 + Cloudflare Tunnel 部署

用本机跑服务，再通过 Cloudflare 的免费隧道把网站暴露到公网，手机/其他设备通过 HTTPS 链接即可访问。**不绑卡、完全免费、部署简单**。

---

## 特点

- **不绑卡**：无需注册云平台、无需信用卡
- **免费**：Cloudflare Tunnel 免费，本机跑项目无额外费用
- **数据在本地**：策略配置、回测结果、股票缓存等均在项目目录（如 `cache/`、`results/`），可随时备份
- **条件**：用的时候电脑要开着，并保持两个终端在运行

---

## 前置要求

- 本机已安装 **Python 3**、**Node.js**（建议 18+）和 **npm**
- 已安装 **cloudflared**：
  - **Mac**：`brew install cloudflared`
  - **Windows**：到 [cloudflared releases](https://github.com/cloudflare/cloudflared/releases) 下载并加入系统 Path

---

## 部署步骤

### 1. 安装依赖并构建

在**项目根目录**打开终端，执行：

```bash
./run.sh
```

首次运行会：创建 Python 虚拟环境、安装 Python 依赖、安装前端依赖、构建前端、并启动服务。  
**先按 `Ctrl+C` 停止**，然后进行下一步（若你已能访问 http://localhost:8086，可跳过“启动本机服务”的再次启动说明）。

若希望分步执行（不用 `run.sh`），可执行：

```bash
python3 -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
cd frontend && npm install && npm run build && cd ..
```

### 2. 启动本机服务

在**项目根目录**执行：

```bash
./run.sh
```

或（已激活虚拟环境且前端已构建时）：

```bash
source venv/bin/activate
python3 app.py
```

看到服务在 **8086** 端口启动后，在浏览器打开 [http://localhost:8086](http://localhost:8086)，确认能访问策略回测界面。

### 3. 暴露到公网

**再开一个终端**（不要关掉运行 `./run.sh` 或 `python3 app.py` 的那个），执行：

```bash
cloudflared tunnel --url http://localhost:8086
```

终端里会输出一行类似：

```text
https://xxxx-xx-xx-xx-xx.trycloudflare.com
```

用手机或其他设备的浏览器打开这个 **https** 链接，即可在外网访问你的 A 股策略回测系统。

### 4. 访问与安全

- 本系统**无登录功能**，任何人拿到该 HTTPS 链接均可访问。
- 若需限制访问，可查阅 [Cloudflare Access](https://developers.cloudflare.com/cloudflare-one/policies/access/) 为隧道配置认证。

---

## 下次使用

1. **终端一**（项目根目录）：`./run.sh`（或 `source venv/bin/activate && python3 app.py`）
2. **终端二**：`cloudflared tunnel --url http://localhost:8086`，用终端里给出的新链接访问（每次运行链接可能变化）

无需重新安装依赖或构建前端，除非你改过代码或依赖。

---

## 数据与备份

- 策略配置、回测结果、股票数据缓存等均在你这台电脑的项目目录下，例如：
  - **cache/**：股票数据缓存
  - **results/**：批量回测结果（如 `*_结果.jsonl`）
  - **common_strategies.json**：常用策略配置
- 可定期复制上述目录或整份项目做备份。
- 关闭本机服务或关机后，外网链接会失效，下次按「下次使用」再开即可。

---

## 常见问题

**Q：链接每次都会变吗？**  
A：使用 `cloudflared tunnel --url ...` 这种快速隧道时，每次运行会生成新链接。若需要固定域名，可参考 [Cloudflare Tunnel 文档](https://developers.cloudflare.com/cloudflare-one/connections/connect-apps/) 配置命名隧道。

**Q：关掉终端后别人还能访问吗？**  
A：不能。关掉「终端一」服务就停了；关掉「终端二」隧道就断了，外网无法访问。

**Q：本机用 pnpm 可以吗？**  
A：可以。若前端用 pnpm，在 `frontend/` 目录下用 `pnpm install`、`pnpm run build` 替代 `npm install`、`npm run build` 即可；本机服务仍用 `./run.sh` 或 `python3 app.py` 启动。

**Q：端口能改吗？**  
A：可以。通过环境变量指定端口，例如：`FLASK_PORT=9090 python3 app.py`，隧道命令改为 `cloudflared tunnel --url http://localhost:9090`。

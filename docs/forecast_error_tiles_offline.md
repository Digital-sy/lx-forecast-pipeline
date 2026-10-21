# 销量预测误差方块图：离线构建与验收规范

## 范围与安全

这是已验收预测结果的只读展示适配器，不是模型实验。训练、推理、数据库查询、生产修改均为零。不得调用历史训练或生产流水线入口。已有归档只读；构建与验收输出目录必须不存在。业务 HTML、JSON、CSV、截图、模型和凭据不纳入 Git。

交付为一个自包含 HTML，内嵌数据、CSS、JavaScript；原生 SVG，无 CDN、网络字体或前端框架依赖。双击 `file://` 使用，不需要本地 HTTP 服务。页面业务文本使用 DOM textContent，JSON 转义 HTML 敏感字符；CSV 文本防公式注入。HTML 包含业务数据，仅限受控本地使用。

## 来源合同

按现有 manifest 和验收回执读取，不根据报告中的舍入数反推。月度回放仅取 `monthly_core`，排除另一套固定样本；YEARAGO 仅使用 reviewed 结果。GATE 另核验最终复核回执。BLEND50 仅在实际已验收输出存在时纳入，不现场生成半量候选。日期、真值与复合键不一致则隔离或停止，不取最后一条覆盖。

复合键为店铺、SPU、签发日期、窗口，并核对目标起止日期及真值版本。重复 DIRECT 以原月度回放为权威源；其他目录的复制件只用于一致性审计。微小浮点往返差异记录最大差值，不替换原始值。不可恢复的历史元数据只作当前描述性标签，不声称点时可用。

## 图形和评分合同

- 前/后30天分别按实际销量≥10展示，累计60天按实际销量≥20展示；未知和未成熟单列，不按低销量删掉。
- 面积为 `k * actual`，边长为 `sqrt(k * actual)`。每个窗口只有一个 k，跨月份、跨模型相同。按实际量降序、店铺及 SPU 稳定装箱。筛选隐藏对象但不重新排版或放大。
- 缺预测保留相同面积及位置，以灰斜纹显示。负预测/非数值/来源冲突单列，不修正为零。
- APE=`abs(prediction-actual)/actual`，未舍入判定。色标固定于 0/20/40/60/80/100%；100%红，严格超过100%黑。箭头表达高低估。
- WAPE 只在有效预测和实销的共同范围计算，披露评分数量与实销分母。完整展示命中率分母包含缺预测；同样本比较另报排除规模，不删除主图灰块。
- 跨期汇总称评估窗口实销权重，不称去重经营销量。可视化过滤成绩与未过滤历史验收分开。
- 搜索只高亮，页面中其余方块可淡化；SVG 导出取消搜索淡化，保留固定误差颜色及完整当前筛选对象。

## 本地复现

在独立的既有 Python 环境中使用 pandas、numpy、Parquet 依赖和时区库；不安装到生产环境。`WORKSPACE` 是获准本地归档根目录，`NEW_OUTPUT` 是其下新的独立目录。必须通过清单版本和哈希校验，不能任意放入同名文件。

```text
python -B -m unittest tests.test_forecast_error_tiles -v
node --check assets/forecast_error_tiles/app.js
python -B scripts/build_forecast_error_tiles.py --workspace WORKSPACE --output NEW_OUTPUT
python -B scripts/validate_forecast_error_tiles.py --output NEW_OUTPUT
node scripts/test_forecast_error_tiles_browser.mjs NEW_OUTPUT LOCAL_CDP_PORT
```

数值验收独立遍历原 Parquet 预测与真值，复核原适用范围指标、几何面积比例、重叠、边界及来源哈希。浏览器验收用另一条 Node 数值路径对照页面汇总，不把 HTML 构建成功当作点击验收。

## 隔离浏览器验收

仅启动临时独立 Chrome 用户目录，不能复用带登录态的用户配置。Windows 后台启动须隐藏窗口；启动参数为 `--headless=new --remote-debugging-port=0 --remote-debugging-address=127.0.0.1 --user-data-dir=NEW_PROFILE --no-first-run --disable-default-apps --disable-background-networking --no-default-browser-check about:blank`。端口从该独立目录的 DevToolsActivePort 读取，不能猜测或对外开放。

测试器先读实际 `/json/protocol` 验证命令及参数，仅允许操作空白页或精确交付文件目标。CDP 控制通道是本机回环；被测页面是 file:// 且设置离线。无网络依赖、真实鼠标/键盘、筛选、联动、导出、窄屏及截图分别留证。

重复验收用新的第三个参数，例如 `browser_tests_02`，不覆盖失败日志。失败时先修复已证实的问题，再记录新运行；不能重写失败回执。实际自动化点击与人工点击分别表述。

## 提交清单与停止条件

只提交 assets 模板、构建/数值/浏览器验收脚本、合成测试及本规范。生成器不自动 git add。业务报告和截图仅保留输出目录。数值、图形及实际浏览器各自给出 PASS/PARTIAL/BLOCKED，不改变原预测研究结论。交付后停止，不触发模型实验、影子运行、生产或 BI 变更。

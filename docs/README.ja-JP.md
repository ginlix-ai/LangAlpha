<p align="center">
  <img src="../web/public/logo_words.png" alt="LangAlpha" height="110" />
</p>

<h3 align="center">Agent トレーディングのための harness。</h3>

<p align="center">
  市場をリサーチし、投資仮説を組み立て、<br>
  あなたが設定した権限の範囲内で、あなた自身の証券口座で取引するオープンソースの AI agent。
</p>

<p align="center">
  <a href="https://langalpha.ai"><strong>LangAlpha を試す ↗</strong></a> ·
  <a href="#使い始める"><strong>使い始める</strong></a> ·
  <a href="#agent-トレーディング"><strong>Agent トレーディング</strong></a> ·
  <a href="#harness-設計"><strong>Harness 設計</strong></a> ·
  <a href="#フルスタックアーキテクチャ"><strong>フルスタックアーキテクチャ</strong></a> ·
  <a href="#セキュリティ"><strong>セキュリティ</strong></a>
  <br />
  <a href="../README.md">English</a> · <a href="README.zh-CN.md">简体中文</a> · 日本語
</p>

<p align="center">
  <a href="https://github.com/ginlix-ai/langalpha/stargazers"><img src="https://img.shields.io/github/stars/ginlix-ai/langalpha?style=flat-square" alt="GitHub スター数" /></a>
  <img src="https://img.shields.io/badge/license-Apache%202.0-green?style=flat-square" alt="ライセンス：Apache 2.0" />
  <img src="https://img.shields.io/badge/python-3.13+-blue?style=flat-square" alt="Python 3.13+" />
  <a href="https://github.com/langchain-ai/langchain"><img src="https://img.shields.io/badge/LangChain-1c3c3c?style=flat-square&logo=langchain&logoColor=white" alt="LangChain" /></a>
  <a href="https://github.com/ginlix-ai/langalpha/releases"><img src="https://img.shields.io/badge/desktop-macOS%20%7C%20Windows%20%7C%20Linux-555?style=flat-square" alt="デスクトップアプリ" /></a>
</p>

<p align="center">
  <img src="https://raw.githubusercontent.com/ginlix-ai/LangAlpha/main/docs/images/hero-cbrs-research-dashboard.webp" alt="demo-cbrs workspace：左では agent が CBRS の株価を確認し、開示資料を取得し、2 人のアナリストを動かしている。右はそのリサーチから作った CBRS のダッシュボードで、IPO 以降の株価をイベントマーカー付きで表示し、選択したイベントのメモと出典を開いている" width="900" />
</p>

## LangAlpha を選ぶ理由

AI 金融ツールの多くは、質問に答えるためのものです。私たちは、投資リサーチはベイズ的だと考えています。まず仮説と、何が起きればそれが誤りだと分かるかを書き出します。その後は決算発表、開示資料、値動きのたびに確信度が上下し、それに合わせてポジションも動きます。このループは数週間から数か月続き、1 回の prompt では捉えきれません。

コーディング agent が実用的になったのは、コードのために作られた harness を得てからです。永続する codebase があり、各 commit が前の commit の上に積み上がり、さらにモデルの周りにツール、memory、runtime がそろっています。LangAlpha はその harness を市場に持ち込みます。vibe coding から vibe investing へ。どのモデルでも動くので、モデルが良くなるたびに LangAlpha も良くなります。設計の軸となる考え方は 2 つです。

- **トレードは tool call ではなく、ループです。** 仮説は workspace に置かれ、リサーチ、ポジションサイジング、注文、その後の監視のそれぞれが、新しい証拠を仮説に返します。
- **自律には、あなたが決める境界が必要です。** agent はあなたが与えた権限の範囲内で動き、すべての注文は統制された 1 本の経路を通ります。

<p align="center">
  <img src="https://raw.githubusercontent.com/ginlix-ai/LangAlpha/main/docs/images/diagrams/loop.webp" alt="トレードはループである：思いつきがリサーチと workspace に保存された仮説につながり、サイジングとリスク、注文、監視へと進み、監視が新しい証拠を仮説に返して確信度を動かす" width="880" />
</p>

## 機能ハイライト

ここでの例の多くは、Cerebras（CBRS）を扱う 1 つの workspace から取っています。最初の質問から Slack に送られた初回カバレッジレポートまで、実際の動きは[この共有会話](https://app.langalpha.ai/s/z7rvgK3P9vZN)で確認できます。

### 🔎 Agent チームでリサーチする

質問すると、LangAlpha はそれを並列のアナリストに振り分けます。アナリストは開示資料を読み、株価、オプション、マクロデータを取得し、コードで数字を計算します。実行中でもいつでも追加のメッセージを送って、方向を変えられます。CBRS では、Cerebras が何を売り、どれだけの速さで成長し、誰が支払っているのかをまとめた [HTML の入門資料](https://app.langalpha.ai/a/lep06ly8Qjfa)が最終的な成果になりました。

<p align="center">
  <img src="https://raw.githubusercontent.com/ginlix-ai/LangAlpha/main/docs/images/chat-cbrs-research-subagents-primer.webp" alt="CBRS についての質問を 4 つの subagent に振り分けた様子。subagent はサイドバーで lead agent の下に並び、隣には Cerebras に誰が支払っているかを扱う入門資料のセクションがある" width="800" />
  <br />
  <sub><b>Subagents</b>：lead agent の下で 4 人のアナリストが質問に取り組み、各カードにはツールと token の使用数が表示されます。<code>/html-report</code> を 1 回実行すると、その調査結果が右の入門資料になります</sub>
</p>

### 🗂️ アイデアはすべて Workspace に残す

仮説、セクター、ポートフォリオごとに 1 つの workspace を使います。ファイル、チャット、agent 自身のメモは翌日もそのまま残っているので、毎回ゼロから始めるのではなく、前回のセッションの上に積み上げられます。

> *「この workspace でここまでに分かったことをもとに、CBRS のインタラクティブなダッシュボードを作って。主要なイベント付きの株価、売上成長、顧客集中度、そして OpenAI にどれだけ依存しているか。あちこちクリックして見られるようにしたい。」*

新しい thread が、それまでの thread が workspace に残したリサーチとファイルをもとにダッシュボードを作ります。完成したのは[実際に操作できるライブダッシュボード](https://app.langalpha.ai/a/DB8NBmVeudB1)です。

<table align="center">
  <tr>
    <td width="50%" align="center" valign="top"><img src="https://raw.githubusercontent.com/ginlix-ai/LangAlpha/main/docs/images/chat-cbrs-workspace-data-files.webp" alt="リサーチ途中の demo-cbrs workspace：agent が xlsx と dcf-model の skill を読み込み、市場データの入力値を得るコードを実行している。隣では、タスクごとに 1 フォルダのファイルツリーから、overview タスクの data フォルダにある revenue_history.json を開いている" /><br /><sub><b>Workspace Files</b>：大量のデータは agent の context ではなくファイルに保存されます。株価、開示資料、モデルの入力値が、ファイルツリー内の各タスクの data/ フォルダに入ります</sub></td>
    <td width="50%" align="center" valign="top"><img src="https://raw.githubusercontent.com/ginlix-ai/LangAlpha/main/docs/images/chat-cbrs-dashboard-openai-exposure.webp" alt="ダッシュボードの thread：作成の依頼と、別の thread のイベントファイルを取り込む追加の依頼。隣には OpenAI への依存度を示すライブの CBRS ダッシュボードがあり、エクスポージャー台帳と受注残のストレステストが表示されている" /><br /><sub><b>動くアプリ</b>：agent はインタラクティブなダッシュボードを作ります。棒、マーカー、行をクリックすると出典が表示され、OpenAI の受注残ストレステストはドラッグで操作でき、別の thread のファイルを @ で指定して取り込むこともできます</sub></td>
  </tr>
</table>

### ⏰ 見張りを続ける Agent

寄り付き前のブリーフを予約したり、株価がある価格を超えたときや 1 日で設定した割合だけ動いたときに agent を起動したりできます。結果は 1 つのフィードに届き、Slack、Discord、iMessage を接続していればそちらにも届きます。

> *「CBRS を見張っておいて。毎週月曜にモデルの前提を確認して、Q3 決算の後にモデルを更新して、株価が強気シナリオの価値を上抜けるか $95 を割り込んだら教えて。」*

agent は workspace に 4 つの automation を設定します。毎週の前提チェック、Q3 決算の翌朝に 1 回だけ実行するモデル更新、そして 2 つの価格トリガーです。価格トリガーは、DCF モデルから出した強気シナリオの価値と、レーティングを変える水準である $95 に置かれます。各実行は workspace のモデルとメモから始まり、結果を Slack に報告します。

<table align="center">
  <tr>
    <td width="50%" align="center" valign="top"><img src="https://raw.githubusercontent.com/ginlix-ai/LangAlpha/main/docs/images/automations-cbrs-schedules-price-watches.webp" alt="毎週の CBRS チェック、決算後のモデル更新、2 つの価格トリガーが並ぶ Automations ページ" /><br /><sub><b>Automations</b>：すべてのスケジュールと価格監視を 1 ページで管理できます。各監視が発火までどれだけ離れているか、結果がどこに届くかも表示されます</sub></td>
    <td width="50%" align="center" valign="top"><img src="https://raw.githubusercontent.com/ginlix-ai/LangAlpha/main/docs/images/automations-cbrs-new-price-move.webp" alt="CBRS が前日終値から 15% 上昇したときに agent を起動する新しい automation。指示内容と Slack DM への配信先が設定されている" /><br /><sub><b>値動きで起動</b>：前日終値または当日始値からの % 変動で agent を起動します。当日の変動があなたの閾値と並べて表示されます</sub></td>
  </tr>
</table>

### 📑 答えだけでなく成果物を受け取る

数式が生きた Excel モデル、PDF と HTML のレポート、スライド資料、インタラクティブなダッシュボード。組み込みの skill は、DCF、類似企業比較、決算プレビュー、初回カバレッジ、モーニングノート、トレードピッチをカバーします。

> *「CBRS について、セルサイドの初回カバレッジのようにまとめて。レーティング、目標株価、投資仮説、市場が織り込んでいること、シナリオ、ピア、主なリスク、チャートを入れて。すでにあるリサーチとモデルを使って。」*

初回カバレッジの skill は、thread の前半で作ったリサーチとリバース DCF のワークブックを土台に、10 ページのレポートを書きます。目標株価 $130 の Sell 判断、シナリオ、ピア、リスク、7 つのチャートを含み、PDF と [HTML 版](https://app.langalpha.ai/a/eedVW2kllALZ)で出力されます。スプレッドシートパネルで範囲を選んで agent に送り返せば、その部分について質問できます。Slack を接続していれば、すべて送るよう頼むだけで全ファイルが DM に届き、その場で開けます。

<table align="center">
  <tr>
    <td width="50%" align="center" valign="top"><img src="https://raw.githubusercontent.com/ginlix-ai/LangAlpha/main/docs/images/chat-cbrs-initiation-report.webp" alt="チャットの横に開いた 10 ページの CBRS 初回カバレッジレポート。売上と顧客集中度のチャートがある" /><br /><sub><b>ファイルパネル</b>：HTML レポートは、レンダリング表示でもソースでも読めます。根拠となった開示資料やプレスリリースは本文中に引用されます</sub></td>
    <td width="50%" align="center" valign="top"><img src="https://raw.githubusercontent.com/ginlix-ai/LangAlpha/main/docs/images/chat-cbrs-dcf-comps-workbook.webp" alt="スプレッドシートパネルに表示した CBRS のリバース DCF ワークブック。選択したセルをチャットに追加できる状態になっている" /><br /><sub><b>スプレッドシート</b>：DCF、WACC、Comps、Checks の各シートに生きた数式が入った本物のワークブックです。隣には、類似企業比較のデータを集めたリサーチの実行が並びます</sub></td>
  </tr>
</table>

<table align="center">
  <tr>
    <td width="50%" align="center" valign="top"><img src="https://raw.githubusercontent.com/ginlix-ai/LangAlpha/main/docs/images/chat-cbrs-initiation-peers.webp" alt="初回カバレッジの依頼と、PDF ビューアで開いた CBRS レポート。強気シナリオをピアのバリュエーションと利益率で検証するページを表示している" /><br /><sub><b>PDF ビューア</b>：完成した PDF をチャットの横でめくったり拡大したりできます。ここでは、強気シナリオをピアのマルチプルと利益率で検証するページを表示しています</sub></td>
    <td width="50%" align="center" valign="top"><img src="https://raw.githubusercontent.com/ginlix-ai/LangAlpha/main/docs/images/slack-cbrs-deliverables-dm.webp" alt="agent が初回カバレッジレポート、ワークブック、ダッシュボード、リサーチノートを Slack DM に送り、Slack 上でレポートを開いている" /><br /><sub><b>Slack</b>：すべてを 2 通のメッセージで届けます。先に成果物、次にリサーチノートで、各ファイルはそれぞれ個別のスレッド返信になります</sub></td>
  </tr>
</table>

### 📈 Agent と一緒にチャートを描く

どのチャットからでも、agent はライブの市場チャートを読み取り、書き込めます。サポートとレジスタンス、トレンドライン、フィボナッチの水準、イベントマーカーを描き、銘柄と時間軸ごとに保存します。Market View 以外では、描画はチャートカードとして届き、会話の横で同じチャートを開けます。

> *「CBRS の IPO 以降のチャートを出して、株価を動かした出来事に印を付けて。決算、ディール、アナリストの見解、ロックアップ。」*

agent は大きく動いた日ごとに何が株価を動かしたのかを突き止め、ライブチャートに描き込みます。イベントバッジ 18 個、ロックアップのマーカー 4 個、そして IPO 価格 $185 とコンセンサス目標株価のラインです。バッジにカーソルを合わせると、そのメモを読めます。

<p align="center">
  <img src="https://raw.githubusercontent.com/ginlix-ai/LangAlpha/main/docs/images/chart-cbrs-post-ipo-event-annotations.webp" alt="IPO 以降の CBRS のライブ日足チャート。agent が描いたイベントバッジ、ロックアップのマーカー、参照ラインがある" width="800" />
  <br />
  <sub><b>ライブチャート</b>：各メモは株価が反応した日に置かれ、チャットではすべての値動きが、根拠となる出典とともに表にまとめられます</sub>
</p>

### 🧾 すべての回答の根拠を確かめる

Sources パネルには、1 つの turn で agent が触れた開示資料、Web ページ、データ呼び出し、ファイルがすべて並びます。agent 自身の Python や Bash から行ったデータ呼び出しも含まれるので、結果を鵜呑みにせず、作業そのものを確かめられます。

<p align="center">
  <img src="https://raw.githubusercontent.com/ginlix-ai/LangAlpha/main/docs/images/chat-cbrs-sources-panel.webp" alt="1 つの turn の裏にある subagent の読み込み、SEC 開示資料、市場データ、データツールの呼び出しを一覧表示する Sources パネル" width="800" />
  <br />
  <sub><b>Sources</b>：この turn の 14 件と thread 全体の 560 件の出典を切り替えて表示します。出典は Web 検索とクロール、SEC 開示資料、市場データ、データツールに分類され、それぞれ実行時の prompt や引数も確認できます</sub>
</p>

## Agent トレーディング

私たちは、LangAlpha を agent トレーディングに最適な harness にすることに取り組んでいます。証券会社は今や AI agent が実際の口座で取引することを認めていますが、そこにつながる agent は汎用のものなので、注文は数ある tool call の 1 つにすぎません。LangAlpha では、注文こそが harness の設計の中心にある呼び出しです。

- **モデルが何をしても崩れない制限。** 各接続に何を許可するか、どの注文があなたの承認を待つか、承認された注文は 1 回だけ実行されるというルールは、すべての注文が通る 1 本の[統制された経路](#統制された注文)上で、ホスト側で強制されます。どんな prompt も、agent が書くどんなスクリプトもこれらを広げることはできず、どのモデルを使っても同じです。
- **読める注文経路。** モデルの呼び出しから証券会社が受け取るリクエストまで、注文が通るすべてのチェックはこのリポジトリ内のコードです。お金を預ける前にコードを読めますし、同じ制御のもとで自分のマシンで動かすこともできます。

現在、4 つの証券会社に同じ制御のもとで接続できます。

| | Robinhood | Interactive Brokers | moomoo | Webull |
| --- | :---: | :---: | :---: | :---: |
| 口座、ポジション、取引履歴 | ✅ | ✅ | ✅ | ✅ |
| 市場データとウォッチリスト | ✅ | ✅ | ✅ | ✅ |
| 注文プレビュー | ✅ | | | |
| ペーパートレード | | | ✅ | |
| 証券会社のアプリで確定するステージング注文 | | ✅ | | |
| 実注文 | ✅ | | ✅ | |

Robinhood はローカルアプリに戻るサインインしか受け付けないため、デスクトップアプリから接続します。moomoo は米国、中華圏、日本、東南アジアの市場をカバーします。

接続するときに、読み取り専用からペーパートレード、実注文まで、何を許可するかを選びます。

<table align="center">
  <tr>
    <td width="50%" align="center" valign="top"><img src="https://raw.githubusercontent.com/ginlix-ai/LangAlpha/main/docs/images/plugins-brokerages-capabilities.webp" alt="Plugins ページの Brokerages タブ。各証券口座の接続でできることを表示している" /><br /><sub><b>Brokerages</b>：証券口座を接続します。各カードには、連携する前にその口座で agent ができることが表示されます</sub></td>
    <td width="50%" align="center" valign="top"><img src="https://raw.githubusercontent.com/ginlix-ai/LangAlpha/main/docs/images/plugins-robinhood-connect-capabilities.webp" alt="Robinhood の接続画面。市場データから実注文まで、agent に許可する操作ごとにスイッチがあり、実注文はオフのままになっている" /><br /><sub><b>Capabilities</b>：信頼できるものだけをオンにします。保有ポジションをもとにサイズを決めた注文は承認カードで待機し、あなたが承認するまで証券会社には何も届きません</sub></td>
  </tr>
</table>

## 使い始める

**すでに料金を払っているモデルを使えます。** Claude や ChatGPT のサブスクリプションでサインインする、Kimi、GLM、MiniMax の coding plan を使う、任意の API key を持ち込む、セルフホストならローカルモデルを動かす、のいずれも選べます。

**いつもの場所で使えます。** ブラウザ、macOS、Windows、Linux 向けの[デスクトップアプリ](https://github.com/ginlix-ai/langalpha/releases)、そして [LangAlpha.ai](https://langalpha.ai) なら Slack、Discord、Telegram、Feishu、iMessage からも使えます。

**ホスト版（beta）。** [LangAlpha.ai](https://langalpha.ai) に登録すると、無料で始めるか、いずれかのプランを試用できます。市場データ、クラウド sandbox、証券口座の接続、チャネルはすべて設定済みです。beta 期間中はプランや機能が変わる可能性があります。

**セルフホスト。** 必要なのは Docker だけです。

```bash
git clone https://github.com/ginlix-ai/langalpha.git && cd langalpha
make config   # wizard: model, data sources, sandbox, web search
make up       # Postgres, Redis, backend and web app
```

[http://localhost:5173](http://localhost:5173) を開いてください。API はポート 8000 で待ち受けており、インタラクティブなドキュメントは `/docs` にあります。

**セルフホストとの違い。** agent、workspace、automation、skill、統制された注文は同じように動きます。それ以外は次のように異なります。

| | ホスト版 | セルフホスト |
| --- | --- | --- |
| 市場データ | 米国株とオプションのリアルタイム相場、時間外取引にも対応。中国 A 株は近日対応予定 | Yahoo Finance または FMP の相場。60 秒ごとに更新され、遅延データとして表示 |
| オプションと市場構造 | オプションチェーンとスナップショット、空売り残高、浮動株、値動き上位銘柄 | 利用不可 |
| 価格トリガー | ライブのティックで発火 | 遅延株価に対して 30 秒ごとにポーリング |
| チャネル | Slack、Discord、Telegram、Feishu、iMessage でチャットでき、automation の結果は Slack、Discord、iMessage に届く | Web アプリとデスクトップアプリ。automation の結果は自分で運用する webhook（`AUTOMATION_WEBHOOK_URL`）に届く |
| アカウント | サインインがあり、ユーザーごとにデータが分かれる | サインインのないローカルユーザー 1 人。アクセスできる人は誰でも、接続済みの証券口座も含めてあなたとして操作できる。自分のマシンかプライベートネットワークの中だけで使うこと |
| モデル | プランに含まれるモデル、自分の key、または Claude と ChatGPT のサインイン | 自分の key、coding plan、ローカルモデル、または Claude と ChatGPT のサインイン |
| 稼働 | AWS の US East で 24 時間 365 日稼働するので、手元の PC がスリープしていても automation と価格トリガーが発火する | 自分のマシンとその Docker スタックが起動している間だけ稼働 |
| リモートアクセス | ブラウザ、デスクトップアプリ、チャネルからどこでも使える | Web アプリと API はすべてのネットワークインターフェイスで待ち受けるため、ローカルネットワークから到達できる。外部から使うには、VPN か認証付きプロキシの背後に置いて自分で公開する必要がある |
| 運用 | アップグレード、マイグレーション、バックアップ、sandbox の容量は運営側で対応 | アップグレード、データベースのマイグレーション、バックアップは自分で実施する。Docker sandbox はマシンの CPU、メモリ、ディスクを共有する |

<details>
<summary><b>オプションの key と、それで使えるようになる機能</b></summary>

| Key | 使えるようになる機能 |
| --- | --- |
| `FMP_API_KEY` | ファンダメンタルズ、財務諸表、マクロ、アナリストデータ（[無料枠あり](https://site.financialmodelingprep.com/)） |
| `DAYTONA_API_KEY` | [Daytona](https://www.daytona.io/) のクラウド sandbox。設定しない場合、sandbox はローカルの Docker で動く |
| `R2_*`、`S3_*` または `OSS_*` と `storage.provider` | Cloudflare R2、AWS S3、Alibaba OSS、MinIO のオブジェクトストレージ。workspace のファイルスナップショット、memo、添付ファイル、skill のアーカイブ、大きなトランスクリプト、チャットウィジェットのデータを保持し、署名付きリンクでダウンロードを提供する。バケットがない場合、これらのデータは Postgres に保存され、チャート画像のキャプチャは無効になる |
| `TAVILY_API_KEY`、`SERPER_API_KEY`、`EXA_API_KEY`、`PARALLEL_API_KEY`、`BOCHA_API_KEY` | Web 検索。使うエンジンは `agent_config.yaml` の `search_api`（デフォルトは `tavily`）か、ユーザーごとに Settings で選ぶ |
| `FIRECRAWL_API_KEY` | 強化された Web 取得とサイトのクロール。組み込みのクローラーは key 不要 |
| `X_BEARER_TOKEN` | X の投稿検索とスレッドの参照。Plugins ページで vault secret として追加する |
| `LANGSMITH_API_KEY`、`OTEL_EXPORTER_OTLP_ENDPOINT` | トレースとメトリクス |

データ用の key がなくても、Yahoo Finance の株価、ファンダメンタルズ、アナリストデータ、スクリーニング、SEC EDGAR の開示資料、ローカルの Docker sandbox が使えます。すべてのコマンドは `make help` で確認できます。backend と Web アプリをホスト上で直接動かす方法は [CONTRIBUTING.md](../CONTRIBUTING.md#quick-start) を参照してください。

</details>

## Harness 設計

**どの context を、どんな形でモデルに届け、モデルが何に対して行動できるか。それが harness を決めます。**

すでに慣習があるところでは、LangAlpha はフロンティアラボが自社の agent に搭載しているものに従います。読み取り、書き込み、編集、検索を行うファイルツール、Bash シェル、`SKILL.md` ファイルとしての skill、そして `AGENTS.md` にならった workspace 用の `agent.md` です。こうした形はモデルの学習データに含まれている可能性が高く、モデルは最初から使い方を知っています。

### Workspace と Memory

仮説は数週間、数か月、あるいはそれ以上にわたって追跡され、どんな context window にも収まりません。その期間を通じて agent を同じ目標に向かわせ続けるのが workspace と memory です。だからこそ agent には最初から完全な workspace とファイルシステムが与えられます。それが他のすべての土台になります。

1 つの **computer** は 1 つの sandbox を持ちます。各 **workspace** はその上のフォルダで、それぞれ独自の thread、ファイル、メモを持つため、2 つ目のアイデアは新しいマシンを起動せずに数秒で開けます。同じ computer 上の workspace は OS ユーザーを共有します。分離したい場合は computer を分けてください。

各フォルダには、agent が管理する `agent.md`（目標、調査結果、thread とファイルの索引）、共有の `data/` ディレクトリ、タスクごとのフォルダがあります。memory、設定、履歴は特定の sandbox ではなくサーバー上にあります。FUSE マウントによって、どの computer でも通常のファイルとして見えるため、computer をまたいでも sandbox を作り直しても引き継がれ、Bash、コード、ファイルツールのどれからも同じ方法でアクセスできます。

| ストア | 保持する内容 |
| --- | --- |
| Memory | ユーザー単位と workspace 単位の、長く残る好みや調査結果。agent が自ら管理し、あなたやあなたの仕事について学んだことを保存し、古くなった項目は更新または削除する |
| Profile | ポートフォリオ、ウォッチリスト、好みの設定。agent が読み書きする JSON ファイルとして保持する |
| Automations | automation 1 つにつき 1 つの JSON ファイル。agent はファイルを書くことで automation を作成、編集、一時停止する。サーバーは保存のたびに内容を検証してから反映し、拒否した保存は tool result で報告する |
| Workflows | 保存済みの workflow スクリプト。agent は名前で実行したり、編集や追加をしたりできる |
| Memos | アップロードした PDF やノート。テキストを抽出してインデックス化し、agent が引用できるようにする。agent からは読み取り専用 |
| Transcripts | 過去のすべての thread。検索できるので、agent は先週自分が何をしたかを調べられる。読み取り専用 |

### Programmatic Tool Calling

LangAlpha は 2026 年 1 月の最初のリリース以来、programmatic tool calling を中心に作られています。agent はツールを 1 回ずつ JSON で呼び出すのではなく、ツールを import する Python を書きます。LangAlpha は任意の [MCP](https://modelcontextprotocol.io) server をドキュメント付きの Python モジュールに変換し、そのコードは sandbox で実行されます。理由は 2 つあります。

- **ツールは使う前から context を消費します。** JSON ツールとしてバインドすると、その turn で必要かどうかにかかわらず、すべての server のスキーマが毎回の呼び出しに乗ります。LangAlpha では prompt に載るのは server ごとに 1 行だけで、agent は server の完全なドキュメントを、初めて必要になったときにファイルから読みます。token を節約でき、ノイズも入らず、server を追加しても増えるのは 1 行です。
- **金融データは文章ではなく表です。** 12 銘柄の 10 年分の日足は約 30,000 行になります。モデルはそれをテキストとして扱えず、そのまま貼り付ければ context window が埋まってしまいます。コードなら agent はデータを集計、変換し、チャートにし、バリュエーションモデルやバックテストに渡せます。context に戻るのは結果だけです。

<p align="center">
  <img src="https://raw.githubusercontent.com/ginlix-ai/LangAlpha/main/docs/images/diagrams/ptc.webp" alt="Programmatic tool calling：モデルは sandbox 内で MCP server を Python モジュールとして import するコードを書き、生の行データは sandbox にとどまり、結果だけがモデルに戻る" width="880" />
</p>

コードはデフォルトであって、唯一の経路ではありません。どの MCP ツールも、Plugins ページでツールごとに設定すれば、通常の JSON tool call として実行できます。口座操作のように、呼び出し 1 つひとつを個別に見せるべき機微な操作にはこちらが向いています。注文ツールは常にこの方式で、[統制された経路](#統制された注文)上で実行されます。

### データとツール

大量のデータは Python モジュールを通じてコードで処理し、素早い参照は **direct** にバインドされます。株価、企業概要、開示資料、スクリーニングは 1 回の JSON tool call で取得でき、チャット内にカードとして表示されます。どちらの経路でも、context に収まらないほど大きな結果はファイルに保存され、代わりにプレビューが残ります。プレビューで足りなければ、agent はコードでそのファイルを開きます。

データソースは次の 3 種類のデータを扱います。

- **市場データ**：株価、株式・指数・暗号資産・FX・コモディティの価格履歴、オプションチェーン、スクリーナー、市場とセクターの概況。
- **ファンダメンタルズとマクロ**：財務諸表と財務比率、アナリストデータ、インサイダー取引、決算カレンダーと経済カレンダー、マクロ系列、イールドカーブ。
- **開示資料とテキスト**：SEC 開示資料と決算説明会のトランスクリプト、X の投稿、Web ページ。

LangAlpha 独自のデータ server は、コードで扱いやすいように作られています。市場データツールはすべて同じエンベロープ（`symbol`、`currency`、`timezone`、`count`、`data`、`source`）を返し、時系列は古い順に並び、失敗は型付きのエラーコードとして返ります。そのため agent のコードは、文章を解析するのではなく、結果をそのままインデックスで参照できます。

provider は市場ごとにフォールバックします。米国データには LangAlpha.ai のリアルタイムフィード、ファンダメンタルズとマクロには FMP、無料のフォールバック先として Yahoo Finance を使います。Web 検索は Tavily、Serper、Exa、Parallel、Bocha に対応します。Web 取得は組み込みのクローラーを使い、オプションで Firecrawl に委譲でき、provider ごとのサーキットブレーカーで保護されています。

データ以外に、agent は次のツールを使います。

| グループ | agent ができること |
| --- | --- |
| コードとファイル | Python とシェルコマンドを実行し、長いジョブはバックグラウンドで動かす。ファイルを読み取り、書き込み、編集、検索する。sandbox から配信するアプリのプレビューリンクを共有する |
| Web | 検索、ページの取得、Firecrawl があればサイト全体のクロールやマップ |
| 出力 | チャット内にインタラクティブなウィジェットを表示し、銘柄のライブチャートに注釈を描く |
| 協調 | [subagent と workflow](#subagent-と-agent-チーム) を起動し、構造化された質問をあなたに投げ、有効にすれば todo リストも管理する |
| あなたの接続 | OAuth またはヘッダー認証による証券口座とリモート MCP server、Agent Plugins |
| メッセージ | LangAlpha.ai では、接続済みのチャネルで、ファイルを添付してあなたにメッセージを送る |

**データ来歴**レイヤーは、agent が触れたすべてのソースを、モデルの context の外で記録します。対象は、市場データと MCP の呼び出し（モデルがツールを直接呼んだ場合も、sandbox 内で agent の Python や Bash が呼んだ場合も含む）、Web 検索と取得したページ、SEC 開示資料、そして agent が読んだファイル、memo、memory です。各レコードには provider、マスク済みの引数、タイムスタンプ、結果のフィンガープリントが含まれ、結果そのものも 64 KB まで保存され、呼び出しを行った turn と agent（main agent か subagent か）に紐づけられます。Sources パネルは turn ごとにこれらを一覧表示し、API も同じレコードを返すので、回答をその根拠と突き合わせて確かめられます。

### Subagent と Agent チーム

main agent は、それぞれ独自の context window を持つ **subagent** に作業を任せます。理由は 3 つです。

- **より多くの作業を同時に。** subagent はバックグラウンドで複数並列に動き、その間も main agent は作業を続けたり、あなたと会話を続けたりできます。
- **より広く、より深いリサーチ。** 1 つの質問を企業、セグメント、ソースごとに 1 つの subagent へ振り分け、各 subagent は担当部分に必要なだけ深く掘り下げられます。
- **全体像を保つ main agent。** subagent が返すのは調査結果であり、その裏にある検索や tool call ではありません。そのため main agent の context には仮説と計画が保たれ、軌道を外れません。

組み込みの subagent は 5 つ（`research`、`general-purpose`、`data-prep`、`equity-analyst`、`report-builder`）で、`agent_config.yaml` で追加できます。subagent とはいつでもやり取りできます。main agent は実行中の subagent に新しい指示を送ったり、完了した subagent を全履歴付きで再開したりできます。各 subagent の tool call と出力は UI にライブでストリーミングされ、あなたが直接メッセージを送ることもできます。

**workflow** はスケールをさらに広げます。大規模な調査では、agent は workflow を実行します。workflow は短い JavaScript プログラムで、サーバー上の sandbox 化された QuickJS runtime の中で、`agent()`、`parallel()`、`pipeline()` を使って subagent に作業を振り分けます。保存済みの workflow を名前で実行することも、その場で新しく書くこともできます。ループを回すのは会話ではなくプログラムなので、100 社のスクリーニングでも 100 turn はかかりません。ただし、100 個の subagent 分の token は消費します。

<p align="center">
  <img src="https://raw.githubusercontent.com/ginlix-ai/LangAlpha/main/docs/images/diagrams/teams.webp" alt="context に仮説、計画、調査結果だけを保持する main agent が、subagent と、100 回の agent() 呼び出しに振り分ける workflow に作業を任せる。それぞれ tool call ではなく調査結果を返し、subagent は workspace のファイルを共有する" width="880" />
</p>

### Context の組み立て方

モデルへの呼び出しはすべて、最も安定したものから最も変わりやすいものの順に並んだレイヤーで組み立てられます。そのため長いセッションでも、provider の prompt cache に継続してヒットします。

<p align="center">
  <img src="https://raw.githubusercontent.com/ginlix-ai/LangAlpha/main/docs/images/diagrams/context.webp" alt="1 回のモデル呼び出しを、最も安定したものから最も変わりやすいものへ並べた 5 つのレイヤー：ツール、system prompt、固定された thread baseline、履歴、市場情報のスタンプを含む末尾。最初の 4 つのレイヤーの後にそれぞれキャッシュポイントがある" width="880" />
</p>

- **System prompt。** 1 つのテンプレートを、使用中のモデルの guidance レベルに合わせてレンダリングします。
- **Thread baseline。** turn の開始時に 1 回だけ読み込み、その後は固定します。内容は workspace、あなたのプロフィール、MCP server の一覧、skill のマニフェスト、`agent.md`、2 階層の memory、memo のインデックスです。後でいずれかが変わると、agent には何が変わったかを伝える短い行が届き、baseline はバイト単位で同一のまま保たれます。baseline は compaction の後か、行が 20 件たまった時点で再構築されます。
- **Runtime 行。** 各 turn は、時刻、市場セッション（変わった場合）、前回の turn からの経過時間を伝える行で始まり、それぞれの provider が想定する形で送られます。
- **Steering。** agent の作業中に送ったメッセージは、main agent でも subagent でも、次のモデル呼び出しの前に届きます。
- **Compaction。** まず古いツールの引数を切り詰め、元のデータはファイルとして残します。モデルの上限に近づくと、古い turn は要約にまとめられ、完全なトランスクリプトが保存され、要約にはその読み先が書かれます。

### モデル

LangAlpha はフロンティアモデルだけに合わせて調整されているわけではありません。すべてのモデルを同じツールと middleware で動かすので、オープンウェイトのモデルでも、わずかなコストで同等の結果を得られます。thread の途中で provider を切り替えることもでき、失敗した呼び出しはリトライされ、その後は設定したモデルにフォールバックします。

多くの provider は Chat Completions 形式を受け付けますが、いくつかの provider では、重要な情報が落ちる互換レイヤーにすぎません。LangAlpha は provider ごとに、そのモデルを最もよく活かす API 形式を選びます。たとえば OpenAI と Codex には OpenAI の [Responses API](https://platform.openai.com/docs/api-reference/responses)、Claude、Kimi、MiniMax、DeepSeek には Anthropic の [Messages API](https://docs.anthropic.com/en/api/messages) を使います。選択の決め手は 2 つです。

- **思考が保持されます。** reasoning は turn 内でも turn をまたいでも、各 provider 独自の形式でモデルに戻されます。Anthropic の署名付き thinking ブロック、OpenAI の暗号化された reasoning item、GLM の reasoning テキストです。
- **runtime context は適切なチャネルで届きます。** harness はモデルに多くの runtime context を渡します。OpenAI 形式の API では `developer` メッセージとして、それを尊重する Anthropic と GLM では会話途中の `system` メッセージとして、それ以外では user メッセージの中に入れて送ります。

各モデルは 3 つのダイヤルで調整でき、Settings でアカウント全体またはモデルごとに設定します。

- **Prompt guidance。** フロンティアモデルには簡潔な prompt を、小さなモデルには具体例と段階的な手順を含む詳細な prompt を渡します。どちらも 1 つのテンプレートから生成されるので、内容がずれることはありません。
- **Reasoning effort。** `none` から `max` までの 1 つの尺度で、各ベンダー独自のパラメーターに書き込まれ、モデルが提供する最も近いレベルまで引き下げられます。入力欄から、メッセージ単位で上書きすることもできます。
- **Compaction profile。** 積極的なものから緩やかなものまで 4 つのプリセットがあり、長い thread をどれだけ早く compaction するかを決めます。デフォルトでは、モデルの context window に応じて 1 つが選ばれます。

| 接続方法 | Provider |
| --- | --- |
| サブスクリプションでのサインイン | Claude（Claude Code）、ChatGPT（Codex） |
| Coding plan | Kimi、GLM、MiniMax |
| API key | OpenAI、Anthropic、Gemini、DeepSeek、Qwen（DashScope）、Kimi（Moonshot）、GLM（Zhipu）、MiniMax、OpenRouter、Groq、Cerebras |
| ローカル | Ollama、LM Studio、vLLM |

key と OAuth token は pgcrypto で保存時に暗号化されます。

### Skills と Plugins

skill は [Agent Skills](https://agentskills.io/specification) 仕様に、plugin は [Agent Plugins 1.0.0](https://agent-plugins.org) 形式に従います。組み込みの MCP server と skill は [`plugins/`](../plugins/) 内の plugin バンドルとして提供されます。これは、Plugins ページでアップロードしたり git URL からインストールしたりするものと同じ形式です。これらの標準に加えて、LangAlpha は次の機能を追加しています。

- **skill は必要なときに読み込まれます。** prompt に載るのは skill ごとに 1 行のマニフェストだけで、skill 全体は slash command か、agent が `SKILL.md` を読んだときに読み込まれます。automation ツールのように skill が持ち込むツールは、その skill が読み込まれるまで隠れたままです。
- **自分の skill を、自分のコマンドで。** skill を zip でアップロードして好きな slash command を割り当てたり、agent に GitHub から workspace へインストールさせたりできます。
- **plugin の拡張は 1 つのブロックに。** `mcp.json` は形式自体のフィールドしか受け付けません。LangAlpha が追加するものはすべて、形式で唯一の拡張ポイントである `plugin.json` の `extensions["ai.langalpha"]` の下に置かれます。prompt に届く server ごとの `description` と `instruction`、agent が各ツールを最初にどこまで見るかを決める `tool_exposure_mode`（`summary` または `detailed`）、そして server が必要とする各 credential とそのバインド先を示す `secrets` です。このブロックを取り除いても、パッケージはどの Agent Plugins ホストにもインストールできます。詳細は [`plugins/README.md`](../plugins/README.md) を参照してください。
- **サードパーティの server は隔離して動きます。** LangAlpha が所有していないコードの server は、アプリ自身の環境からではなく、`uvx` か `npx` でバージョンを固定して起動します。そのため、どちらか一方で SDK をアップグレードしても、もう一方が壊れることはありません。

<p align="center">
  <img src="https://raw.githubusercontent.com/ginlix-ai/LangAlpha/main/docs/images/plugins-builtin-packages.webp" alt="Plugins ページの Packages タブ。6 つの組み込み plugin バンドルが並んでいる" width="720" />
  <br />
  <sub><b>Packages</b>：MCP server と skill をまとめた 6 つの組み込みバンドルは、それぞれ一括でオン・オフでき、自分のバンドルも 50 個まで追加できます</sub>
</p>

組み込みの skill は 38 個です。

| バンドル | Skills |
| --- | --- |
| Research | DCF モデル、類似企業比較、財務 3 表モデル、モデルの更新とチェック、初回カバレッジ、決算プレビューと決算分析、投資仮説トラッカー、トレードピッチ、企業プロファイル、競合分析、セクター概況、影響分析、カタリストカレンダー、アイデア創出、モーニングノート、マーケットウォッチ、プレゼン資料チェック |
| Deliverables | Excel、Word、PowerPoint、PDF、HTML レポート、インタラクティブダッシュボード、インラインウィジェット、チャート注釈、UI デザイン |
| Service | automation、オンボーディング、ユーザープロフィールとポートフォリオ、secretary、workflow、プロダクトヘルプ、自己改善 |
| Alternative data | X リサーチ、Web スクレイピング |

謝辞：一部のリサーチ skill は [anthropics/financial-services-plugins](https://github.com/anthropics/financial-services-plugins) を元にしています。

## フルスタックアーキテクチャ

<p align="center">
  <img src="https://raw.githubusercontent.com/ginlix-ai/LangAlpha/main/docs/images/diagrams/architecture.webp" alt="アーキテクチャ：Web、デスクトップ、チャットチャネルはマルチワーカーの FastAPI backend につながり、その run ライフサイクルが agent を駆動する。agent は sandbox 化された computer の中で動き、そのファイルは Postgres とオブジェクトストレージに永続化される。証券口座とリモート MCP への呼び出しは、credential を付与する egress relay を通って外に出る" width="880" />
</p>

turn は、それを開始した HTTP 接続から独立したバックグラウンドの run として実行されます。イベントは run ごとの Redis stream を経由して SSE で配信されるため、タブを閉じてもネットワークが切れても何も失われません。クライアントは再接続して追いつき、完了した turn は LangGraph の checkpoint から再生されます。Postgres が信頼できる唯一の情報源で、Redis は転送路にすぎないため、backend は複数のワーカーで動き、どのワーカーでもストリームの配信、キューの処理、孤立した run の復旧を担当できます。設定すれば、agent の run は LangSmith にトレースされ、backend は OpenTelemetry でトレースとメトリクスをエクスポートします。

**ファイルは sandbox より長く残ります。** 各 turn の後と computer が停止するたびに、変更のあった workspace フォルダはすべてスナップショットされます。マニフェストは、パスごとに 1 行の Postgres レコードです。データ本体は sandbox から S3 互換のオブジェクトストレージへ直接送られ、オブジェクトストレージなしで運用している場合は Postgres に保存されます。computer が停止している間もファイルブラウザとダウンロードは使え、作り直した sandbox はスナップショットから復元されます。1 週間停止したままの Daytona computer は、さらにディスク全体をコールドストレージにアーカイブし、次回の起動時にそこから再開します。

**チャネルゲートウェイ**は LangAlpha.ai の一部です。Slack、Discord、Telegram、Feishu、iMessage の会話を Web アプリと同じチャット API に届け、automation の結果を選んだチャネルに投稿します。

### 統制された注文

注文は実際のお金を動かすため、専用の経路を通ります。注文ツールは direct な JSON 呼び出しに固定されています。1 回の呼び出しが 1 件の注文になり、システムはそれを把握し、表示し、止めることができます。sandbox のスクリプトなら 1 回の実行で何件でも注文を出せてしまうため、注文ツールが sandbox に公開されることはありません。

<p align="center">
  <img src="https://raw.githubusercontent.com/ginlix-ai/LangAlpha/main/docs/images/diagrams/orders.webp" alt="統制された注文：モデルの注文呼び出しは記録され、承認のためにあなたに表示される。承認によって実行トークンが発行され、egress relay がそれを検証し、1 回限りの送信権を確保して、保存済みの credential で注文を送る" width="880" />
</p>

- **接続ごとの同意。** 証券口座を接続するときに、どの capability グループを許可するかを選びます。relay は、モデルが何を求めても、その範囲外の呼び出しをすべて拒否します。
- **注文ごとの承認。** 実注文とステージング注文はデフォルトであなたの承認を待ち、ペーパー注文は待ちません。モードごとに、あなたがスイッチで切り替えられます。
- **実行は 1 回だけ。** 承認によって、その試行、そのツール、その引数のハッシュに紐づいた短命なトークンが発行されます。引数の変更、リプレイ、2 回目の呼び出しは relay で失敗します。
- **証券会社の credential は sandbox に入りません。** 証券口座の OAuth token とリモート MCP の credential はホスト上の relay が付与するため、agent が書くコードからは見えません。
- **読める台帳。** すべての試行、承認、拒否、約定は Orders ページに記録され、照合処理が証券会社自身の記録と突き合わせて注文の状態を確定させます。

### セキュリティ

- **Vault。** API key を一度保存すれば、`from vault import get` でどの workspace のコードからでも使えます。secret は保存時に暗号化され、表示や変更ができるのは所有者だけです。
- **漏えい箇所のマスク。** すべての tool result はモデルに届く前に既知の secret 値がないかスキャンされ、一致した部分は `[REDACTED:NAME]` に置き換えられます。ダウンロードや共有ファイルにも同じ処理が適用されます。
- **sandbox での実行。** agent のコードは Daytona または Docker の sandbox で実行され、保護パスのガードが、システムディレクトリに触れる tool call を拒否します。

## ロードマップ

- [x] リサーチ harness：コードで扱うデータ、永続する workspace、agent チーム
- [x] LangAlpha.ai とデスクトップアプリ
- [x] 統制された注文を備えた証券口座連携
- [ ] 暗号資産と予測市場向けに調整した agent
- [ ] あなた自身の AI agent（ChatGPT、Claude）から LangAlpha に取引を引き渡せるようにする

## コントリビュート

Issue と pull request を歓迎します。[CONTRIBUTING.md](../CONTRIBUTING.md) を参照してください。このリポジトリには、backend と agent のコア（[`src/`](../src/)）、Web アプリ（[`web/`](../web/)）、デスクトップシェル（[`desktop/`](../desktop/)）、組み込み plugin（[`plugins/`](../plugins/)）が含まれています。パートナーシップについては [contact@ginlix.ai](mailto:contact@ginlix.ai) までメールでご連絡ください。

<a href="https://star-history.com/#ginlix-ai/langalpha&Date">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=ginlix-ai/langalpha&type=Date&theme=dark" />
    <img alt="スター履歴" src="https://api.star-history.com/svg?repos=ginlix-ai/langalpha&type=Date" width="600" />
  </picture>
</a>

## 免責事項

LangAlpha はソフトウェアであり、金融アドバイザーではありません。LangAlpha が生成するものはいずれも、投資助言でも、証券の売買の推奨でもありません。agent はあなたが与えた権限の範囲内でのみ動作し、あなたの口座で出されたすべての注文の責任はあなたにあります。必ずご自身でデューデリジェンスを行ってください。

## ライセンス

[Apache 2.0](../LICENSE)

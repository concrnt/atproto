# concrnt-atproto-bridge

concrnt (v2) と Bluesky / atproto を双方向に接続するブリッジです。
[activitypub ブリッジ](https://github.com/concrnt/activitypub) と同じ位置づけの
サービスモジュールとして動作します。

## 仕組み

- **concrnt → Bluesky**: ブリッジが「読み取り専用PDS」として振る舞います。
  ブリッジ有効化した concrnt アカウントごとに did:plc と atproto repo (MST) を
  管理し、`com.atproto.sync.*` XRPC と `subscribeRepos` firehose を提供して
  relay (bsky.network) にクロールさせ、appview に投稿を取り込ませます。
- **Bluesky → concrnt**: Jetstream を購読し、フォロー中ユーザーの投稿・
  リポストと、ブリッジ済みアカウント宛のリプライ・引用・いいねを concrnt に
  取り込みます。投稿本文はコピーせず、軽量参照スキーマ
  `atproto/record.json` (`{did, atUri, cid, profileOverride}`) として配送し、
  表示時にクライアントが解決します(ap/note.json と同じ方式)。

## 対応マッピング

| concrnt | Bluesky |
|---|---|
| m/markdown, gfm, plaintext | `app.bsky.feed.post`(plaintext化 + link/tag facets) |
| m/media.json | post + `embed.images`(≦4枚、事前取り込みblob) |
| m/reply.json | post + `reply{root, parent}` |
| m/reroute.json(本文なし) | `app.bsky.feed.repost` |
| m/reroute.json(本文あり) | post + `embed.record`(引用) |
| a/like.json, a/reaction.json | `app.bsky.feed.like`(リアクションは縮退) |
| delete.json | レコード削除 |
| profiles/main | `app.bsky.actor.profile` |
| atproto/follow.json(新設) | `app.bsky.graph.follow` |

300グラフェン超過時は切り詰めて `…` を付け、元投稿へのリンクを
`embed.external` に載せます。

## アカウント登録(持ち込みドメイン + 2段階検証)

ハンドルはサーバーが発番せず、ユーザーが**自分で管理するドメイン**を持ち込みます。
DID発行前にDNSレコードは書けないため、登録は2段階です:

1. `POST /atproto/api/setup {handle: "alice.example.com"}` → DID を発行し、
   その DID が `at://alice.example.com` を alsoKnownAs として主張する
   `status: pending` のアカウントを作ります。この時点では repo 初期化・
   firehose イベント・relay クロールは**行いません**。レスポンスに設定すべき
   レコードが入ります:
   - DNS TXT: `_atproto.alice.example.com` = `did=<発行されたDID>`(推奨)
   - または HTTPS: ユーザー自身のWebサーバーで
     `https://alice.example.com/.well-known/atproto-did` が DID を plain text で
     返すようにする(ブリッジ側の関与はありません)
2. ユーザーが上記いずれかを設定 → `POST /atproto/api/verify` → ハンドルが
   その DID に解決できることを確認し、成功時にアカウントを active 化
   (repo 初期化 → #identity → #account → プロフィール同期 → requestCrawl)。

PDS の serviceEndpoint は concrnt サーバーの FQDN(= ゲートウェイの公開オリジン)で、
専用ドメインもワイルドカード DNS/TLS も不要です。

## フォロー

concrnt ユーザーが Bluesky ユーザーをフォローする操作は、ユーザー自身の
cckv 空間への follow document commit が真実です:

```
key:    cckv://<ccid>/atproto.concrnt.world/follows/<hash(did)>
schema: https://schema.concrnt.world/atproto/follow.json
value:  {"did": "did:plc:..."}
```

キー末尾は ActivityPub ブリッジの follows キーと同じ CDID ハッシュ
(`CDID.newFromStringX(did)`)です。ブリッジはキーをパースせず value の did を
読むため、キー形状には依存しません。

ブリッジは Redis イベントでこれを検知し、ユーザーのブリッジ repo に
`app.bsky.graph.follow` を書き込みます(相手にフォロー通知が届きます)。
document の削除がアンフォローです。加えて起動時と定期リロードで cckv 空間を
prefix クエリして repo と突合するため、ブリッジ停止中の
フォロー/アンフォローも復元されます(クエリは匿名アクセスなので、読み取り
ポリシーで保護されたレコードは対象外です)。handle→DID の解決は
`GET /atproto/api/resolve-actor?target=<handle>` を使ってください。

## ユーザー設定

ActivityPub ブリッジと同様、ユーザーごとの設定はユーザー自身の cckv 空間の
settings record が真実です(クライアントが commit し、ブリッジは読むだけ):

```
key:    cckv://<ccid>/atproto.concrnt.world/settings
schema: https://schema.concrnt.world/atproto/settings.json
value:  {"listenTimelines": ["cckv://..."], "enabled": true}
```

- `listenTimelines`: Bluesky へ転送する投稿の転送元タイムライン。空または
  レコード未作成ならホームタイムライン
  (`cckv://<ccid>/concrnt.world/profiles/main/home-timeline`)のみ。
- `enabled`: ブリッジの一時停止フラグ。省略・レコード未作成は有効扱い。
  `at_entities.enabled` はこの値のキャッシュで、daemon が追従更新します。

## デプロイ

### 必要なもの

- Postgres(ブリッジ専用DB)
- concrnt core と**同一の** Redis(イベント購読)
- 永続ボリューム(`dataDir`: carstore / blobs / firehoseイベントログ)
- **専用ドメインは不要**。全経路(公開 PDS `/xrpc` 含む)を concrnt ゲートウェイ
  経由でプロキシします。PDS の serviceEndpoint は concrnt サーバーの FQDN
  そのものになるため、ワイルドカード DNS/TLS も要りません。

### 単一リスナー(すべてゲートウェイ経由)

ブリッジは 1 ポート(既定 8010)で `/cc-info`・`/atproto/api/*`・`/xrpc/*`・
firehose をすべて配信します。**このポートは直接公開せず**、必ず concrnt
ゲートウェイ経由でアクセスさせてください(管理 API はゲートウェイが付与する
cc-requester ヘッダを信頼するため)。handle 解決はユーザーのドメイン側で
完結するため、ブリッジは `/.well-known/atproto-did` を配信しません。

### concrnt core への登録

管理 API(認証あり)と公開 PDS 面(認証なし)は `noAuth` が異なるため、
サービスエントリを 2 つに分けます(同じ host:port を指す):

```yaml
services:
  - name: world.concrnt.atproto
    host: atproto-bridge
    port: 8010
    paths: ["/atproto"]
    preservePath: true
  - name: world.concrnt.atproto.pds
    host: atproto-bridge
    port: 8010
    paths: ["/xrpc"]
    preservePath: true
    noAuth: true
```

> **前提**: concrnt 本体の `internal/present/rest/proxy.go` に、プロキシ
> ハンドラ冒頭で `cc-requester` / `cc-requester-tag` ヘッダを常に `Del` する
> 修正が必要です(未認証パス経由でのヘッダ偽装防止)。この修正は本ブリッジと
> 同じ変更セットに含まれます。
>
> **firehose(WebSocket)**は concrnt ゲートウェイの ReverseProxy を通ります。
> Go の ReverseProxy は Upgrade を透過しますが、本体は WS プロキシの実績が
> ないため、デプロイ時に `wss://<concrnt>/xrpc/com.atproto.sync.subscribeRepos`
> の疎通を必ず確認してください。

### 設定

`config.example.yaml` をコピーして編集し、`CONFIG_PATH` で指定します
(既定 `/app/config.yaml`)。マイグレーションは起動時に自動実行されます。

## 管理API(concrnt ゲートウェイ経由)

| エンドポイント | 説明 |
|---|---|
| `POST /atproto/api/setup {handle}` | 登録1段階目: 持ち込みドメインで did:plc 発番、status=pending。検証手順を返す |
| `POST /atproto/api/verify` | 登録2段階目: handle の DNS/well-known 検証成功で active 化(repo 初期化・crawl) |
| `GET /atproto/api/info` | ブリッジ情報 + 自分のエンティティ状態 |
| `GET /atproto/api/following` | フォロー中の Bluesky アカウント一覧 |
| `GET /atproto/api/resolve-actor?target=` | handle/DID → プロフィールプレビュー |
| `GET /atproto/api/resolve?uri=` | at-uri → appview の post view(atproto/record.json 表示用) |

## 開発

```sh
go test ./...
docker compose up   # postgres + redis + bridge
```

`go.work` でローカルの `../concrnt` を参照しています(リポジトリ移動時は調整)。

セルフ検証には [goat](https://github.com/bluesky-social/goat) が便利です:

```sh
goat repo export --host http://localhost:8011 <did>
goat firehose --host ws://localhost:8011
```

## 注意事項・既知の制約

- Jetstream 経由の受信は署名検証なし・リプレイ窓限定です(長時間停止時の
  取りこぼしは許容)。将来 relay firehose 直結に差し替え可能な構造です。
- `getRecord` の MST proof、`#sync` イベント、`prevData`(sync v1.1)は未実装
  です。relay の lenient モードを前提としています。
- relay にはホスト毎アカウント上限があります。アカウント数が増えたら
  Bluesky 運営に trusted domain 枠を相談してください。
- `masterRotationKey` は全ブリッジ DID の PLC rotation key です。漏洩すると
  全アカウントの DID を乗っ取れるため厳重に管理してください。

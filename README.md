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

## フォロー

concrnt ユーザーが Bluesky ユーザーをフォローする操作は、ユーザー自身の
cckv 空間への follow document commit が真実です:

```
key:    cckv://<ccid>/atproto.concrnt.world/follows/<did>
schema: https://schema.concrnt.world/atproto/follow.json
value:  {"did": "did:plc:..."}
```

ブリッジは Redis イベントでこれを検知し、ユーザーのブリッジ repo に
`app.bsky.graph.follow` を書き込みます(相手にフォロー通知が届きます)。
document の削除がアンフォローです。handle→DID の解決は
`GET /atproto/api/resolve-actor?target=<handle>` を使ってください。

## デプロイ

### 必要なもの

- Postgres(ブリッジ専用DB)
- concrnt core と**同一の** Redis(イベント購読)
- 永続ボリューム(`dataDir`: carstore / blobs / firehoseイベントログ)
- **専用ドメイン + ワイルドカードDNS/TLS**: `pdsHost`(例 `atp.example.net`)と
  `*.atp.example.net` を pdsPort(既定8011)に直接ルーティングします。
  DID document の serviceEndpoint は origin しか書けないため、
  concrnt ゲートウェイのパスプロキシは経由できません。

### リスナーは2系統

| ポート | 用途 | 公開範囲 |
|---|---|---|
| 8010 (`server.port`) | `/cc-info`, `/atproto/api/*` | concrnt ゲートウェイからのみ(直接公開しないこと。cc-requesterヘッダを信頼するため) |
| 8011 (`server.pdsPort`) | `/xrpc/*`, firehose, `/.well-known/atproto-did` | インターネット(pdsHost + ワイルドカード) |

### concrnt core への登録

core の config.yaml:

```yaml
services:
  - name: world.concrnt.atproto
    host: atproto-bridge
    port: 8010
    paths: ["/atproto"]
    preservePath: true
```

### 設定

`config.example.yaml` をコピーして編集し、`CONFIG_PATH` で指定します
(既定 `/app/config.yaml`)。マイグレーションは起動時に自動実行されます。

## 管理API(concrnt ゲートウェイ経由)

| エンドポイント | 説明 |
|---|---|
| `POST /atproto/api/setup {handle}` | ブリッジ有効化: 鍵生成 → did:plc 登録 → repo 初期化 → プロフィール同期。handle は `<name>.<pdsHost>` になります |
| `GET/POST /atproto/api/settings` | enabled / listenTimelines |
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

# m1s-backend

M1S Backend - メンバー情報管理システムのバックエンド

## 概要
このリポジトリはM1S（Member Information System）のバックエンド処理を管理するものです。
AWS Glueを使用してメンバー情報の最新化処理を行います。

## 主要機能
- **メンバー情報最新化ジョブ**: AWS Glueを使用したデータ処理パイプライン
- **データ品質チェック**: 入力データの検証と品質保証
- **データ変換**: 名前・メールアドレス等の正規化処理
- **監視・アラート**: CloudWatchを使用した処理状況の監視

## ディレクトリ構成
```
m1s-backend/
├── glue_jobs/                    # AWS Glueジョブスクリプト
│   └── member_info_update.py     # メンバー情報最新化ジョブ
├── config/                       # 設定ファイル
│   └── job_config.yaml          # ジョブ設定
├── scripts/                      # デプロイメントスクリプト
│   └── deploy_glue_job.sh       # Glueジョブデプロイスクリプト
├── cloudformation/               # インフラ定義
│   └── glue-job-infrastructure.json # CloudFormationテンプレート
├── tests/                        # テストコード
│   ├── test_member_info_update.py  # ユニットテスト
│   └── requirements-test.txt       # テスト用依存関係
├── docs/                         # ドキュメント
│   └── member_info_update_job.md # ジョブ詳細ドキュメント
└── requirements.txt              # 依存関係
```

## セットアップ

### 前提条件
- Python 3.8+
- AWS CLI設定済み
- 適切なIAM権限

### インストール
```bash
pip install -r requirements.txt
```

### テスト実行
```bash
pip install -r tests/requirements-test.txt
pytest tests/
```

## デプロイ

### CloudFormationを使用したインフラ構築
```bash
aws cloudformation deploy \
  --template-file cloudformation/glue-job-infrastructure.json \
  --stack-name m1s-glue-infrastructure \
  --parameter-overrides GlueScriptBucket=your-script-bucket DataBucket=your-data-bucket \
  --capabilities CAPABILITY_IAM
```

### Glueジョブのデプロイ
```bash
export AWS_ACCOUNT_ID=your-account-id
./scripts/deploy_glue_job.sh
```

## 使用方法

### 手動実行
```bash
aws glue start-job-run \
  --job-name member-info-update-job \
  --arguments \
    --SOURCE_TYPE=s3 \
    --SOURCE_PATH=s3://your-input-bucket/members/ \
    --DESTINATION_TYPE=s3 \
    --DESTINATION_PATH=s3://your-output-bucket/processed/
```

### 定期実行
CloudFormationテンプレートにより、毎日午前2時（JST）に自動実行されるよう設定されます。

## 監視
- CloudWatch Logsでジョブ実行ログを確認
- CloudWatch Metricsでパフォーマンスを監視
- 失敗時はSNS通知（設定時）

## 開発・コントリビューション
1. ブランチを作成
2. 変更を実装
3. テストを実行
4. プルリクエストを作成

詳細なドキュメントは `docs/` ディレクトリを参照してください。

## ライセンス
Private - M1S Internal Use Only
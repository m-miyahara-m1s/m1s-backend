# AWS Glue Member Information Update Job
# メンバー情報最新化ジョブ ドキュメント

## 概要
このジョブは、様々なデータソースからメンバー情報を読み込み、データの正規化と品質チェックを行った後、指定された出力先に書き込むAWS Glueジョブです。

## 機能
- CSV、データベース等の複数のデータソースからの読み込み
- データ検証（必須フィールドチェック、重複チェック等）
- データ正規化（名前・メールアドレス等の統一）
- S3、データベースへの出力
- エラーハンドリングとログ出力
- CloudWatch監視

## ジョブパラメータ
| パラメータ名 | 必須 | 説明 | 例 |
|-------------|------|------|-----|
| JOB_NAME | ○ | ジョブ名 | member-info-update-job |
| SOURCE_TYPE | ○ | ソースタイプ | s3, jdbc |
| SOURCE_PATH | ○ | ソースパス | s3://bucket/path/ |
| DESTINATION_TYPE | ○ | 出力先タイプ | s3, jdbc |
| DESTINATION_PATH | ○ | 出力先パス | s3://bucket/output/ |

## データスキーマ

### 入力データ
```
member_id: String (必須) - メンバーID
name: String (必須) - 氏名
email: String (必須) - メールアドレス
department: String (任意) - 部署
join_date: String (任意) - 入社日
```

### 出力データ
```
member_id: String - メンバーID
name: String - 氏名
name_normalized: String - 正規化済み氏名
email: String - メールアドレス
email_normalized: String - 正規化済みメールアドレス
department: String - 部署
join_date: String - 入社日
status: String - ステータス (active/inactive)
updated_at: String - 更新日時
```

## データ変換ルール

### 名前の正規化
- 全角数字を半角数字に変換
- 前後の空白を除去

### メールアドレスの正規化
- 小文字に統一
- 前後の空白を除去

### ステータス判定
- メールアドレスが空または null の場合: inactive
- それ以外の場合: active

## 品質チェック
1. **必須フィールドチェック**: member_id, name, email が存在するか
2. **重複チェック**: member_id の重複を検出
3. **メールアドレス形式チェック**: 有効なメールアドレス形式か

## エラーハンドリング
- データ読み込みエラー: ログ出力後、処理中断
- データ変換エラー: エラー記録を残して処理継続
- 出力エラー: ログ出力後、処理中断

## ログ出力
- CloudWatch Logsに詳細ログを出力
- ログレベル: INFO, WARNING, ERROR
- 処理件数、エラー件数等の統計情報を出力

## 監視・アラート
- CloudWatch Metricsでジョブ実行状況を監視
- 失敗時にSNS通知（設定時）
- 実行時間の監視

## セキュリティ
- IAMロールによる最小権限の原則
- データベース認証情報はAWS Systems Managerで管理
- S3データの暗号化

## パフォーマンス
- デフォルト設定: G.1X x 2ワーカー
- 大量データ処理時は設定を調整可能
- Spark UIでパフォーマンス分析可能

## 使用例

### S3 to S3
```bash
aws glue start-job-run \
  --job-name member-info-update-job \
  --arguments \
    --SOURCE_TYPE=s3 \
    --SOURCE_PATH=s3://input-bucket/members/ \
    --DESTINATION_TYPE=s3 \
    --DESTINATION_PATH=s3://output-bucket/processed/
```

### Database to S3
```bash
aws glue start-job-run \
  --job-name member-info-update-job \
  --arguments \
    --SOURCE_TYPE=jdbc \
    --SOURCE_PATH=members_table \
    --DESTINATION_TYPE=s3 \
    --DESTINATION_PATH=s3://output-bucket/processed/
```

## トラブルシューティング

### よくあるエラー
1. **S3アクセスエラー**: IAMロールの権限を確認
2. **データベース接続エラー**: 接続文字列、認証情報を確認
3. **メモリ不足エラー**: ワーカー数またはワーカータイプを増加

### ログの確認方法
```bash
aws logs describe-log-groups --log-group-name-prefix /aws/glue/member-info-update
aws logs get-log-events --log-group-name /aws/glue/member-info-update-job --log-stream-name <stream-name>
```

## 定期実行
CloudWatch Eventsを使用して定期実行を設定
- デフォルト: 毎日午前2時（JST）
- cron式: `cron(0 17 * * ? *)` (UTC)

## 開発・テスト
- 単体テスト: `pytest tests/`
- ローカル開発環境でのテスト実行
- ステージング環境での統合テスト
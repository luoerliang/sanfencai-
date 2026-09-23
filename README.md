# 三分六合彩 AI免费外部备份版 v28F

用途：不买Render持久盘、不绑卡，也尽量避免AI学习记录和实盘验证在重部署后丢失。

方案：
- Render Free继续跑程序
- Supabase Free Storage保存学习检查点
- 每次下一期预测锁定后自动上传
- Render重部署/重启后启动时自动下载并恢复

Supabase Free当前提供：
- $0/月
- 1GB Storage
- 500MB数据库
- 5GB egress

需要在Supabase建立一个Private bucket：
sanfen-backup

Render环境变量：
SUPABASE_URL
SUPABASE_SERVICE_ROLE_KEY
SUPABASE_BUCKET=sanfen-backup
SUPABASE_OBJECT=sanfen_ai_checkpoint.json.gz

service_role key只能放Render环境变量，不要发到聊天，不要写进GitHub。

备份内容：
- prediction_log
- AI权重
- learner_scores
- 最近1400期开奖
- 60期验证相关记录

页面成功后会显示：
学习数据：Supabase免费外部备份

/api/backup-status 可看到远程备份时间和错误状态。

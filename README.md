# v28F1 免费 Supabase 外部备份修正版

修复：
1. iPhone 复制 Secret key 时夹带换行/空格，程序会自动清理。
2. Supabase 新版 `sb_secret_...` 不是 JWT，只使用 `apikey` header，不再发送为 Bearer。
3. 私有 Storage 恢复使用服务端对象接口。

继续沿用环境变量：
SUPABASE_SERVICE_ROLE_KEY

VALUE 直接填 Supabase 的 `sb_secret_...` Secret key。

其余功能保持：
- Render Free
- Supabase Free Storage
- 每期自动上传 AI 检查点
- 重部署后自动恢复
- prediction_log / AI权重 / learner_scores / 最近开奖记录
- 多任务 AI 训练

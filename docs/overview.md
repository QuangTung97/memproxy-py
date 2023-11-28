# Architecture Overview
This library developed using the **Cache-Aside Pattern**, with basic steps:
1. Data is first get from Cache Server (Redis)
2. If Data is Existed on the Cache => Returns to Client
3. If Data is NOT Existed => Get From DB => Set Back to Redis => Returns to Client

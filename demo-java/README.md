     * 目前這個方法可能的結果有以下幾種:
     * 1. 成功寫入 Remote Cache 並通知刪除所有 Local Cache
     * 2. 序列化失敗，拋出 KacheSerializeException
     *   2.1 修復完成前，如果 Caches 尚未過期，查詢會返回舊資料
     *   2.2 修復完成前，如果 Caches 已過期，拿到互斥鎖的查詢會從 Upstream 載入資料，但因為遇到序列化失敗，導致查詢會返回空值
     *   2.3 修復完成前，如果 Caches 已過期，沒拿到互斥鎖的查詢會返回空值
     * 3. 寫入 Remote Cache 失敗，拋出 KacheRemoteCacheOperateException
     *   3.1 如果 Remote Cache 只是暫時異常，這將導致短時間內資料不同步
     *   3.2 如果 Remote Cache 長時間異常，如果 Local Cache 尚未過期，查詢會返回舊資料
     *   3.3 如果 Remote Cache 長時間異常，如果 Local Cache 已過期，目前的互斥鎖機制相依 Remote Cache，將會因為無法取得互斥所導致查詢返回空值
     * 4. 寫入 Remote Cache 成功，但通知刪除 Local Cache 失敗，拋出 KacheLocalCacheOperateException
     *   4.1 Remote Cache 寫入正常代表錯誤只是暫時異常，這將導致短時間內資料不同步
     *
     * XXX : 目前互斥鎖的實作會導致 Remote Cache 與 Upstream 生命週期相依，這是我們的取捨，之後可以做成可配置的選項

     * 目前這個方法可能的結果有以下幾種:
     * 1. Local Cache 命中，回傳資料
     * 2. Local Cache 未命中，Remote Cache 命中，回傳資料並更新 Local Cache
     * 3. Local Cache 未命中，Remote Cache 未命中，拿到互斥鎖，Upstream 有資料，回傳資料並更新 Remote 然後通知刪除所有 Local Cache
     * 4. Local Cache 未命中，Remote Cache 未命中，拿到互斥鎖，Upstream 有資料，但序列化或是寫入 Remote Cache 失敗，回傳 Optional.empty()
     * 4. Local Cache 未命中，Remote Cache 未命中，拿到互斥鎖，Upstream 有資料，但通知刪除 Local Cache 失敗，回傳資料
     * 5. Local Cache 未命中，Remote Cache 未命中，拿到互斥鎖，Upstream 無資料，回傳 Optional.empty()
     * 6. Local Cache 未命中，Remote Cache 未命中，沒拿到互斥鎖，回傳 Optional.empty()
     * 7. Local Cache 未命中，Remote Cache 讀取失敗
     *   7.1 如果 Remote Cache 只是暫時異常 -> 3. 4. 5. 6. 皆有可能
     * 8. Local Cache 未命中，Remote Cache 讀取失敗
     *   8.1 如果 Remote Cache 長時間異常，目前的互斥鎖機制相依 Remote Cache，將會因為無法取得互斥所導致查詢返回空值
     *
     * 在拿不到互斥鎖的情況下，目前會直接回傳空，可以考慮加上重試機制以提升命中率

bloom filter
metrics
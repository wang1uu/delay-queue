package cc.wang1.queue;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 消息实体
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Item<T> {
    /**
     * 消息id
     */
    private long itemId;
    /**
     * 到期时间
     */
    private long expiry;
    /**
     * 消息数据
     */
    private T data;

    public Item<T> itemId(long itemId) {
        this.itemId = itemId;
        return this;
    }

    public Item<T> expiry(long expiry) {
        this.expiry = Math.max(0, expiry);
        return this;
    }

    public Item<T> data(T data) {
        this.data = data;
        return this;
    }
}

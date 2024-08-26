package cc.wang1.queue;

import cc.wang1.adapter.RedisClientAdapter;

import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

/**
 * 延迟队列
 * @author wang1
 */
public class DelayQueue<T> {
    /**
     * Redis客户端
     */
    private RedisClientAdapter<T> redisClient;
    /**
     * 消息id生成器
     */
    private Supplier<Long> itemIdGenerator;
    /**
     * 延迟队列名称
     */
    private String name;
    private String delayQueueName;
    private String expiryQueueName;
    /**
     * 到期消息转移线程
     */
    private final Executor executor = new ThreadPoolExecutor(1, 1, Integer.MAX_VALUE, TimeUnit.DAYS,
            new SynchronousQueue<>(),
            r -> {
                Thread thread = new Thread(r);
                thread.setDaemon(false);
                thread.setName(name + "_EXECUTOR");
                transferThread = thread;
                return thread;
            },
            new ThreadPoolExecutor.AbortPolicy());;
    private volatile Thread transferThread;
    /**
     * 到期消息转移Lua脚本
     */
    private static final String TRANSFER_SCRIPT =       "local zset_key = KEYS[1] " +
                                                        "local list_key = KEYS[2] " +
                                                        "local T = tonumber(ARGV[1]) " +
                                                        "while T < T + 1000 do " +
                                                        "local elements = redis.call('ZRANGEBYSCORE', zset_key, '-inf', T, 'WITHSCORES') " +
                                                        "if #elements > 0 then " +
                                                        "local num_elements = #elements / 2 " +
                                                        "for i = 1, #elements, 2 do " +
                                                        "local removed = redis.call('ZREM', zset_key, elements[i]) " +
                                                        "if removed == 1 then " +
                                                        "redis.call('RPUSH', list_key, elements[i]) " +
                                                        "end " +
                                                        "end " +
                                                        "T = T + (num_elements * 5) " +
                                                        "else " +
                                                        "break " +
                                                        "end " +
                                                        "end " +
                                                        "local first_element = redis.call('ZRANGE', zset_key, 0, 0, 'WITHSCORES') " +
                                                        "if #first_element > 0 then " +
                                                        "return tonumber(first_element[2]) " +
                                                        "else " +
                                                        "return -1 " +
                                                        "end";

    private DelayQueue(){}
    public static class Builder<T> {
        private String name;
        private RedisClientAdapter<T> redisClient;
        private Supplier<Long> itemIdGenerator;

        public Builder<T> name(String name) {
            this.name = name;
            return this;
        }

        public Builder<T> redisClient(RedisClientAdapter<T> redisClient) {
            this.redisClient = redisClient;
            return this;
        }

        public Builder<T> itemIdGenerator(Supplier<Long> itemIdGenerator) {
            this.itemIdGenerator = itemIdGenerator;
            return this;
        }

        public DelayQueue<T> build() {
            if (name == null || name.isEmpty()) {
                throw new RuntimeException("The delay queue name is required.");
            }
            if (redisClient == null) {
                throw new RuntimeException("The redisClient is required.");
            }
            if (itemIdGenerator == null) {
                throw new RuntimeException("The itemIdGenerator is required.");
            }
            DelayQueue<T> delayQueue = new DelayQueue<>();
            delayQueue.name = name;
            delayQueue.delayQueueName = "DELAY_QUEUE_" + name;
            delayQueue.expiryQueueName = "EXPIRY_QUEUE_" + name;
            delayQueue.redisClient = redisClient;
            delayQueue.itemIdGenerator = itemIdGenerator;

            // 最后开启
            delayQueue.executor.execute(delayQueue::startTransfer);
            return delayQueue;
        }
    }

    /**
     * 新增
     * @author wang1
     * @param id 消息id
     * @param expiry 到期时间
     * @param data 消息内容
     */
    public boolean offer(long id, long expiry, T data) {
        Item<T> item = new Item<T>().itemId(id).expiry(expiry).data(data);

        boolean result = redisClient.zAdd(delayQueueName, expiry, item);
        if (!result) {
            return false;
        }

        // 添加成功
        // 检查当前队头元素是否有变化
        Item<T> first = redisClient.zFirst(delayQueueName);
        if (transferThread != null
                && transferThread.isAlive()
                && first != null
                && first.getItemId() == id) {
            LockSupport.unpark(transferThread);
            return true;
        }

        return true;
    }
    public boolean offer(long expiry, T data) {
        return offer(itemIdGenerator.get(), expiry, data);
    }

    /**
     * 获取
     * @author wang1
     * @param timeout 超时时间
     * @param timeUnit 时间单位
     * @return Item
     */
    public Item<T> poll(long timeout, TimeUnit timeUnit) {
        return redisClient.lPoll(expiryQueueName, timeout, timeUnit);
    }


    /**
     * 转移
     * @author wang1
     */
    private void startTransfer() {
        while (true) {
            try {
                // 转移到期元素
                // 获取下一个最近到期的元素
                long timeout = redisClient.executeTransferScript(TRANSFER_SCRIPT, Arrays.asList(delayQueueName, expiryQueueName), System.currentTimeMillis());
                // 延迟消息队列为空
                if (timeout < 0) {
                    LockSupport.park(this);
                }
                // park到下一个最近到期的元素
                if (timeout > 0) {
                    LockSupport.parkNanos(this, TimeUnit.MILLISECONDS.toNanos(Math.max(0, System.currentTimeMillis() - timeout)));
                }
            }catch (Exception e) {
                e.printStackTrace();
                LockSupport.parkNanos(this, TimeUnit.SECONDS.toNanos(1));
            }
        }
    }
}

package cc.wang1;

import cc.wang1.adapter.RedissionClientAdapter;
import cc.wang1.queue.DelayQueue;
import cc.wang1.queue.Item;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class SampleDelayQueueApplicationTests {

    private static final String TRANSFER_SCRIPT =
                    "local zset_key = KEYS[1] " +
                    "local list_key = KEYS[2] " +
                    "local T = tonumber(ARGV[1]) " +
                    "local increment = tonumber(ARGV[2]) " +
                    "local limit = T + 1000 " +
                    "while T < limit do " +
                    "    local elements = redis.call('ZRANGEBYSCORE', zset_key, '-inf', T, 'WITHSCORES') " +
                    "    if #elements <= 0 then " +
                    "        break " +
                    "    end " +
                    "    for i = 1, #elements, 2 do " +
                    "        local removed = redis.call('ZREM', zset_key, elements[i]) " +
                    "        if removed == 1 then " +
                    "            redis.call('RPUSH', list_key, elements[i]) " +
                    "        end " +
                    "        T = T + increment " +
                    "        if T >= limit then " +
                    "            break " +
                    "        end " +
                    "    end " +
                    "end " +
                    "local first_element = redis.call('ZRANGE', zset_key, 0, 0, 'WITHSCORES') " +
                    "if #first_element > 0 then " +
                    "    return tonumber(first_element[2]) " +
                    "else " +
                    "    return -1 " +
                    "end";

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissionClientAdapter redissionClientAdapter;


    @Test
    public void test01() throws InterruptedException {
        DelayQueue.Builder<String> builder = new DelayQueue.Builder<>();
        DelayQueue<String> test20240819 = builder.redisClient(redissionClientAdapter)
                .name("test20240819")
                .itemIdGenerator(System::currentTimeMillis)
                .build();

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        test20240819.offer(123123L, System.currentTimeMillis() + 1000L, "测试消息");




        Thread.sleep(TimeUnit.MINUTES.toMillis(10));
    }

    @Test
    public void test02() {
        Item<String> item = redissionClientAdapter.zFirst("DELAY_QUEUE_test20240819");
        System.out.println(item);
    }

    @Test
    public void test03() {
        long timeout = redissionClientAdapter.executeTransferScript(TRANSFER_SCRIPT, Arrays.asList("DELAY_QUEUE_test20240819", "EXPIRY_QUEUE_test20240819"), System.currentTimeMillis());
        System.out.println(timeout);
    }
}

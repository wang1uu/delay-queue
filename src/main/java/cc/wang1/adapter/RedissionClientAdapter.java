package cc.wang1.adapter;

import cc.wang1.queue.Item;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Component
public class RedissionClientAdapter implements RedisClientAdapter<String> {

    @Qualifier(value = "redisClient")
    private final RedisTemplate<String, Item<String>> redisClient;

    @Autowired
    public RedissionClientAdapter(RedisTemplate<String, Item<String>> redisClient) {
        this.redisClient = redisClient;
    }

    @Override
    public boolean zAdd(String zSet, long score, Item<String> value) {
        return Optional.ofNullable(redisClient.opsForZSet().add(zSet, value, (double) score)).orElse(false);
    }

    @Override
    public Item<String> zFirst(String zSet) {
        Set<Item<String>> items = redisClient.opsForZSet().range(zSet, 0, 0);
        if (items == null || items.isEmpty()) {
            return null;
        }
        return items.iterator().next();
    }

    @Override
    public Item<String> lPoll(String zSet, long timeout, TimeUnit timeUnit) {
        return timeout < 0 ? redisClient.opsForList().leftPop(zSet) : redisClient.opsForList().leftPop(zSet, timeout, timeUnit);
    }

    @Override
    public long executeTransferScript(String script, List<String> keys, Object... args) {
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
        redisScript.setScriptText(script);
        redisScript.setResultType(Long.class);

        return Optional.ofNullable(redisClient.execute(redisScript, keys, args)).orElse(System.currentTimeMillis() + 1000L);
    }
}

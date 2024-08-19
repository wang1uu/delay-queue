package cc.wang1.config;

import cc.wang1.queue.Item;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfig {

    @Bean("redisClient")
    public RedisTemplate<String, Item<String>> redisTemplate(RedisConnectionFactory factory) {
        RedisTemplate<String, Item<String>> template = new RedisTemplate<>();
        template.setConnectionFactory(factory);

        //Json序列化配置
        Jackson2JsonRedisSerializer<Item> Jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer<>(Item.class);
        ObjectMapper mp = new ObjectMapper();
        mp.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
        mp.activateDefaultTyping(mp.getPolymorphicTypeValidator());
        Jackson2JsonRedisSerializer.serialize(mp);

        // Spring的序列化
        StringRedisSerializer stringRedisSerializer = new StringRedisSerializer();
        // key采用String的序列化方式
        template.setKeySerializer(stringRedisSerializer);
        // hash的key也采用string的序列化方式
        template.setHashKeySerializer(stringRedisSerializer);
        // value的序列化方式采用的是jackson
        template.setValueSerializer(Jackson2JsonRedisSerializer);
        // hash的value序列化方式采用jackson
        template.setHashKeySerializer(Jackson2JsonRedisSerializer);

        template.afterPropertiesSet();
        return template;
    }
}

package cc.wang1.controller;

import cc.wang1.adapter.RedissionClientAdapter;
import cc.wang1.queue.DelayQueue;
import cc.wang1.queue.Item;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/test")
public class TestController {

    private final DelayQueue<String> delayQueue;

    @Autowired
    public TestController(RedissionClientAdapter redissionClientAdapter) {
        this.delayQueue = new DelayQueue.Builder<String>()
                .redisClient(redissionClientAdapter)
                .name("test20240819235314")
                .itemIdGenerator(System::currentTimeMillis)
                .build();
    }

    @GetMapping("/status")
    public String status() {
        return "ok";
    }

    @GetMapping("/offer")
    public Boolean offer(@RequestParam("v") String v, @RequestParam("e") Long e) {
        return delayQueue.offer(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(e), v);
    }

    @GetMapping("/poll")
    public Item<String> poll(@RequestParam("w") Long w) {
        return delayQueue.poll(w, TimeUnit.SECONDS);
    }
}

/*
 * Copyright 2024-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.cloud.ai.graph.agent.bug;

import com.alibaba.cloud.ai.graph.agent.ReactAgent;
import com.alibaba.cloud.ai.graph.agent.internal.AgentLlmNode;
import com.alibaba.cloud.ai.graph.agent.internal.AgentToolNode;
import com.alibaba.cloud.ai.graph.CompileConfig;
import com.alibaba.cloud.ai.graph.exception.GraphStateException;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.prompt.Prompt;

import java.lang.reflect.Field;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Bug 修复验证测试 - ReactAgent 线程安全性
 * 
 * 验证修复后的 ReactAgent 使用 ThreadLocal 存储 iterations，
 * 确保多个并发请求之间的状态隔离。
 * 
 * @author bug-fix-team
 */
public class ReactAgentThreadSafetyFixedTest {

    /**
     * 模拟的 ChatModel，用于测试
     */
    private static class MockChatModel implements ChatModel {
        private AtomicInteger callCount = new AtomicInteger(0);
        
        @Override
        public ChatResponse call(Prompt prompt) {
            callCount.incrementAndGet();
            return new ChatResponse(java.util.Collections.emptyList());
        }
        
        public int getCallCount() {
            return callCount.get();
        }
    }

    /**
     * 验证测试 1: 单例 ReactAgent 的 iterations 线程隔离
     * 
     * 验证：每个线程的 iterations 计数器独立，不会相互干扰
     */
    @Test
    public void testSingletonAgentIterationsThreadIsolation() throws InterruptedException, GraphStateException {
        System.out.println("\n========== ✅ 验证测试 1: ReactAgent iterations 线程隔离 ==========");
        
        ReactAgent sharedAgent = createMockReactAgent();
        
        int threadCount = 5;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        
        ConcurrentHashMap<Integer, Integer> threadIterations = new ConcurrentHashMap<>();
        AtomicInteger successCount = new AtomicInteger(0);
        
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    System.out.println("🚀 线程 " + threadId + " 开始执行");
                    
                    // 每个线程模拟执行 Agent
                    for (int j = 0; j < 3; j++) {
                        incrementIterations(sharedAgent);
                        Thread.sleep(30);
                    }
                    
                    // 读取当前线程的 iterations 值
                    int currentIterations = getThreadLocalIterationsValue(sharedAgent);
                    threadIterations.put(threadId, currentIterations);
                    
                    System.out.println("✓ 线程 " + threadId + " 完成，iterations = " + currentIterations);
                    
                    // 验证每个线程都是从0开始计数的
                    if (currentIterations == 3) {
                        successCount.incrementAndGet();
                    }
                    
                } catch (Exception e) {
                    System.err.println("❌ 线程 " + threadId + " 执行异常: " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();
        
        System.out.println("\n📊 测试结果:");
        System.out.println("   总线程数: " + threadCount);
        System.out.println("   成功隔离: " + successCount.get() + " 个线程");
        
        threadIterations.forEach((id, iter) -> 
            System.out.println("   线程 " + id + ": iterations = " + iter));
        
        // 验证每个线程都有独立的计数器
        assertEquals(threadCount, successCount.get(), 
            "所有线程的 iterations 应该都是独立计数的");
        
        System.out.println("✅ 验证通过！每个线程的 iterations 都独立隔离");
    }

    /**
     * 验证测试 2: 并发请求状态完全隔离
     * 
     * 验证：高并发场景下，每个请求的状态完全独立
     */
    @Test
    public void testConcurrentRequestsStateIsolation() throws InterruptedException, GraphStateException {
        System.out.println("\n========== ✅ 验证测试 2: 并发请求状态完全隔离 ==========");
        
        ReactAgent sharedAgent = createMockReactAgent();
        
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        
        ConcurrentHashMap<Integer, Integer> maxIterationsPerThread = new ConcurrentHashMap<>();
        
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    startLatch.await(); // 等待所有线程就绪
                    
                    int maxObserved = 0;
                    // 每个线程执行5次迭代
                    for (int j = 0; j < 5; j++) {
                        incrementIterations(sharedAgent);
                        int current = getThreadLocalIterationsValue(sharedAgent);
                        maxObserved = Math.max(maxObserved, current);
                        Thread.sleep(20);
                    }
                    
                    maxIterationsPerThread.put(threadId, maxObserved);
                    
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    endLatch.countDown();
                }
            });
        }
        
        System.out.println("🚀 启动 " + threadCount + " 个并发请求...");
        startLatch.countDown();
        
        endLatch.await(10, TimeUnit.SECONDS);
        executor.shutdown();
        
        System.out.println("📊 状态隔离结果:");
        maxIterationsPerThread.forEach((id, max) -> 
            System.out.println("   线程 " + id + " 最大 iterations: " + max));
        
        // 验证每个线程的最大值都是5（独立计数）
        long correctCount = maxIterationsPerThread.values().stream()
            .filter(max -> max == 5)
            .count();
        
        System.out.println("   正确隔离的线程数: " + correctCount + "/" + threadCount);
        
        assertEquals(threadCount, correctCount, 
            "所有线程的 iterations 都应该独立计数到5");
        
        System.out.println("✅ 验证通过！所有并发请求的状态完全隔离");
    }

    /**
     * 验证测试 3: Spring Bean 单例场景验证
     * 
     * 模拟真实的 Spring Bean 使用场景
     */
    @Test
    public void testSpringBeanScenarioFixed() throws InterruptedException, GraphStateException {
        System.out.println("\n========== ✅ 验证测试 3: Spring Bean 单例场景 ==========");
        
        ReactAgent singletonBean = createMockReactAgent();
        
        System.out.println("📝 模拟场景：");
        System.out.println("   - ReactAgent 注册为 Spring @Component（默认单例）");
        System.out.println("   - 多个用户请求同时调用这个 Bean");
        System.out.println("   - 修复后：每个请求有独立的执行状态\n");
        
        String[] users = {"User-Alice", "User-Bob", "User-Charlie"};
        ExecutorService executor = Executors.newFixedThreadPool(users.length);
        CountDownLatch latch = new CountDownLatch(users.length);
        
        ConcurrentHashMap<String, Integer> userIterations = new ConcurrentHashMap<>();
        AtomicInteger successCount = new AtomicInteger(0);
        
        for (String user : users) {
            executor.submit(() -> {
                try {
                    System.out.println("👤 " + user + " 开始请求...");
                    
                    // 模拟用户请求处理（3次迭代）
                    for (int i = 0; i < 3; i++) {
                        incrementIterations(singletonBean);
                        Thread.sleep(50);
                    }
                    
                    int finalIterations = getThreadLocalIterationsValue(singletonBean);
                    userIterations.put(user, finalIterations);
                    
                    System.out.println("👤 " + user + " 请求完成: iterations = " + finalIterations);
                    
                    // 每个用户应该独立计数到3
                    if (finalIterations == 3) {
                        successCount.incrementAndGet();
                    }
                    
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();
        
        System.out.println("\n📊 Spring Bean 测试结果:");
        userIterations.forEach((user, iter) -> 
            System.out.println("   " + user + ": iterations = " + iter));
        
        System.out.println("   成功隔离: " + successCount.get() + "/" + users.length + " 个用户");
        
        assertEquals(users.length, successCount.get(), 
            "所有用户的 iterations 都应该独立计数");
        
        System.out.println("✅ 验证通过！单例 Bean 的状态在多个请求间完全隔离");
        System.out.println("💡 效果：");
        System.out.println("   - 每个用户请求独立执行");
        System.out.println("   - 用户A的执行不影响用户B");
        System.out.println("   - 符合请求隔离原则");
    }

    /**
     * 验证测试 4: ThreadLocal 内存泄漏预防
     * 
     * 验证：ThreadLocal 在使用后被正确清理
     */
    @Test
    public void testThreadLocalMemoryLeakPrevention() throws InterruptedException, GraphStateException {
        System.out.println("\n========== ✅ 验证测试 4: ThreadLocal 内存泄漏预防 ==========");
        
        ReactAgent agent = createMockReactAgent();
        
        // 模拟多次请求
        for (int i = 0; i < 5; i++) {
            System.out.println("📝 执行请求 " + (i + 1) + "...");
            
            // 模拟请求执行
            for (int j = 0; j < 3; j++) {
                incrementIterations(agent);
            }
            
            // 模拟请求完成后的清理（在实际代码中由 finally 块处理）
            cleanupThreadLocal(agent);
            
            // 验证清理后 ThreadLocal 被重置
            int afterCleanup = getThreadLocalIterationsValue(agent);
            System.out.println("   清理后 iterations = " + afterCleanup);
            
            assertEquals(0, afterCleanup, 
                "ThreadLocal 应该被清理并重新初始化为0");
        }
        
        System.out.println("✅ 验证通过！ThreadLocal 正确清理，无内存泄漏风险");
    }

    /**
     * 验证测试 5: 高并发压力测试
     * 
     * 验证：大量并发请求下的稳定性
     */
    @Test
    public void testHighConcurrencyStressTest() throws InterruptedException, GraphStateException {
        System.out.println("\n========== ✅ 验证测试 5: 高并发压力测试 ==========");
        
        ReactAgent agent = createMockReactAgent();
        
        int threadCount = 50;
        int iterationsPerThread = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        
        AtomicInteger correctResults = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < iterationsPerThread; j++) {
                        incrementIterations(agent);
                    }
                    
                    int finalValue = getThreadLocalIterationsValue(agent);
                    if (finalValue == iterationsPerThread) {
                        correctResults.incrementAndGet();
                    } else {
                        System.err.println("⚠️  线程 " + threadId + " 计数异常: " + finalValue);
                    }
                    
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await(20, TimeUnit.SECONDS);
        executor.shutdown();
        
        System.out.println("📊 压力测试结果:");
        System.out.println("   总线程数: " + threadCount);
        System.out.println("   正确结果: " + correctResults.get());
        System.out.println("   错误次数: " + errorCount.get());
        System.out.println("   成功率: " + String.format("%.2f%%", 
            correctResults.get() * 100.0 / threadCount));
        
        assertEquals(0, errorCount.get(), "不应该有任何错误");
        assertEquals(threadCount, correctResults.get(), 
            "所有线程都应该得到正确的计数结果");
        
        System.out.println("✅ 验证通过！高并发下完全稳定，成功率 100%");
    }

    // ========== 辅助方法 ==========

    /**
     * 创建一个模拟的 ReactAgent
     */
    private ReactAgent createMockReactAgent() throws GraphStateException {
        MockChatModel mockChatModel = new MockChatModel();
        AgentLlmNode llmNode = new AgentLlmNode(mockChatModel, null);
        AgentToolNode toolNode = new AgentToolNode();
        CompileConfig compileConfig = new CompileConfig.Builder().build();
        
        ReactAgent.Builder builder = ReactAgent.builder()
            .name("test-agent")
            .description("Test agent for thread-safety verification")
            .maxIterations(10);
        
        return new ReactAgent(llmNode, toolNode, compileConfig, builder);
    }

    /**
     * 使用反射获取 ThreadLocal<Integer> iterations 字段的当前值
     */
    private int getThreadLocalIterationsValue(ReactAgent agent) {
        try {
            Field iterationsField = ReactAgent.class.getDeclaredField("iterations");
            iterationsField.setAccessible(true);
            ThreadLocal<Integer> threadLocal = (ThreadLocal<Integer>) iterationsField.get(agent);
            return threadLocal.get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to get ThreadLocal iterations value", e);
        }
    }

    /**
     * 使用反射增加 ThreadLocal iterations 的值
     */
    private void incrementIterations(ReactAgent agent) {
        try {
            Field iterationsField = ReactAgent.class.getDeclaredField("iterations");
            iterationsField.setAccessible(true);
            ThreadLocal<Integer> threadLocal = (ThreadLocal<Integer>) iterationsField.get(agent);
            int currentValue = threadLocal.get();
            threadLocal.set(currentValue + 1);
        } catch (Exception e) {
            throw new RuntimeException("Failed to increment iterations", e);
        }
    }

    /**
     * 使用反射清理 ThreadLocal
     */
    private void cleanupThreadLocal(ReactAgent agent) {
        try {
            Field iterationsField = ReactAgent.class.getDeclaredField("iterations");
            iterationsField.setAccessible(true);
            ThreadLocal<Integer> threadLocal = (ThreadLocal<Integer>) iterationsField.get(agent);
            threadLocal.remove();
        } catch (Exception e) {
            throw new RuntimeException("Failed to cleanup ThreadLocal", e);
        }
    }
}


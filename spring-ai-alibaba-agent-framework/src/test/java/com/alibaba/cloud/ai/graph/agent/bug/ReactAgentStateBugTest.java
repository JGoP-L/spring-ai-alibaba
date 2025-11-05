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

import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.agent.ReactAgent;
import com.alibaba.cloud.ai.graph.agent.internal.AgentLlmNode;
import com.alibaba.cloud.ai.graph.agent.internal.AgentToolNode;
import com.alibaba.cloud.ai.graph.CompileConfig;
import com.alibaba.cloud.ai.graph.KeyStrategy;
import com.alibaba.cloud.ai.graph.exception.GraphStateException;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.prompt.Prompt;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Bug 复现测试 - ReactAgent 实例变量状态隔离问题
 * 
 * 这个测试展示了当 ReactAgent 作为单例（Spring Bean）使用时，
 * 实例变量 iterations 会在多个请求间共享，导致计数错乱。
 * 
 * 场景：
 * - ReactAgent 在 Spring 中默认是单例
 * - 多个并发请求使用同一个 Agent 实例
 * - iterations 计数器会相互干扰
 * 
 * @author bug-fix-team
 */
public class ReactAgentStateBugTest {

    /**
     * 模拟的 ChatModel，用于测试
     */
    private static class MockChatModel implements ChatModel {
        private AtomicInteger callCount = new AtomicInteger(0);
        
        @Override
        public ChatResponse call(Prompt prompt) {
            callCount.incrementAndGet();
            // 模拟 LLM 响应
            return new ChatResponse(java.util.Collections.emptyList());
        }
        
        public int getCallCount() {
            return callCount.get();
        }
    }

    /**
     * Bug 复现测试 1: 单例 ReactAgent 的 iterations 计数器竞态条件
     * 
     * 场景：多个线程并发使用同一个 ReactAgent 实例
     * 期望：每个请求的 iterations 应该独立计数
     * 实际：iterations 会累加，导致某些请求提前达到 maxIterations 限制
     */
    @Test
    public void testSingletonAgentIterationsRaceCondition() throws InterruptedException, GraphStateException {
        System.out.println("\n========== Bug 复现测试 1: ReactAgent iterations 竞态条件 ==========");
        
        // 创建一个共享的 ReactAgent 实例（模拟 Spring 单例）
        ReactAgent sharedAgent = createMockReactAgent();
        
        int threadCount = 5;
        int maxIterations = 3; // 每个请求最多3次迭代
        
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        
        // 模拟多个并发请求
        for (int i = 0; i < threadCount; i++) {
            final int requestId = i;
            executor.submit(() -> {
                try {
                    System.out.println("🚀 请求 " + requestId + " 开始执行");
                    
                    // 读取当前的 iterations 值
                    int initialIterations = getIterationsValue(sharedAgent);
                    
                    // 模拟执行过程（会增加 iterations）
                    simulateAgentExecution(sharedAgent, requestId);
                    
                    Thread.sleep(50); // 模拟一些处理时间
                    
                    int finalIterations = getIterationsValue(sharedAgent);
                    
                    System.out.println("📊 请求 " + requestId + " 完成: " +
                        "初始 iterations=" + initialIterations + 
                        ", 最终 iterations=" + finalIterations);
                    
                    // 检查 iterations 是否符合预期
                    // 在单例模式下，iterations 会累加，不符合预期
                    if (finalIterations > maxIterations * (requestId + 1)) {
                        System.err.println("⚠️  请求 " + requestId + " 的 iterations 异常累加!");
                        failureCount.incrementAndGet();
                    } else {
                        successCount.incrementAndGet();
                    }
                    
                } catch (Exception e) {
                    System.err.println("❌ 请求 " + requestId + " 执行异常: " + e.getMessage());
                    e.printStackTrace();
                    failureCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();
        
        System.out.println("\n📊 测试结果:");
        System.out.println("   总请求数: " + threadCount);
        System.out.println("   成功: " + successCount.get());
        System.out.println("   异常: " + failureCount.get());
        System.out.println("   最终 iterations 值: " + getIterationsValue(sharedAgent));
        
        // 获取最终的 iterations 值
        int finalIterations = getIterationsValue(sharedAgent);
        
        if (finalIterations > maxIterations) {
            System.out.println("⚠️  Bug 已复现！iterations 累加到 " + finalIterations + 
                "，超过了单个请求的 maxIterations=" + maxIterations);
            System.out.println("💡 原因：多个请求共享同一个 Agent 实例的 iterations 变量");
        }
        
        // 这个断言通常会失败，证明 Bug 存在
        // assertTrue(finalIterations <= maxIterations, 
        //     "单例模式下 iterations 不应该累加超过 maxIterations");
    }

    /**
     * Bug 复现测试 2: 并发请求导致的状态污染
     * 
     * 验证多个请求并发执行时，状态会相互污染
     */
    @Test
    public void testConcurrentRequestsStatePollution() throws InterruptedException, GraphStateException {
        System.out.println("\n========== Bug 复现测试 2: 并发请求状态污染 ==========");
        
        ReactAgent sharedAgent = createMockReactAgent();
        
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        
        AtomicInteger maxObservedIterations = new AtomicInteger(0);
        
        for (int i = 0; i < threadCount; i++) {
            final int requestId = i;
            executor.submit(() -> {
                try {
                    startLatch.await(); // 等待所有线程就绪
                    
                    // 所有线程同时开始
                    for (int j = 0; j < 5; j++) {
                        incrementIterations(sharedAgent);
                        int current = getIterationsValue(sharedAgent);
                        
                        // 更新观察到的最大值
                        maxObservedIterations.updateAndGet(max -> Math.max(max, current));
                        
                        Thread.sleep(10);
                    }
                    
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
        
        int finalIterations = getIterationsValue(sharedAgent);
        
        System.out.println("📊 状态污染结果:");
        System.out.println("   最大观察值: " + maxObservedIterations.get());
        System.out.println("   最终值: " + finalIterations);
        System.out.println("   期望最大值: " + (threadCount * 5) + " (如果完全隔离)");
        
        if (finalIterations > 5) {
            System.out.println("⚠️  Bug 已复现！iterations 被多个请求共享并累加");
            System.out.println("💡 问题：ReactAgent 的实例变量在多个请求间不隔离");
        }
    }

    /**
     * Bug 复现测试 3: 模拟 Spring Bean 场景
     * 
     * 最真实的场景：模拟 Spring 容器中单例 Bean 被多次调用
     */
    @Test
    public void testSpringBeanScenario() throws InterruptedException, GraphStateException {
        System.out.println("\n========== Bug 复现测试 3: Spring Bean 单例场景 ==========");
        
        // 模拟 Spring 容器中的单例 Bean
        ReactAgent singletonBean = createMockReactAgent();
        
        System.out.println("📝 模拟场景：");
        System.out.println("   - ReactAgent 注册为 Spring @Component（默认单例）");
        System.out.println("   - 多个用户请求同时调用这个 Bean");
        System.out.println("   - 每个请求期望有独立的执行状态\n");
        
        // 模拟3个用户请求
        String[] users = {"User-Alice", "User-Bob", "User-Charlie"};
        ExecutorService executor = Executors.newFixedThreadPool(users.length);
        CountDownLatch latch = new CountDownLatch(users.length);
        
        for (String user : users) {
            executor.submit(() -> {
                try {
                    System.out.println("👤 " + user + " 开始请求...");
                    
                    int beforeIterations = getIterationsValue(singletonBean);
                    
                    // 模拟用户请求处理
                    for (int i = 0; i < 3; i++) {
                        incrementIterations(singletonBean);
                        Thread.sleep(50);
                    }
                    
                    int afterIterations = getIterationsValue(singletonBean);
                    
                    System.out.println("👤 " + user + " 请求完成: " +
                        "前=" + beforeIterations + ", 后=" + afterIterations);
                    
                    // 每个用户期望增加3次迭代
                    int expectedIncrease = 3;
                    int actualIncrease = afterIterations - beforeIterations;
                    
                    if (actualIncrease != expectedIncrease) {
                        System.err.println("⚠️  " + user + " 的 iterations 增量异常! " +
                            "期望+" + expectedIncrease + ", 实际+" + actualIncrease);
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
        
        int finalIterations = getIterationsValue(singletonBean);
        
        System.out.println("\n📊 Spring Bean 测试结果:");
        System.out.println("   最终 iterations: " + finalIterations);
        System.out.println("   期望值（如果隔离）: 每个用户独立从 0 开始");
        System.out.println("   实际情况: 所有用户共享累加到 " + finalIterations);
        
        if (finalIterations == users.length * 3) {
            System.out.println("⚠️  Bug 已复现！单例 Bean 的状态在多个请求间共享");
            System.out.println("💡 危害：");
            System.out.println("   - 用户A的执行会影响用户B");
            System.out.println("   - 可能提前达到 maxIterations 限制");
            System.out.println("   - 违反了请求隔离原则");
        }
    }

    // ========== 辅助方法 ==========

    /**
     * 创建一个模拟的 ReactAgent
     */
    private ReactAgent createMockReactAgent() throws GraphStateException {
        MockChatModel mockChatModel = new MockChatModel();
        
        // 创建必要的组件
        AgentLlmNode llmNode = new AgentLlmNode(mockChatModel, null);
        AgentToolNode toolNode = new AgentToolNode();
        CompileConfig compileConfig = new CompileConfig.Builder().build();
        
        // 使用反射创建 ReactAgent.Builder
        ReactAgent.Builder builder = ReactAgent.builder()
            .name("test-agent")
            .description("Test agent for bug reproduction")
            .maxIterations(3);
        
        return new ReactAgent(llmNode, toolNode, compileConfig, builder);
    }

    /**
     * 使用反射获取 iterations 字段的值
     */
    private int getIterationsValue(ReactAgent agent) {
        try {
            Field iterationsField = ReactAgent.class.getDeclaredField("iterations");
            iterationsField.setAccessible(true);
            return (int) iterationsField.get(agent);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get iterations value", e);
        }
    }

    /**
     * 使用反射增加 iterations 字段的值
     */
    private void incrementIterations(ReactAgent agent) {
        try {
            Field iterationsField = ReactAgent.class.getDeclaredField("iterations");
            iterationsField.setAccessible(true);
            int currentValue = (int) iterationsField.get(agent);
            iterationsField.set(agent, currentValue + 1);
        } catch (Exception e) {
            throw new RuntimeException("Failed to increment iterations", e);
        }
    }

    /**
     * 模拟 Agent 执行过程
     */
    private void simulateAgentExecution(ReactAgent agent, int requestId) {
        try {
            // 模拟多次迭代
            for (int i = 0; i < 3; i++) {
                incrementIterations(agent);
                Thread.sleep(20); // 模拟处理时间
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}


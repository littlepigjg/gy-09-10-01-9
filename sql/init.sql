-- 创建数据库
CREATE DATABASE IF NOT EXISTS ratelimiter DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE ratelimiter;

-- 限流规则表
CREATE TABLE IF NOT EXISTS rate_limit_rules (
    id INT AUTO_INCREMENT PRIMARY KEY,
    path VARCHAR(255) NOT NULL,
    method VARCHAR(10) DEFAULT 'ALL',
    algorithm VARCHAR(50) NOT NULL,
    rate FLOAT NOT NULL,
    burst INT DEFAULT 0,
    window_size INT DEFAULT 60,
    slot_granularity FLOAT DEFAULT 1.0,
    enabled BOOLEAN DEFAULT TRUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_path (path)
) ENGINE=InnoDB;

-- 熔断器状态表
CREATE TABLE IF NOT EXISTS circuit_breaker_states (
    id INT AUTO_INCREMENT PRIMARY KEY,
    service_name VARCHAR(255) NOT NULL UNIQUE,
    backend_url VARCHAR(500) NOT NULL,
    state VARCHAR(20) NOT NULL DEFAULT 'closed',
    failure_count INT DEFAULT 0,
    success_count INT DEFAULT 0,
    failure_threshold INT DEFAULT 5,
    recovery_timeout INT DEFAULT 30,
    half_open_max_calls INT DEFAULT 3,
    success_threshold INT DEFAULT 2,
    last_failure_time DATETIME,
    last_state_change DATETIME DEFAULT CURRENT_TIMESTAMP,
    total_requests INT DEFAULT 0,
    total_failures INT DEFAULT 0,
    enabled BOOLEAN DEFAULT TRUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- 限流事件日志
CREATE TABLE IF NOT EXISTS rate_limit_events (
    id INT AUTO_INCREMENT PRIMARY KEY,
    rule_id INT NULL,
    path VARCHAR(255) NOT NULL,
    client_ip VARCHAR(50),
    algorithm VARCHAR(50),
    allowed BOOLEAN NOT NULL,
    current_rate FLOAT,
    limit_rate FLOAT,
    reason VARCHAR(200),
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    INDEX idx_created (created_at),
    INDEX idx_rule_id (rule_id)
) ENGINE=InnoDB;

-- 滑动窗口参数重估结果表
CREATE TABLE IF NOT EXISTS window_tune_results (
    id INT AUTO_INCREMENT PRIMARY KEY,
    rule_id INT NOT NULL,
    path VARCHAR(255) NOT NULL,
    window_size INT,
    current_slot_granularity FLOAT,
    suggested_window_size INT,
    suggested_slot_granularity FLOAT,
    suggested_slot_count INT,
    sample_count INT DEFAULT 0,
    confidence FLOAT DEFAULT 0,
    status VARCHAR(20) DEFAULT 'ok',
    deviation_pct FLOAT DEFAULT 0,
    detail VARCHAR(500) DEFAULT '',
    evaluated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_rule_evaluated (rule_id, evaluated_at)
) ENGINE=InnoDB;

-- 流量统计表
CREATE TABLE IF NOT EXISTS traffic_stats (
    id INT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    total_requests INT DEFAULT 0,
    allowed_requests INT DEFAULT 0,
    rejected_requests INT DEFAULT 0,
    avg_latency_ms FLOAT DEFAULT 0,
    circuit_breaker_trips INT DEFAULT 0,
    INDEX idx_timestamp (timestamp)
) ENGINE=InnoDB;

-- 插入默认限流规则
INSERT IGNORE INTO rate_limit_rules (path, method, algorithm, rate, burst, window_size, enabled) VALUES
('/api/*', 'ALL', 'token_bucket', 100, 200, 60, TRUE),
('/api/auth/*', 'ALL', 'sliding_window', 20, 30, 60, TRUE),
('/api/upload', 'POST', 'fixed_window', 5, 5, 10, TRUE);

-- 插入默认熔断器配置
INSERT IGNORE INTO circuit_breaker_states (service_name, backend_url, state, failure_threshold, recovery_timeout, enabled) VALUES
('auth-service', 'http://localhost:8081', 'closed', 5, 30, TRUE),
('user-service', 'http://localhost:8082', 'closed', 5, 30, TRUE),
('order-service', 'http://localhost:8083', 'closed', 5, 30, TRUE);

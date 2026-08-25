CREATE TABLE IF NOT EXISTS `notification_attempt` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `notification_id` VARCHAR(64) NOT NULL,
    `attempt_no` INT NOT NULL,
    `result` VARCHAR(32) NOT NULL,
    `fail_reason` VARCHAR(64) NULL,
    `http_status` INT NULL,
    `duration_ms` INT NOT NULL,
    `error_message` VARCHAR(2048) NULL,
    `created_at` DATETIME NOT NULL,
    PRIMARY KEY (`id`),
    KEY `idx_notification_attempt_notification_id` (`notification_id`),
    KEY `idx_notification_attempt` (`notification_id`, `attempt_no`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

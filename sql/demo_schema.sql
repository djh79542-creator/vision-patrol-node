-- 演示用建表脚本：与 patrol/ 里读写的表名、列名一一对应。
-- 生产上这些表由上游投递方维护；本节点只会 UPDATE patrol_job_ledger / patrol_digest_record。
-- 字符集统一 utf8mb4，与代码里 pymysql 的 charset 参数一致。

CREATE DATABASE IF NOT EXISTS vision_patrol
    DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE vision_patrol;

-- ---------------------------------------------------------------- 点位
CREATE TABLE patrol_station (
    id           BIGINT       NOT NULL AUTO_INCREMENT,
    user_id      BIGINT       NOT NULL,
    station_no   VARCHAR(64)  NOT NULL COMMENT '点位编号，队列消息里的 station_no',
    station_name VARCHAR(128) NOT NULL DEFAULT '',
    model_ref    VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '关联 patrol_station_model.model_ref',
    timezone     VARCHAR(64)  NULL     COMMENT '点位时区，IANA 名称，如 Asia/Shanghai',
    status       CHAR(1)      NOT NULL DEFAULT '0' COMMENT '0 启用 1 停用',
    del_flag     CHAR(1)      NOT NULL DEFAULT '0' COMMENT '0 正常 1 已删除',
    PRIMARY KEY (id),
    UNIQUE KEY uk_user_station (user_id, station_no)
);

-- 点位型号：汇总快照用它区分入口级设备（model_code = CAM-M1）
CREATE TABLE patrol_station_model (
    id         BIGINT      NOT NULL AUTO_INCREMENT,
    model_ref  VARCHAR(64) NOT NULL,
    model_code VARCHAR(32) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_model_ref (model_ref)
);

-- ---------------------------------------------------------------- 抓取计划与执行记录
CREATE TABLE patrol_capture_plan (
    id           BIGINT      NOT NULL AUTO_INCREMENT,
    user_id      BIGINT      NOT NULL,
    station_id   BIGINT      NOT NULL COMMENT 'patrol_station.id',
    run_hour     TINYINT     NOT NULL DEFAULT 0,
    run_minute   TINYINT     NOT NULL DEFAULT 0,
    run_units    SMALLINT    NOT NULL DEFAULT 0 COMMENT '单次抓多少帧/路',
    cadence      CHAR(1)     NOT NULL DEFAULT '0' COMMENT '0 按星期 1 每天 2 自定义',
    weekday_mask VARCHAR(32) NOT NULL DEFAULT '' COMMENT '逗号分隔的 isoweekday，如 1,3,5',
    plan_state   CHAR(1)     NOT NULL DEFAULT '0' COMMENT '0 生效中',
    timezone     VARCHAR(64) NULL,
    PRIMARY KEY (id),
    KEY idx_user_plan (user_id, plan_state)
);

CREATE TABLE patrol_capture_log (
    id           BIGINT   NOT NULL AUTO_INCREMENT,
    user_id      BIGINT   NOT NULL,
    station_id   BIGINT   NOT NULL,
    captured_at  DATETIME NOT NULL,
    run_units    SMALLINT NOT NULL DEFAULT 0,
    trigger_kind CHAR(1)  NOT NULL DEFAULT '1' COMMENT '0 手动 1 自动',
    PRIMARY KEY (id),
    KEY idx_user_captured (user_id, captured_at)
);

-- ---------------------------------------------------------------- 规则
CREATE TABLE patrol_rule (
    id        BIGINT      NOT NULL AUTO_INCREMENT,
    rule_code VARCHAR(16) NOT NULL COMMENT '汇总快照只关心 200001 / 200002',
    rule_name VARCHAR(64) NOT NULL DEFAULT '',
    status    CHAR(1)     NOT NULL DEFAULT '0',
    PRIMARY KEY (id),
    UNIQUE KEY uk_rule_code (rule_code)
);

CREATE TABLE patrol_user_rule (
    id       BIGINT  NOT NULL AUTO_INCREMENT,
    user_id  BIGINT  NOT NULL,
    rule_ref BIGINT  NOT NULL COMMENT 'patrol_rule.id',
    status   CHAR(1) NOT NULL DEFAULT '0',
    PRIMARY KEY (id),
    KEY idx_user_rule (user_id, status)
);

-- ---------------------------------------------------------------- 事件
CREATE TABLE patrol_station_event (
    id           BIGINT       NOT NULL AUTO_INCREMENT,
    user_id      BIGINT       NOT NULL,
    station_no   VARCHAR(64)  NOT NULL DEFAULT '',
    station_name VARCHAR(128) NOT NULL DEFAULT '',
    capture_kind VARCHAR(8)   NOT NULL DEFAULT 'idle'
                 COMMENT 'object/action/stock/idle，也兼容数字串 1/2/3/4',
    event_at     DATETIME     NOT NULL,
    headline     VARCHAR(200) NOT NULL DEFAULT '',
    detail       TEXT         NULL,
    media_hint   VARCHAR(255) NOT NULL DEFAULT '',
    PRIMARY KEY (id),
    KEY idx_user_event (user_id, event_at)
);

-- ---------------------------------------------------------------- 作业账本（本节点主表）
-- 由投递方 INSERT，节点只 UPDATE；status 含义见 patrol/brokers/ledger.py
CREATE TABLE patrol_job_ledger (
    id           BIGINT      NOT NULL AUTO_INCREMENT,
    job_ref      VARCHAR(64) NOT NULL COMMENT '与队列消息 job_ref 一致的幂等键',
    station_no   VARCHAR(64) NOT NULL DEFAULT '',
    status       CHAR(1)     NOT NULL DEFAULT '0',
    runner_id    VARCHAR(64) NULL     COMMENT '处理节点标识 <hostname>-<pid>',
    failure_note VARCHAR(500) NULL    COMMENT '失败原因，超 500 字符由节点截断',
    create_time  DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time  DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_job_ref (job_ref),
    -- 补偿扫描按 status=4 + update_time 捞超时作业
    KEY idx_status_update (status, update_time)
);

-- ---------------------------------------------------------------- 周期汇总
-- job_state：0 待处理/重试中，1 快照已采集等工作流，2 完成，9 失败
CREATE TABLE patrol_digest_record (
    id                BIGINT       NOT NULL AUTO_INCREMENT,
    digest_ref        VARCHAR(64)  NOT NULL,
    job_state         CHAR(1)      NOT NULL DEFAULT '0',
    user_id           BIGINT       NOT NULL,
    user_language     VARCHAR(16)  NOT NULL DEFAULT '',
    user_timezone     VARCHAR(64)  NOT NULL DEFAULT '',
    station_tz        VARCHAR(64)  NOT NULL DEFAULT '',
    title             VARCHAR(200) NOT NULL DEFAULT '',
    captured_at       VARCHAR(32)  NOT NULL DEFAULT '',
    period_body       TEXT         NULL,
    rule_body         TEXT         NULL,
    next_day_note     TEXT         NULL,
    ai_suggestions    TEXT         NULL,
    rule_text         TEXT         NULL,
    plan_preview_json LONGTEXT     NULL,
    capture_plan_json LONGTEXT     NULL,
    capture_stats_json LONGTEXT    NULL,
    capture_log_json  LONGTEXT     NULL,
    rule_info_json    LONGTEXT     NULL,
    event_log_json    LONGTEXT     NULL,
    remark            VARCHAR(500) NULL,
    create_time       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_digest_ref (digest_ref),
    KEY idx_user_state (user_id, job_state)
);

-- ---------------------------------------------------------------- 自测数据
-- 让 digest.compose 在没有真实业务库时也能跑出非空快照：
--   xadd JOB_TOPIC，字段 digest_ref=D-001 / user_id=1 / job_kind=digest.compose
INSERT INTO patrol_station (id, user_id, station_no, station_name, model_ref, timezone)
VALUES (1, 1, 'ST-001', '前门', 'REF-M1', 'Asia/Shanghai'),
       (2, 1, 'ST-002', '走廊', 'REF-M1', 'Asia/Shanghai');

INSERT INTO patrol_station_model (model_ref, model_code) VALUES ('REF-M1', 'CAM-A1');

INSERT INTO patrol_capture_plan
    (user_id, station_id, run_hour, run_minute, run_units, cadence, weekday_mask, plan_state, timezone)
VALUES (1, 1, 8, 30, 4, '0', '1,2,3,4,5', '0', 'Asia/Shanghai'),
       (1, 2, 21, 0, 2, '1', '',           '0', 'Asia/Shanghai');

INSERT INTO patrol_capture_log (user_id, station_id, captured_at, run_units, trigger_kind)
VALUES (1, 1, DATE_SUB(NOW(), INTERVAL 3 HOUR), 4, '1'),
       (1, 2, DATE_SUB(NOW(), INTERVAL 1 HOUR), 2, '0');

INSERT INTO patrol_rule (id, rule_code, rule_name) VALUES (1, '200001', '画面变化告警'), (2, '200002', '目标出现告警');
INSERT INTO patrol_user_rule (user_id, rule_ref) VALUES (1, 1), (1, 2);

INSERT INTO patrol_station_event
    (user_id, station_no, station_name, capture_kind, event_at, headline, detail, media_hint)
VALUES (1, 'ST-001', '前门', 'object', DATE_SUB(NOW(), INTERVAL 3 HOUR), '看到一只猫', '画面里出现猫', 'img-1'),
       (1, 'ST-002', '走廊', 'action', DATE_SUB(NOW(), INTERVAL 1 HOUR), '画面有明显变化', '变化占比 12%', 'img-2'),
       (1, 'ST-001', '前门', 'idle',   DATE_SUB(NOW(), INTERVAL 10 MINUTE), '无异常', '', '');

-- ==========================================
-- 0. 【重要】环境清理阶段
-- 目的：删除所有旧表和视图，解决字段名冲突、表已存在报错
-- ==========================================
USE user_behavior;

-- 关闭外键检查以加速删除
SET FOREIGN_KEY_CHECKS = 0;

-- 删除结果表
DROP TABLE IF EXISTS user_behavior_before_filter;
DROP TABLE IF EXISTS pv_uv_puv;
DROP TABLE IF EXISTS retention_rate;
DROP TABLE IF EXISTS date_hour_behavior;
DROP TABLE IF EXISTS behavior_user_num;
DROP TABLE IF EXISTS behavior_num;
DROP TABLE IF EXISTS renhua;
DROP TABLE IF EXISTS path_result;
DROP TABLE IF EXISTS rfm_model;
DROP TABLE IF EXISTS popular_categories;
DROP TABLE IF EXISTS popular_items;
DROP TABLE IF EXISTS popular_cateitems;
DROP TABLE IF EXISTS item_detail;
DROP TABLE IF EXISTS category_detail;
DROP TABLE IF EXISTS temp_path_count;

-- 删除视图
DROP VIEW IF EXISTS user_behavior_view;
DROP VIEW IF EXISTS user_behavior_standard;
DROP VIEW IF EXISTS user_behavior_path;
DROP VIEW IF EXISTS path_count;

-- 删除主表 (如果存在，以便重新创建正确的结构)
DROP TABLE IF EXISTS user_behavior;
DROP TABLE IF EXISTS user_behavior_cleaned;
DROP TABLE IF EXISTS temp_behavior;

-- 恢复外键检查
SET FOREIGN_KEY_CHECKS = 1;

-- 切换到新数据库
USE user_behavior;

-- 验证是否清空成功（应该显示 0 或空）
SHOW TABLES;

-- ==========================================
-- 1. 建表与数据导入
-- ==========================================
-- 1.1 创建数据库 (如果不存在)
CREATE DATABASE IF NOT EXISTS user_behavior DEFAULT CHARACTER SET utf8mb4;
USE user_behavior;

-- 1.2 创建用户行为表 (修正：字段名为 timestamp)
CREATE TABLE user_behavior (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    item_id BIGINT NOT NULL,
    category_id BIGINT NOT NULL,
    behavior_type VARCHAR(20) NOT NULL,
    timestamp BIGINT NOT NULL,
    behavior_time DATETIME,
    INDEX idx_user (user_id),
    INDEX idx_time (behavior_time),
    INDEX idx_behavior (behavior_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- 1.3 导入数据
-- 注意：请确保文件路径正确，且 MySQL 有读取权限
-- 修正：移除 +8*3600，假设原始数据为北京时间戳，服务器也是北京时间
-- 1. 关闭自动提交/唯一检查/外键检查（加速导入）
-- 删除 user_behavior 表的所有索引（导入时不需要索引，导入后再重建）
-- 1. 先删除所有索引（解决超时核心问题）


-- 2. 关闭自动提交/唯一检查/外键检查（加速导入）
SET autocommit = 0;
SET unique_checks = 0;
SET foreign_key_checks = 0;

-- 3. 延长超时时间
SET SESSION net_read_timeout = 600;
SET SESSION net_write_timeout = 600;
SET SESSION wait_timeout = 600;
SET SESSION interactive_timeout = 600;

-- 4. 开启本地文件加载权限
SET GLOBAL local_infile = 1;

SHOW VARIABLES LIKE 'local_infile';

-- 5. 用 LOCAL 模式导入（彻底绕过 secure_file_priv 限制）
LOAD DATA LOCAL INFILE 'E:/MySQL/MySQL Server 8.0/Uploads/UserBehavior_50000users.csv'
INTO TABLE user_behavior
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(user_id, item_id, category_id, behavior_type, timestamp)
SET behavior_time = FROM_UNIXTIME(timestamp);

-- 6. 提交事务 + 恢复安全设置
COMMIT;
SET autocommit = 1;
SET unique_checks = 1;
SET foreign_key_checks = 1;


-- 验证导入
SELECT COUNT(*) AS 总数据条数 FROM user_behavior;
SELECT MIN(behavior_time) AS 最早时间, MAX(behavior_time) AS 最晚时间 FROM user_behavior;

-- 统计总数据量
SELECT COUNT(*) AS 总数据量 FROM user_behavior;

-- 统计早于开始时间 (2017-11-25) 的数据
SELECT COUNT(*) AS 异常_时间过早 FROM user_behavior 
WHERE behavior_time < '2017-11-25 00:00:00';

-- 统计晚于结束时间 (2017-12-03) 的数据 (包括那个 2030 年的)
SELECT COUNT(*) AS 异常_时间过晚 FROM user_behavior 
WHERE behavior_time > '2017-12-03 23:59:59';

-- 统计所有异常数据的总和
SELECT 
    COUNT(*) AS 异常数据总数,
    CONCAT(ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM user_behavior), 4), '%') AS 异常占比
FROM user_behavior 
WHERE behavior_time < '2017-11-25 00:00:00' 
   OR behavior_time > '2017-12-03 23:59:59';
   
   -- 开启事务，确保安全（如果不小心删错了可以回滚，虽然这里逻辑很稳）
START TRANSACTION;

-- 删除所有不在指定时间范围内的数据
DELETE FROM user_behavior 
WHERE behavior_time < '2017-11-25 00:00:00' 
   OR behavior_time > '2017-12-03 23:59:59';

-- 提交事务
COMMIT;

-- 再次验证：现在应该只剩下有效数据了
SELECT 
    COUNT(*) AS 清洗后剩余数据量,
    MIN(behavior_time) AS 最早时间,
    MAX(behavior_time) AS 最晚时间
FROM user_behavior;

-- 备用表
CREATE TABLE user_behavior_backup AS SELECT * FROM user_behavior;


select distinct behavior_type from user_behavior;

-- ==========================================
-- 3. 基础指标分析 (PV/UV) - [已修复：适配无 dates 字段的表]
-- ==========================================
DROP TABLE IF EXISTS pv_uv_puv;
CREATE TABLE pv_uv_puv (
    dates CHAR(10) NOT NULL PRIMARY KEY,
    pv BIGINT NOT NULL,
    uv BIGINT NOT NULL,
    puv DECIMAL(10,1)
);

-- ✅ 修改点：使用 DATE(behavior_time) 代替 dates 列
INSERT INTO pv_uv_puv (dates, pv, uv, puv)
SELECT 
    DATE(behavior_time) AS dates,
    COUNT(*) AS pv,
    COUNT(DISTINCT user_id) AS uv,
    ROUND(COUNT(*) / COUNT(DISTINCT user_id), 1) AS puv
FROM user_behavior
WHERE behavior_type = 'pv'
GROUP BY DATE(behavior_time);

SELECT * FROM pv_uv_puv;

-- ==========================================
-- 4. 留存率分析 - [已修复]
-- ==========================================
DROP TABLE IF EXISTS retention_rate;
CREATE TABLE retention_rate (
    dates CHAR(10) NOT NULL PRIMARY KEY,
    retention_1 FLOAT COMMENT '1天留存率'
);

-- ✅ 修改点：子查询中动态提取日期
INSERT INTO retention_rate (dates, retention_1)
SELECT 
    a.dates,
    IFNULL(
        COUNT(IF(DATEDIFF(b.dates, a.dates) = 1, b.user_id, NULL)) / 
        COUNT(IF(DATEDIFF(b.dates, a.dates) = 0, b.user_id, NULL)),
        0
    ) AS retention_1
FROM (
    SELECT user_id, DATE(behavior_time) as dates FROM user_behavior GROUP BY user_id, DATE(behavior_time)
) a
LEFT JOIN (
    SELECT user_id, DATE(behavior_time) as dates FROM user_behavior GROUP BY user_id, DATE(behavior_time)
) b ON a.user_id = b.user_id AND a.dates <= b.dates
GROUP BY a.dates;

SELECT * FROM retention_rate;

-- ==========================================
-- 5. 跳失率分析 (保持不变)
-- ==========================================
SELECT 
    CONCAT(
        ROUND(
            (SELECT COUNT(*) FROM (
                SELECT user_id FROM user_behavior GROUP BY user_id HAVING COUNT(*) = 1
            ) a) / 
            (SELECT COUNT(DISTINCT user_id) FROM user_behavior) * 100,
            2
        ),
        '%'
    ) AS 跳失率;

-- ==========================================
-- 6. 时段行为分析 (Hourly Analysis) - [已修复：适配你的数据集]
-- ==========================================
DROP TABLE IF EXISTS date_hour_behavior;
CREATE TABLE date_hour_behavior (
    dates CHAR(10) NOT NULL,
    hours CHAR(2) NOT NULL,
    pv BIGINT NOT NULL DEFAULT 0,
    cart BIGINT NOT NULL DEFAULT 0,
    fav BIGINT NOT NULL DEFAULT 0,  -- 对应 behavior_type = 'fav'
    buy BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (dates, hours)
);

INSERT INTO date_hour_behavior (dates, hours, pv, cart, fav, buy)
SELECT 
    DATE(behavior_time) AS dates,
    LPAD(HOUR(behavior_time), 2, '0') AS hours, 
    COUNT(IF(behavior_type='pv', 1, NULL)) AS pv,
    COUNT(IF(behavior_type='cart', 1, NULL)) AS cart,
    COUNT(IF(behavior_type='fav', 1, NULL)) AS fav,     
    COUNT(IF(behavior_type='buy', 1, NULL)) AS buy
FROM user_behavior
-- 关键修改：GROUP BY 使用与 SELECT 中完全相同的表达式
GROUP BY DATE(behavior_time), LPAD(HOUR(behavior_time), 2, '0');

SELECT * FROM date_hour_behavior LIMIT 10;
SELECT '✅ 第6步完成：时段行为表创建成功' AS Status;

-- ==========================================
-- 7. 行为转化漏斗
-- ==========================================
DROP TABLE IF EXISTS behavior_num;
CREATE TABLE behavior_num (
    behavior_type VARCHAR(10) NOT NULL PRIMARY KEY,
    behavior_count_num BIGINT NOT NULL
);

INSERT INTO behavior_num (behavior_type, behavior_count_num)
SELECT behavior_type, COUNT(*)
FROM user_behavior
WHERE behavior_type IN ('pv', 'cart', 'fav', 'buy')  
GROUP BY behavior_type;

SELECT * FROM behavior_num LIMIT 10;

DROP TABLE IF EXISTS behavior_user_num;
CREATE TABLE behavior_user_num (
    behavior_type VARCHAR(10) NOT NULL PRIMARY KEY,
    user_num BIGINT NOT NULL
);


-- --- 1. 统计 PV (浏览) 用户 ---
CREATE TEMPORARY TABLE tmp_pv AS SELECT DISTINCT user_id FROM user_behavior WHERE behavior_type = 'pv';
INSERT INTO behavior_user_num VALUES ('pv', (SELECT COUNT(*) FROM tmp_pv));
DROP TEMPORARY TABLE tmp_pv;
SELECT '✅ PV 统计完成' AS status;

-- --- 2. 统计 CART (加购) 用户 ---
CREATE TEMPORARY TABLE tmp_cart AS SELECT DISTINCT user_id FROM user_behavior WHERE behavior_type = 'cart';
INSERT INTO behavior_user_num VALUES ('cart', (SELECT COUNT(*) FROM tmp_cart));
DROP TEMPORARY TABLE tmp_cart;
SELECT '✅ CART 统计完成' AS status;

-- --- 3. 统计 FAV (收藏) 用户 ---
CREATE TEMPORARY TABLE tmp_fav AS SELECT DISTINCT user_id FROM user_behavior WHERE behavior_type = 'fav';
INSERT INTO behavior_user_num VALUES ('fav', (SELECT COUNT(*) FROM tmp_fav));
DROP TEMPORARY TABLE tmp_fav;
SELECT '✅ FAV 统计完成' AS status;

-- --- 4. 统计 BUY (购买) 用户 ---
CREATE TEMPORARY TABLE tmp_buy AS SELECT DISTINCT user_id FROM user_behavior WHERE behavior_type = 'buy';
INSERT INTO behavior_user_num VALUES ('buy', (SELECT COUNT(*) FROM tmp_buy));
DROP TEMPORARY TABLE tmp_buy;
SELECT '✅ BUY 统计完成' AS status;

-- --- 最终查看结果 ---
SELECT * FROM behavior_user_num;

SELECT 
    CONCAT(ROUND(
        (SELECT behavior_count_num FROM behavior_num WHERE behavior_type='buy') / 
        (SELECT behavior_count_num FROM behavior_num WHERE behavior_type='pv') * 100, 4
    ), '%') AS 购买率;

-- ==========================================
-- 7. 行为转化漏斗 (修改后：生成标准漏斗模型)
-- ==========================================
DROP TABLE IF EXISTS behavior_num;
CREATE TABLE behavior_num (
    behavior_type VARCHAR(10) NOT NULL PRIMARY KEY,
    behavior_count_num BIGINT NOT NULL
);

INSERT INTO behavior_num (behavior_type, behavior_count_num)
SELECT behavior_type, COUNT(*)
FROM user_behavior
WHERE behavior_type IN ('pv', 'cart', 'fav', 'buy')
GROUP BY behavior_type;

DROP TABLE IF EXISTS behavior_user_num;
CREATE TABLE behavior_user_num (
    behavior_type VARCHAR(10) NOT NULL PRIMARY KEY,
    user_num BIGINT NOT NULL
);

-- 统计各行为的去重用户数
CREATE TEMPORARY TABLE tmp_pv AS SELECT DISTINCT user_id FROM user_behavior WHERE behavior_type = 'pv';
INSERT INTO behavior_user_num VALUES ('pv', (SELECT COUNT(*) FROM tmp_pv));
DROP TEMPORARY TABLE tmp_pv;

CREATE TEMPORARY TABLE tmp_fav AS SELECT DISTINCT user_id FROM user_behavior WHERE behavior_type = 'fav';
INSERT INTO behavior_user_num VALUES ('fav', (SELECT COUNT(*) FROM tmp_fav));
DROP TEMPORARY TABLE tmp_fav;

CREATE TEMPORARY TABLE tmp_cart AS SELECT DISTINCT user_id FROM user_behavior WHERE behavior_type = 'cart';
INSERT INTO behavior_user_num VALUES ('cart', (SELECT COUNT(*) FROM tmp_cart));
DROP TEMPORARY TABLE tmp_cart;

CREATE TEMPORARY TABLE tmp_buy AS SELECT DISTINCT user_id FROM user_behavior WHERE behavior_type = 'buy';
INSERT INTO behavior_user_num VALUES ('buy', (SELECT COUNT(*) FROM tmp_buy));
DROP TEMPORARY TABLE tmp_buy;

-- ✅ 新增：构建标准漏斗结果表
DROP TABLE IF EXISTS standard_funnel;
CREATE TABLE standard_funnel AS
WITH funnel_base AS (
    -- 1. 定义漏斗层级和标准名称 (根据实际业务，通常排序为：浏览 -> 收藏 -> 加购 -> 购买)
    SELECT '1_浏览商品(PV)' AS stage_name, user_num, 1 AS step_order FROM behavior_user_num WHERE behavior_type = 'pv'
    UNION ALL
    SELECT '2_收藏商品(Fav)' AS stage_name, user_num, 2 AS step_order FROM behavior_user_num WHERE behavior_type = 'fav'
    UNION ALL
    SELECT '3_加入购物车(Cart)' AS stage_name, user_num, 3 AS step_order FROM behavior_user_num WHERE behavior_type = 'cart'
    UNION ALL
    SELECT '4_提交订单(Buy)' AS stage_name, user_num, 4 AS step_order FROM behavior_user_num WHERE behavior_type = 'buy'
),
funnel_calc AS (
    -- 2. 使用窗口函数获取上一阶段和初始阶段的数据
    SELECT 
        stage_name,
        user_num,
        LAG(user_num, 1, user_num) OVER(ORDER BY step_order) AS prev_stage_num,
        FIRST_VALUE(user_num) OVER(ORDER BY step_order) AS first_stage_num,
        step_order
    FROM funnel_base
)
-- 3. 计算转化率并输出标准格式
SELECT 
    stage_name AS `阶段名称`,
    user_num AS `各阶段用户数`,
    -- 阶段转化率 = 当前阶段 / 上一阶段
    CONCAT(ROUND((user_num / prev_stage_num) * 100, 2), '%') AS `阶段转化率`,
    -- 累计转化率 = 当前阶段 / 第一阶段
    CONCAT(ROUND((user_num / first_stage_num) * 100, 2), '%') AS `累计转化率`
FROM funnel_calc
ORDER BY step_order;

-- 查看生成的标准漏斗数据
SELECT * FROM standard_funnel;

-- ==========================================
-- 修正后的标准漏斗：采用方案一（核心主干路径）
-- ==========================================
DROP TABLE IF EXISTS standard_funnel_final;
CREATE TABLE standard_funnel_final AS
WITH funnel_base AS (
    -- 仅保留核心三步：浏览 -> 加购 -> 购买
    SELECT '1_浏览商品(PV)' AS stage_name, user_num, 1 AS step_order FROM behavior_user_num WHERE behavior_type = 'pv'
    UNION ALL
    SELECT '2_加入购物车(Cart)' AS stage_name, user_num, 2 AS step_order FROM behavior_user_num WHERE behavior_type = 'cart'
    UNION ALL
    SELECT '3_提交订单(Buy)' AS stage_name, user_num, 3 AS step_order FROM behavior_user_num WHERE behavior_type = 'buy'
),
funnel_calc AS (
    SELECT 
        stage_name,
        user_num,
        LAG(user_num, 1, user_num) OVER(ORDER BY step_order) AS prev_stage_num,
        FIRST_VALUE(user_num) OVER(ORDER BY step_order) AS first_stage_num,
        step_order
    FROM funnel_base
)
SELECT 
    stage_name AS `阶段名称`,
    user_num AS `各阶段用户数`,
    CONCAT(ROUND((user_num / prev_stage_num) * 100, 2), '%') AS `阶段转化率`,
    CONCAT(ROUND((user_num / first_stage_num) * 100, 2), '%') AS `累计转化率`
FROM funnel_calc
ORDER BY step_order;

-- 检查最终结果，此时你会发现转化率全部低于 100%
SELECT * FROM standard_funnel_final;

-- ==========================================
-- 8. 用户购买路径分析
-- ==========================================
DROP TABLE IF EXISTS renhua;
CREATE TABLE renhua (
    path_type CHAR(4) NOT NULL PRIMARY KEY,
    description VARCHAR(50) NOT NULL
);
INSERT INTO renhua (path_type, description) VALUES 
('0001', '直接购买'), ('1001', '浏览后购买'), ('0011', '加购后购买'), ('1011', '浏览加购后购买'),
('0101', '收藏后购买'), ('1101', '浏览收藏后购买'), ('0111', '收藏加购后购买'), ('1111', '全路径购买');

DROP TABLE IF EXISTS path_result;
CREATE TABLE path_result (
    path_type CHAR(4) NOT NULL PRIMARY KEY,
    description VARCHAR(50),
    num BIGINT
);

CREATE TEMPORARY TABLE temp_path_count AS
SELECT 购买路径类型, COUNT(*) AS 数量
FROM (
    SELECT 
        user_id, item_id,
        CONCAT(
            MAX(CASE WHEN behavior_type = 'pv' THEN 1 ELSE 0 END),
            MAX(CASE WHEN behavior_type = 'fav' THEN 1 ELSE 0 END),   
            MAX(CASE WHEN behavior_type = 'cart' THEN 1 ELSE 0 END),
            MAX(CASE WHEN behavior_type = 'buy' THEN 1 ELSE 0 END)
        ) AS 购买路径类型
    FROM user_behavior
    GROUP BY user_id, item_id
    HAVING MAX(CASE WHEN behavior_type = 'buy' THEN 1 ELSE 0 END) = 1
) t
GROUP BY 购买路径类型;

INSERT INTO path_result (path_type, description, num)
SELECT 
    r.path_type, 
    r.description, 
    t.数量
FROM temp_path_count t 
JOIN renhua r 
    -- 👇 关键修改：强制将两边的字段都转换为同一种排序规则
    ON t.购买路径类型 COLLATE utf8mb4_0900_ai_ci = r.path_type COLLATE utf8mb4_0900_ai_ci;

SELECT * FROM path_result ORDER BY num DESC;
SELECT '✅ 第8步完成：路径分析结束' AS Status;

-- ==========================================
-- 9. RFM 模型分析
-- ==========================================
DROP TABLE IF EXISTS rfm_model;
CREATE TABLE rfm_model (
    user_id BIGINT NOT NULL PRIMARY KEY,
    frequency INT NOT NULL,
    recent CHAR(10) NOT NULL,
    fscore INT,
    rscore INT,
    class VARCHAR(40)
);

INSERT INTO rfm_model (user_id, frequency, recent)
SELECT 
    user_id, 
    COUNT(*) AS frequency, 
    MAX(DATE(behavior_time)) AS recent
FROM user_behavior
WHERE behavior_type = 'buy'
GROUP BY user_id;

UPDATE rfm_model SET fscore = CASE
    WHEN frequency >= 100 THEN 5
    WHEN frequency >= 50 THEN 4
    WHEN frequency >= 20 THEN 3
    WHEN frequency >= 5 THEN 2
    ELSE 1
END;

UPDATE rfm_model SET rscore = CASE
    WHEN recent = '2017-12-03' THEN 5
    WHEN recent IN ('2017-12-01', '2017-12-02') THEN 4
    WHEN recent IN ('2017-11-29', '2017-11-30') THEN 3
    WHEN recent IN ('2017-11-27', '2017-11-28') THEN 2
    ELSE 1
END;

SET SQL_SAFE_UPDATES = 0;
UPDATE rfm_model rm
JOIN (SELECT AVG(fscore) as f_avg, AVG(rscore) as r_avg FROM rfm_model) avg_vals
ON 1=1
SET rm.class = CASE
    WHEN rm.fscore > avg_vals.f_avg AND rm.rscore > avg_vals.r_avg THEN '价值用户'
    WHEN rm.fscore > avg_vals.f_avg AND rm.rscore <= avg_vals.r_avg THEN '保持用户'
    WHEN rm.fscore <= avg_vals.f_avg AND rm.rscore > avg_vals.r_avg THEN '发展用户'
    ELSE '挽留用户'
END;

SELECT class, COUNT(*) as 用户数, CONCAT(ROUND(COUNT(*)/(SELECT COUNT(*) FROM rfm_model)*100, 2), '%') as 占比
FROM rfm_model GROUP BY class;
SELECT '✅ 第9步完成：RFM模型构建完毕' AS Status;

-- ==========================================
-- 10. 商品与品类热度分析
-- ==========================================
DROP TABLE IF EXISTS popular_categories;
CREATE TABLE popular_categories AS
SELECT category_id, COUNT(IF(behavior_type='pv',1,NULL)) as pv
FROM user_behavior WHERE category_id IS NOT NULL
GROUP BY category_id ORDER BY pv DESC LIMIT 10;

DROP TABLE IF EXISTS popular_items;
CREATE TABLE popular_items AS
SELECT item_id, COUNT(IF(behavior_type='pv',1,NULL)) as pv
FROM user_behavior WHERE item_id IS NOT NULL
GROUP BY item_id ORDER BY pv DESC LIMIT 10;

DROP TABLE IF EXISTS item_detail;
CREATE TABLE item_detail (
    item_id BIGINT NOT NULL,
    category_id BIGINT NOT NULL,
    pv BIGINT, fav BIGINT, cart BIGINT, buy BIGINT,
    user_buy_rate DECIMAL(10,4),
    PRIMARY KEY (item_id, category_id)
);

INSERT INTO item_detail (item_id, category_id, pv, fav, cart, buy, user_buy_rate)
SELECT 
    item_id, category_id,
    COUNT(IF(behavior_type='pv',1,NULL)) as pv,
    COUNT(IF(behavior_type='fav',1,NULL)) as fav,    
    COUNT(IF(behavior_type='cart',1,NULL)) as cart,
    COUNT(IF(behavior_type='buy',1,NULL)) as buy,
    IFNULL(COUNT(DISTINCT IF(behavior_type='buy', user_id, NULL)) / NULLIF(COUNT(DISTINCT user_id), 0), 0)
FROM user_behavior
WHERE item_id IS NOT NULL AND category_id IS NOT NULL
GROUP BY item_id, category_id;

SELECT * FROM item_detail ORDER BY user_buy_rate DESC LIMIT 10;
SELECT '✅ 第10步完成：商品详情表生成完毕' AS Status;

DROP TABLE IF EXISTS popular_cateitems;

-- 建表（不变）
create table if not exists popular_cateitems(
category_id int,
item_id int,
pv int
);

insert into popular_cateitems
select category_id,item_id,`品类商品浏览量`
from (
    select *,
    rank() over(partition by category_id order by `品类商品浏览量` desc) as r
    from (
        select 
            category_id,
            item_id,
            count(if(behavior_type='pv',behavior_type,null)) as `品类商品浏览量`
        from user_behavior
        group by category_id,item_id
    ) temp
) a
where a.r=1
order by a.`品类商品浏览量` desc
limit 10;

select * from popular_cateitems;

-- 品类转化率 
DROP TABLE IF EXISTS category_detail;
create table category_detail(
category_id int,
pv int,
fav int,
cart int,
buy int,
user_buy_rate float);

insert into category_detail
select category_id
,count(if(behavior_type='pv',behavior_type,null)) 'pv'
,count(if(behavior_type='fav',behavior_type,null)) 'fav'
,count(if(behavior_type='cart',behavior_type,null)) 'cart'
,count(if(behavior_type='buy',behavior_type,null)) 'buy'
,count(distinct if(behavior_type='buy',user_id,null))/count(distinct user_id) 品类转化率
from user_behavior
group by category_id
order by 品类转化率 desc;

select * from category_detail;

-- ==========================================
-- 最终总结
-- ==========================================
SELECT '🎉 所有数据分析任务已完成！请检查各结果表。' AS Final_Status;

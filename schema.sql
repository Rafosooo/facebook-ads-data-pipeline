CREATE TABLE Facebook_Ads (
    id INT IDENTITY(1,1) PRIMARY KEY,
    account_name VARCHAR(255),
    date_start DATE,
    campaign_id VARCHAR(100),
    account_id VARCHAR(100),
    campaign_name VARCHAR(255),
    objective VARCHAR(100),
    adset_id VARCHAR(100),
    adset_name VARCHAR(255),
    ad_id VARCHAR(100),
    ad_name VARCHAR(255),
    metric_type VARCHAR(100),
    metric_value INT,
    insert_date DATETIME DEFAULT GETDATE()
);

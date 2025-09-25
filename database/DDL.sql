
CREATE DATABASE cryptodb;
USE cryptodb;

CREATE TABLE IF NOT EXISTS coins (
    coin_id VARCHAR(50) PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    name VARCHAR(100) NOT NULL,
    image_url VARCHAR(512),
    market_cap_rank INT,
	high_24h DECIMAL(20,5),
    low_24h DECIMAL(20,5),
    ath DECIMAL(20,5),
    ath_date DATETIME,
    atl DECIMAL(20,5),
    atl_date DATETIME,
    max_supply BIGINT
);

CREATE TABLE IF NOT EXISTS market_snapshots (
    snapshot_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    coin_id VARCHAR(50),
    current_price DECIMAL(20,5),
    market_cap DECIMAL(30,2),
    circulating_supply BIGINT,
    total_volume DECIMAL(30,2),
    price_change_percentage_24h DECIMAL(20,5),
    market_cap_change_percentage_24h DECIMAL(20,5),
    ath_change_percentage DECIMAL(20,5),
    atl_change_percentage DECIMAL(20,5),
    last_updated DATETIME,
	extraction_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (coin_id) REFERENCES coins(coin_id)
);

CREATE TABLE IF NOT EXISTS market_history (
    history_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    coin_id VARCHAR(50),
    timestamp DATETIME,
    price DECIMAL(20,5),
    market_cap DECIMAL(30,5),
    volume DECIMAL(30,5),
    FOREIGN KEY (coin_id) REFERENCES coins(coin_id)
);

CREATE TABLE IF NOT EXISTS exchanges (
    exchange_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100),
    year_established INT,
    country VARCHAR(100),
    url VARCHAR(255),
    image_url VARCHAR(512),
    trust_score INT,
    trust_score_rank INT,
    trade_volume_24h_btc DECIMAL(30,5)
);

CREATE TABLE IF NOT EXISTS coin_ohlc (
    ohlc_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    coin_id VARCHAR(50),
    timestamp DATETIME,
    open_price DECIMAL(20,5),
    high_price DECIMAL(20,5),
    low_price DECIMAL(20,5),
    close_price DECIMAL(20,5),
    FOREIGN KEY (coin_id) REFERENCES coins(coin_id)
);



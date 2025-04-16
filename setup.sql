CREATE DATABASE IF NOT EXISTS fraud_detection;
USE fraud_detection;

CREATE TABLE IF NOT EXISTS frauds (
    transaction_id INT PRIMARY KEY,
    user_id INT,
    amount DECIMAL(10,2),
    timestamp DATETIME,
    location VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id INT PRIMARY KEY,
    user_id INT,
    amount DECIMAL(10,2),
    timestamp DATETIME,
    location VARCHAR(10)
);

-- выбираем БД
use OrderLog;
-- Основная таблица, которая включает в себя все заявки
CREATE TABLE
IF
	NOT EXISTS OrdLog (
	NO INT NOT NULL auto_increment PRIMARY KEY,
	seccode TEXT NOT NULL,
	buysell VARCHAR ( 1 ) NOT NULL,
	TIME INT NOT NULL,
	orderno INT NOT NULL,
	ACTION SMALLINT NOT NULL,
	price decimal(20, 10) NOT NULL,
	volume INT NOT NULL,
	tradeno BIGINT,
	tradeprice decimal(20, 10) 
	);
	
-- Таблица с заявками по привилегированным акциям
CREATE TABLE
IF
	NOT EXISTS PreferredStock (
	NO INT NOT NULL auto_increment PRIMARY KEY,
	seccode TEXT NOT NULL,
	buysell VARCHAR ( 1 ) NOT NULL,
	TIME INT NOT NULL,
	orderno INT NOT NULL,
	ACTION SMALLINT NOT NULL,
	price decimal(20, 10) NOT NULL,
	volume INT NOT NULL,
	tradeno BIGINT,
	tradeprice decimal(20, 10)
	);
	
-- Таблица с заявками по обыкновенным акциям
CREATE TABLE
IF
	NOT EXISTS CommonStock (
	NO INT NOT NULL auto_increment PRIMARY KEY,
	seccode TEXT NOT NULL,
	buysell VARCHAR ( 1 ) NOT NULL,
	TIME INT NOT NULL,
	orderno INT NOT NULL,
	ACTION SMALLINT NOT NULL,
	price decimal(20, 10) NOT NULL,
	volume INT NOT NULL,
	tradeno BIGINT,
	tradeprice decimal(20, 10)
	);
	
-- Таблица с заявками по облигациям
CREATE TABLE
IF
	NOT EXISTS Bonds (
	NO INT NOT NULL auto_increment PRIMARY KEY,
	seccode TEXT NOT NULL,
	buysell VARCHAR ( 1 ) NOT NULL,
	TIME INT NOT NULL,
	orderno INT NOT NULL,
	ACTION SMALLINT NOT NULL,
	price decimal(20, 10) NOT NULL,
	volume INT NOT NULL,
	tradeno BIGINT,
	tradeprice decimal(20, 10)
	);
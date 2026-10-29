INSERT INTO MySink SELECT amount * r.rate FROM Orders AS o JOIN RatesHistory FOR SYSTEM_TIME AS OF o.rowtime AS r ON o.currency = r.currency;

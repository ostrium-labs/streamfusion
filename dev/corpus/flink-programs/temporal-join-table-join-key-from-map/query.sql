INSERT INTO MySink SELECT amount * r.rate FROM OrdersNestedId AS o JOIN RatesHistory FOR SYSTEM_TIME AS OF o.rowtime AS r ON o.nested_map['currency'] = r.currency;

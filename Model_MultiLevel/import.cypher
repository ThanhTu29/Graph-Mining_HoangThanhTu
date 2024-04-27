CREATE CONSTRAINT FOR (p:Person) REQUIRE p.employeeID IS UNIQUE;
CREATE CONSTRAINT FOR (i:Item) REQUIRE i.itemID IS UNIQUE;
CREATE CONSTRAINT FOR (t:Transaction) REQUIRE t.transactionID IS UNIQUE;
CREATE CONSTRAINT FOR (p:Period) REQUIRE p.period IS UNIQUE;

CREATE INDEX FOR (i:Item) ON (i.price);
CREATE INDEX FOR (i:Item) ON (i.wholesalePrice);

WITH range(1,52) as periods
FOREACH (period IN periods | 
  MERGE (p:Period {period:period}));

MATCH (p:Period)
WITH p
ORDER BY p.period
WITH COLLECT(p) AS periods
FOREACH (ignore IN RANGE(0, size(periods)-2) | 
  FOREACH(p1 IN [periods[ignore]] | 
      FOREACH(p2 IN [periods[ignore+1]] | 
          MERGE (p1)-[:NEXT]->(p2))));

LOAD CSV WITH HEADERS FROM "https://raw.githubusercontent.com/ThanhTu29/Graph-Mining_HoangThanhTu/master/item.csv" AS line
WITH line, toFloat(line.price) AS price, toInteger(line.item) AS itemID, toFloat(line.kicker) AS kick, toFloat(line.wprice) AS wholesale
CREATE (:Item {itemID:itemID, name:line.name, price:price, kicker:kick, wholesalePrice:wholesale});

// Tải dữ liệu từ tệp employees.csv và tạo các nút Person
LOAD CSV WITH HEADERS FROM "https://raw.githubusercontent.com/ThanhTu29/Graph-Mining_HoangThanhTu/master/employees.csv" AS line
WITH line, toInteger(line.employeeID) AS empID
CREATE (:Person {employeeID:empID, name:line.name});

// Tạo các mối quan hệ REPORTS_TO giữa các nhân viên
LOAD CSV WITH HEADERS FROM "https://raw.githubusercontent.com/ThanhTu29/Graph-Mining_HoangThanhTu/master/employees.csv" AS line
WITH line, toInteger(line.employeeID) AS empID, toInteger(line.reportsTo) AS reportsToID
MATCH (sub:Person {employeeID:empID}), (boss:Person {employeeID:reportsToID})
MERGE (sub)-[:REPORTS_TO]->(boss);

// Tải dữ liệu từ tệp transactions.csv và tạo các nút Transaction
LOAD CSV WITH HEADERS FROM "https://raw.githubusercontent.com/ThanhTu29/Graph-Mining_HoangThanhTu/master/transactions.csv" AS line
WITH line, toInteger(line.transactionID) AS transID
CREATE (:Transaction {transactionID:transID});

// Tạo mối quan hệ OCCURRED_IN giữa các giao dịch và các khoảng thời gian
LOAD CSV WITH HEADERS FROM "https://raw.githubusercontent.com/ThanhTu29/Graph-Mining_HoangThanhTu/master/transactions.csv" AS line
WITH line, toInteger(line.transactionID) AS transID, toInteger(line.period) AS period
MATCH (t:Transaction {transactionID:transID}), (p:Period {period:period})
CREATE (t)-[:OCCURRED_IN]->(p);

// Tạo mối quan hệ CONTAINS giữa các giao dịch và các mặt hàng
LOAD CSV WITH HEADERS FROM "https://raw.githubusercontent.com/ThanhTu29/Graph-Mining_HoangThanhTu/master/transactions.csv" AS line
WITH line,
toInteger(line.transactionID) AS transID,
toInteger(line.item1) AS itemID1,
toInteger(line.item2) AS itemID2,
toInteger(line.item3) AS itemID3
MATCH (tx:Transaction {transactionID:transID}),
      (i1:Item {itemID:itemID1}),
      (i2:Item {itemID:itemID2}),
      (i3:Item {itemID:itemID3})
CREATE (tx)-[:CONTAINS]->(i1),
       (tx)-[:CONTAINS]->(i2),
       (tx)-[:CONTAINS]->(i3);

// Tạo mối quan hệ SOLD giữa nhân viên và các giao dịch
LOAD CSV WITH HEADERS FROM "https://raw.githubusercontent.com/ThanhTu29/Graph-Mining_HoangThanhTu/master/transactions.csv" AS line
WITH line,
toInteger(line.transactionID) AS transID,
toInteger(line.salesRepID) AS repID
MATCH (rep:Person {employeeID:repID}),
      (tx:Transaction {transactionID:transID})
CREATE (rep)-[:SOLD]->(tx);

MATCH (target:Person)<-[r:REPORTS_TO*..]-(e)
WITH target, count(e) as totalReports
SET target.reportsCount = totalReports
WITH target,
//setting the right "level" based on number of reports
CASE
WHEN target.reportsCount > 124
THEN 6
WHEN target.reportsCount < 124 and target.reportsCount >= 75
THEN 5
WHEN target.reportsCount < 75 and target.reportsCount >= 25
THEN 4
WHEN target.reportsCount < 25 and target.reportsCount >= 10
THEN 3
WHEN target.reportsCount < 10 and target.reportsCount >= 2
THEN 2
ELSE 1
END AS levels
SET target.level = levels;
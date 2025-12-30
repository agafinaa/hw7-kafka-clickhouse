SELECT
    state,
    argMax(category, amount) AS max_category,
    max(amount) AS max_amount
FROM teta.transactions
GROUP BY state
ORDER BY state
FORMAT CSVWithNames;

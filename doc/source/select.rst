******
SELECT
******



| SELECT [DISTINCT(col, [,...] )] { * | column | expression } [[AS] alias] [, ...]
|    [ FROM table (is ignored, present only for compatibility) ]
|    [ WHERE condition ]
|    [ GROUP BY { column | expression | alias} [, ...] ]
|    [ HAVING condition ]
|    [ ORDER BY { column | expression } [ASC | DESC] [, ...] ]
|    [ LIMIT limit, [offset] ]


Notes:

* Subquereis and JOINs are currently not supported.


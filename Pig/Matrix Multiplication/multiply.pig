A = LOAD '$M' USING PigStorage(',') AS (x, y, z);
B = LOAD '$N' USING PigStorage(',') AS (a,b,c);
C = JOIN A BY y, B BY a;
D = FOREACH C GENERATE x AS i,b AS j,z*c AS value;
E = GROUP D BY (i,j);
F = FOREACH E GENERATE FLATTEN($0),SUM(D.value);
STORE F INTO '$O' USING PigStorage (',');

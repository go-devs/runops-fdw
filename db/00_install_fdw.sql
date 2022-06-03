CREATE EXTENSION multicorn;

CREATE SERVER runops_fdw FOREIGN DATA WRAPPER multicorn
OPTIONS (
  wrapper 'runops.fdw.RunopsForeignDataWrapper'
);

CREATE SCHEMA runops;

IMPORT FOREIGN SCHEMA runops FROM SERVER runops_fdw INTO runops;

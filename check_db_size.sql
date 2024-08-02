/** Check database size here **/
select pg_size_pretty(pg_database_size('db_name'));